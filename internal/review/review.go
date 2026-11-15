package review

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	amqp "github.com/rabbitmq/amqp091-go"
	"price-crawler/services/catalog-comparison/internal/messaging"
)

const consumerPrefix = "catalog-review-"

type actor struct {
	Subject string `json:"subject"`
	Role    string `json:"role"`
}

type listRequest struct {
	Actor  actor  `json:"actor"`
	Limit  int    `json:"limit"`
	Cursor string `json:"cursor"`
}

type productInput struct {
	Name  string  `json:"name"`
	Brand *string `json:"brand"`
	Model *string `json:"model"`
	GTIN  *string `json:"gtin"`
}

type selection struct {
	Kind      string        `json:"kind"`
	ProductID string        `json:"product_id,omitempty"`
	Product   *productInput `json:"product,omitempty"`
}

type decisionRequest struct {
	Actor      actor      `json:"actor"`
	Suggestion string     `json:"suggestion_id"`
	Decision   string     `json:"decision"`
	Selection  *selection `json:"selection,omitempty"`
}

type suggestion struct {
	SuggestionID    string    `json:"suggestion_id"`
	OfferID         string    `json:"offer_id"`
	CandidateID     string    `json:"candidate_id"`
	ProposedProduct *string   `json:"proposed_product_id"`
	Title           *string   `json:"title"`
	CreatedAt       time.Time `json:"created_at"`
}

func decode(raw []byte, target any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(target); err != nil {
		return err
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON data")
	}
	return nil
}

func randomID() (string, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}
	return id.String(), nil
}

func digest(value []byte) []byte {
	sum := sha256.Sum256(value)
	return sum[:]
}

func digestHex(value []byte) string { return hex.EncodeToString(digest(value)) }

func validUUID(value string) bool {
	parsed, err := uuid.Parse(value)
	return err == nil && parsed.Version() >= 1 && parsed.Version() <= 8 && parsed.Variant() == uuid.RFC4122
}

func allowedListRole(role string) bool {
	return role == "manager" || role == "administrator" || role == "auditor"
}

func validActor(value actor) bool {
	return strings.TrimSpace(value.Subject) != "" && len(value.Subject) <= 200
}

func response(status string, extra map[string]any) map[string]any {
	result := map[string]any{"status": status}
	for key, value := range extra {
		result[key] = value
	}
	return result
}

func roleError(role string) string {
	if role == "end_user" || role == "auditor" {
		return "forbidden"
	}
	return "invalid"
}

func list(ctx context.Context, db *sql.DB, organization string, request listRequest) (map[string]any, error) {
	if !validActor(request.Actor) || !allowedListRole(request.Actor.Role) {
		return response(roleError(request.Actor.Role), nil), nil
	}
	if request.Limit < 1 || request.Limit > 100 || len(request.Cursor) > 200 {
		return response("invalid", nil), nil
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	var snapshotHash []byte
	offset := 0
	if request.Cursor == "" {
		token, err := randomID()
		if err != nil {
			return nil, err
		}
		snapshotHash = digest([]byte(token))
		_, err = tx.ExecContext(ctx, `INSERT INTO catalog_review_snapshot(organization_id,token_hash,actor_subject,page_limit,expires_at)
			VALUES($1,$2,$3,$4,now()+interval '120 seconds')`, organization, snapshotHash, request.Actor.Subject, request.Limit)
		if err != nil {
			return nil, err
		}
		rows, err := tx.QueryContext(ctx, `SELECT s.id::text,s.offer_id::text,s.candidate_id::text,s.proposed_product_id::text,s.title,s.created_at
			FROM match_suggestion s WHERE s.organization_id=$1 AND NOT EXISTS
			(SELECT 1 FROM review_decision d WHERE d.organization_id=s.organization_id AND d.suggestion_id=s.id)
			ORDER BY s.created_at,s.id`, organization)
		if err != nil {
			return nil, err
		}
		ordinal := 0
		for rows.Next() {
			var item suggestion
			if err = rows.Scan(&item.SuggestionID, &item.OfferID, &item.CandidateID, &item.ProposedProduct, &item.Title, &item.CreatedAt); err != nil {
				_ = rows.Close()
				return nil, err
			}
			encoded, marshalErr := json.Marshal(item)
			if marshalErr != nil {
				_ = rows.Close()
				return nil, marshalErr
			}
			if _, err = tx.ExecContext(ctx, `INSERT INTO catalog_review_snapshot_item(organization_id,snapshot_token_hash,ordinal,item) VALUES($1,$2,$3,$4)`, organization, snapshotHash, ordinal, encoded); err != nil {
				_ = rows.Close()
				return nil, err
			}
			ordinal++
		}
		if err = rows.Err(); err != nil {
			_ = rows.Close()
			return nil, err
		}
		if err = rows.Close(); err != nil {
			return nil, err
		}
	} else {
		var storedOrganization, storedActor string
		var pageLimit int
		var expires time.Time
		cursorHash := digest([]byte(request.Cursor))
		err = tx.QueryRowContext(ctx, `SELECT organization_id::text,actor_subject,snapshot_token_hash,offset_items,page_limit,expires_at
			FROM catalog_review_cursor WHERE token_hash=$1`, cursorHash).
			Scan(&storedOrganization, &storedActor, &snapshotHash, &offset, &pageLimit, &expires)
		if errors.Is(err, sql.ErrNoRows) || (err == nil && (storedOrganization != organization || storedActor != request.Actor.Subject)) {
			return response("forbidden", nil), tx.Commit()
		}
		if err != nil {
			return nil, err
		}
		if !expires.After(time.Now()) {
			return response("expired", nil), tx.Commit()
		}
		if pageLimit != request.Limit {
			return response("invalid", nil), tx.Commit()
		}
		var snapshotExpires time.Time
		err = tx.QueryRowContext(ctx, `SELECT expires_at FROM catalog_review_snapshot WHERE organization_id=$1 AND token_hash=$2 AND actor_subject=$3`, organization, snapshotHash, request.Actor.Subject).Scan(&snapshotExpires)
		if errors.Is(err, sql.ErrNoRows) || (err == nil && !snapshotExpires.After(time.Now())) {
			return response("expired", nil), tx.Commit()
		}
		if err != nil {
			return nil, err
		}
	}
	items := make([]json.RawMessage, 0, request.Limit)
	rows, err := tx.QueryContext(ctx, `SELECT item FROM catalog_review_snapshot_item WHERE organization_id=$1 AND snapshot_token_hash=$2 AND ordinal >= $3 ORDER BY ordinal LIMIT $4`, organization, snapshotHash, offset, request.Limit+1)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var item []byte
		if err = rows.Scan(&item); err != nil {
			_ = rows.Close()
			return nil, err
		}
		items = append(items, append(json.RawMessage(nil), item...))
	}
	if err = rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	if err = rows.Close(); err != nil {
		return nil, err
	}
	more := len(items) > request.Limit
	if more {
		items = items[:request.Limit]
	}
	var next any
	if more {
		cursor, err := createCursor(ctx, tx, organization, request.Actor.Subject, snapshotHash, offset+len(items), request.Limit)
		if err != nil {
			return nil, err
		}
		next = cursor
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return response("ok", map[string]any{"items": items, "next_cursor": next}), nil
}

func createCursor(ctx context.Context, tx *sql.Tx, organization, subject string, snapshotHash []byte, offset, limit int) (string, error) {
	token, err := randomID()
	if err != nil {
		return "", err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO catalog_review_cursor(token_hash,organization_id,snapshot_token_hash,actor_subject,offset_items,page_limit,expires_at)
		VALUES($1,$2,$3,$4,$5,$6,now()+interval '120 seconds')`, digest([]byte(token)), organization, snapshotHash, subject, offset, limit)
	return token, err
}

func validateDecision(e messaging.Envelope, request decisionRequest) string {
	if !validActor(request.Actor) || !validUUID(request.Suggestion) || (request.Decision != "approve" && request.Decision != "reject") ||
		len(e.IdempotencyKey) < 1 || len(e.IdempotencyKey) > 200 {
		return "invalid"
	}
	if request.Actor.Role != "manager" && request.Actor.Role != "administrator" {
		return "forbidden"
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(e.Payload, &fields) != nil {
		return "invalid"
	}
	if request.Decision == "reject" {
		if _, supplied := fields["selection"]; supplied {
			return "invalid"
		}
		return ""
	}
	if request.Selection == nil {
		return "invalid"
	}
	switch request.Selection.Kind {
	case "select":
		if !validUUID(request.Selection.ProductID) || request.Selection.Product != nil {
			return "invalid"
		}
	case "create":
		p := request.Selection.Product
		if p == nil || request.Selection.ProductID != "" || strings.TrimSpace(p.Name) == "" || len(p.Name) > 200 ||
			!optionalValid(p.Brand) || !optionalValid(p.Model) || !optionalValid(p.GTIN) {
			return "invalid"
		}
	default:
		return "invalid"
	}
	return ""
}

func hasFields(raw []byte, path string, fields ...string) bool {
	var root map[string]json.RawMessage
	if json.Unmarshal(raw, &root) != nil {
		return false
	}
	object := root
	if path != "" {
		for _, part := range strings.Split(path, ".") {
			var nested json.RawMessage
			if json.Unmarshal(object[part], &nested) != nil {
				return false
			}
			var next map[string]json.RawMessage
			if json.Unmarshal(nested, &next) != nil || next == nil {
				return false
			}
			object = next
		}
	}
	for _, field := range fields {
		if _, ok := object[field]; !ok {
			return false
		}
	}
	return true
}

func optionalValid(value *string) bool {
	return value == nil || (strings.TrimSpace(*value) != "" && len(*value) <= 200)
}

func decide(ctx context.Context, db *sql.DB, e messaging.Envelope, request decisionRequest) (map[string]any, error) {
	if invalid := validateDecision(e, request); invalid != "" {
		return response(invalid, nil), nil
	}
	body, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	requestHash := digestHex(body)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	lockMaterial, err := json.Marshal([]string{e.Organization, request.Actor.Subject, "review.decide", e.IdempotencyKey})
	if err != nil {
		return nil, err
	}
	lockKey := string(lockMaterial)
	var lockResult any
	if err = tx.QueryRowContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,1))`, lockKey).Scan(&lockResult); err != nil {
		return nil, err
	}
	var previousHash string
	var previousResult []byte
	err = tx.QueryRowContext(ctx, `SELECT request_hash,result FROM catalog_review_idempotency
		WHERE organization_id=$1 AND actor_subject=$2 AND operation='review.decide' AND idempotency_key=$3`, e.Organization, request.Actor.Subject, e.IdempotencyKey).
		Scan(&previousHash, &previousResult)
	if err == nil {
		if previousHash != requestHash {
			return response("conflict", nil), tx.Commit()
		}
		var cached map[string]any
		if err = json.Unmarshal(previousResult, &cached); err != nil {
			return nil, err
		}
		return cached, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	result, applyErr := applyDecision(ctx, tx, e, request)
	if applyErr != nil {
		return nil, applyErr
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO catalog_review_idempotency(organization_id,actor_subject,operation,idempotency_key,request_hash,result)
		VALUES($1,$2,'review.decide',$3,$4,$5)`, e.Organization, request.Actor.Subject, e.IdempotencyKey, requestHash, encoded)
	if err != nil {
		return nil, err
	}
	return result, tx.Commit()
}

// Decide applies one authenticated review command. Actor and organization are
// supplied by the trusted user-api message boundary and rechecked here.
func Decide(ctx context.Context, db *sql.DB, e messaging.Envelope) (map[string]any, error) {
	if e.Validate() != nil || e.MessageType != "catalog.review.decide.requested" ||
		!hasFields(e.Payload, "", "actor", "suggestion_id", "decision") {
		return response("invalid", nil), nil
	}
	var request decisionRequest
	if err := decode(e.Payload, &request); err != nil {
		return response("invalid", nil), nil
	}
	if !hasFields(e.Payload, "actor", "subject", "role") {
		return response("invalid", nil), nil
	}
	if request.Selection != nil {
		if !hasFields(e.Payload, "selection", "kind") {
			return response("invalid", nil), nil
		}
		if request.Selection.Kind == "select" && !hasFields(e.Payload, "selection", "product_id") {
			return response("invalid", nil), nil
		}
		if request.Selection.Kind == "create" && !hasFields(e.Payload, "selection.product", "name", "brand", "model", "gtin") {
			return response("invalid", nil), nil
		}
	}
	return decide(ctx, db, e, request)
}

func List(ctx context.Context, db *sql.DB, e messaging.Envelope) (map[string]any, error) {
	if e.Validate() != nil || e.MessageType != "catalog.review.list.requested" ||
		!hasFields(e.Payload, "", "actor", "limit") || !hasFields(e.Payload, "actor", "subject", "role") {
		return response("invalid", nil), nil
	}
	var request listRequest
	if err := decode(e.Payload, &request); err != nil {
		return response("invalid", nil), nil
	}
	return list(ctx, db, e.Organization, request)
}

func applyDecision(ctx context.Context, tx *sql.Tx, e messaging.Envelope, request decisionRequest) (map[string]any, error) {
	var item suggestion
	err := tx.QueryRowContext(ctx, `SELECT id::text,offer_id::text,candidate_id::text,proposed_product_id::text,title,created_at
		FROM match_suggestion WHERE organization_id=$1 AND id=$2 FOR UPDATE`, e.Organization, request.Suggestion).
		Scan(&item.SuggestionID, &item.OfferID, &item.CandidateID, &item.ProposedProduct, &item.Title, &item.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		var exists bool
		if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM match_suggestion WHERE id=$1)`, request.Suggestion).Scan(&exists); err != nil {
			return nil, err
		}
		if exists {
			return response("forbidden", nil), nil
		}
		return response("not_found", nil), nil
	}
	if err != nil {
		return nil, err
	}
	var alreadyDecided bool
	if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM review_decision WHERE organization_id=$1 AND suggestion_id=$2)`, e.Organization, request.Suggestion).Scan(&alreadyDecided); err != nil {
		return nil, err
	}
	if alreadyDecided {
		return response("conflict", nil), nil
	}
	var lockResult any
	if err = tx.QueryRowContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1 || ':' || $2,0))`, e.Organization, item.OfferID).Scan(&lockResult); err != nil {
		return nil, err
	}
	var associationExists bool
	if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM offer_match WHERE organization_id=$1 AND offer_id=$2)`, e.Organization, item.OfferID).Scan(&associationExists); err != nil {
		return nil, err
	}
	if request.Decision == "approve" && associationExists {
		return response("conflict", nil), nil
	}
	productID := ""
	if request.Decision == "approve" {
		if request.Selection.Kind == "select" {
			var owner string
			err = tx.QueryRowContext(ctx, `SELECT id::text FROM catalog_product WHERE organization_id=$1 AND id=$2`, e.Organization, request.Selection.ProductID).Scan(&owner)
			if errors.Is(err, sql.ErrNoRows) {
				var foreign bool
				if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM catalog_product WHERE id=$1)`, request.Selection.ProductID).Scan(&foreign); err != nil {
					return nil, err
				}
				if foreign {
					return response("forbidden", nil), nil
				}
				return response("not_found", nil), nil
			}
			if err != nil {
				return nil, err
			}
			productID = owner
		} else {
			productID, err = randomID()
			if err != nil {
				return nil, err
			}
			p := request.Selection.Product
			_, err = tx.ExecContext(ctx, `INSERT INTO catalog_product(id,organization_id,name,brand,manufacturer_model,gtin)
				VALUES($1,$2,$3,$4,$5,$6)`, productID, e.Organization, strings.TrimSpace(p.Name), nullableTrim(p.Brand), nullableTrim(p.Model), nullableTrim(p.GTIN))
			if err != nil {
				return nil, err
			}
		}
	}
	decisionID, err := randomID()
	if err != nil {
		return nil, err
	}
	auditID, err := randomID()
	if err != nil {
		return nil, err
	}
	var decidedAt time.Time
	var productValue any
	if productID != "" {
		productValue = productID
	}
	err = tx.QueryRowContext(ctx, `INSERT INTO review_decision(id,organization_id,suggestion_id,offer_id,decision,product_id,reviewer_subject)
		VALUES($1,$2,$3,$4,$5,$6,$7) RETURNING decided_at`, decisionID, e.Organization, request.Suggestion, item.OfferID, request.Decision, productValue, request.Actor.Subject).Scan(&decidedAt)
	if err != nil {
		var postgresError *pq.Error
		if errors.As(err, &postgresError) && postgresError.Code == "23505" {
			return response("conflict", nil), nil
		}
		return nil, err
	}
	if request.Decision == "approve" {
		_, err = tx.ExecContext(ctx, `INSERT INTO offer_match(organization_id,offer_id,product_id,method,review_decision_id)
			VALUES($1,$2,$3,'review',$4)`, e.Organization, item.OfferID, productID, decisionID)
		if err != nil {
			return nil, err
		}
	}
	jobStatus := "rejected"
	if request.Decision == "approve" {
		jobStatus = "matched"
	}
	_, err = tx.ExecContext(ctx, `UPDATE pending_match_job SET status=$4,decision_id=$5,next_attempt_at=NULL,updated_at=now()
		WHERE organization_id=$1 AND offer_id=$2 AND candidate_id=$3 AND status='review'`, e.Organization, item.OfferID, item.CandidateID, jobStatus, decisionID)
	if err != nil {
		return nil, err
	}
	before, _ := json.Marshal(map[string]any{"status": "pending"})
	after, _ := json.Marshal(map[string]any{"decision_id": decisionID, "decision": request.Decision, "offer_id": item.OfferID, "product_id": productValue})
	_, err = tx.ExecContext(ctx, `INSERT INTO catalog_audit_record(id,organization_id,actor_subject,actor_role,action,entity_type,entity_id,before_value,after_value,occurred_at)
		VALUES($1,$2,$3,$4,$5,'match_suggestion',$6,$7,$8,$9)`, auditID, e.Organization, request.Actor.Subject, request.Actor.Role, "catalog.review."+request.Decision, request.Suggestion, before, after, decidedAt)
	if err != nil {
		return nil, err
	}
	if err = enqueueAudit(ctx, tx, e, request.Actor, item, request.Decision, before, after, decidedAt); err != nil {
		return nil, err
	}
	if request.Decision == "approve" {
		if err = enqueueAssociation(ctx, tx, e, item.OfferID, productID); err != nil {
			return nil, err
		}
	}
	extra := map[string]any{"decision_id": decisionID, "suggestion_id": item.SuggestionID, "offer_id": item.OfferID, "decision": request.Decision, "product_id": productValue, "reviewer": request.Actor.Subject, "decided_at": decidedAt.UTC()}
	return response("ok", extra), nil
}

func nullableTrim(value *string) any {
	if value == nil || strings.TrimSpace(*value) == "" {
		return nil
	}
	return strings.TrimSpace(*value)
}

func enqueueAudit(ctx context.Context, tx *sql.Tx, request messaging.Envelope, actorValue actor, item suggestion, decision string, before, after []byte, occurred time.Time) error {
	messageID, err := randomID()
	if err != nil {
		return err
	}
	payload, err := json.Marshal(map[string]any{
		"actor":           map[string]string{"subject": actorValue.Subject, "role": actorValue.Role},
		"organization_id": request.Organization, "action": "catalog.review." + decision,
		"entity": map[string]string{"type": "match_suggestion", "id": item.SuggestionID},
		"before": json.RawMessage(before), "after": json.RawMessage(after), "occurred_at": occurred.UTC(),
	})
	if err != nil {
		return err
	}
	event := messaging.Envelope{MessageID: messageID, MessageType: "audit.event.recorded", SchemaVersion: 1, OccurredAt: occurred.UTC(), Organization: request.Organization, CorrelationID: request.CorrelationID, CausationID: request.MessageID, Payload: payload}
	return messaging.EnqueueOutbox(ctx, tx, event, "audit.event.recorded.v1")
}

func enqueueAssociation(ctx context.Context, tx *sql.Tx, request messaging.Envelope, offerID, productID string) error {
	messageID, err := randomID()
	if err != nil {
		return err
	}
	payload, err := json.Marshal(map[string]any{"offer_id": offerID, "revision": 1, "product_id": productID})
	if err != nil {
		return err
	}
	event := messaging.Envelope{MessageID: messageID, MessageType: "catalog.offer.association.changed", SchemaVersion: 1, OccurredAt: time.Now().UTC(), Organization: request.Organization, CorrelationID: request.CorrelationID, CausationID: request.MessageID, Payload: payload}
	return messaging.EnqueueOutbox(ctx, tx, event, "catalog.offer.association.changed.v1")
}

func handle(ctx context.Context, db *sql.DB, request messaging.Envelope) (messaging.Envelope, error) {
	var payload map[string]any
	switch request.MessageType {
	case "catalog.review.list.requested":
		var err error
		payload, err = List(ctx, db, request)
		if err != nil {
			return messaging.Envelope{}, err
		}
	case "catalog.review.decide.requested":
		var err error
		payload, err = Decide(ctx, db, request)
		if err != nil {
			return messaging.Envelope{}, err
		}
	default:
		return messaging.Envelope{}, errors.New("unsupported catalog review message")
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return messaging.Envelope{}, err
	}
	messageID, err := randomID()
	if err != nil {
		return messaging.Envelope{}, err
	}
	resultType := strings.TrimSuffix(request.MessageType, ".requested") + ".result"
	return messaging.Envelope{MessageID: messageID, MessageType: resultType, SchemaVersion: 1, OccurredAt: time.Now().UTC(), Organization: request.Organization, CorrelationID: request.CorrelationID, CausationID: request.MessageID, Payload: body}, nil
}

func Run(ctx context.Context, db *sql.DB, url string, logger *slog.Logger) {
	for _, queue := range []string{"catalog.review.list.requested.v1", "catalog.review.decide.requested.v1"} {
		queue := queue
		go runQueue(ctx, db, url, queue, logger)
	}
	<-ctx.Done()
}

func runQueue(ctx context.Context, db *sql.DB, url, queue string, logger *slog.Logger) {
	for ctx.Err() == nil {
		if err := consume(ctx, db, url, queue); err != nil && ctx.Err() == nil {
			logger.Warn("catalog review consumer disconnected", "queue", queue, "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func consume(ctx context.Context, db *sql.DB, url, queue string) error {
	conn, err := amqp.Dial(url)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer func() { _ = ch.Close() }()
	deadQueue := queue + ".dead"
	if _, err = ch.QueueDeclare(deadQueue, true, false, false, false, nil); err != nil {
		return err
	}
	queueArgs := amqp.Table{"x-dead-letter-exchange": "", "x-dead-letter-routing-key": deadQueue}
	if _, err = ch.QueueDeclare(queue, true, false, false, false, queueArgs); err != nil {
		return err
	}
	if err = ch.Qos(1, 0, false); err != nil {
		return err
	}
	if err = ch.Confirm(false); err != nil {
		return err
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	returns := ch.NotifyReturn(make(chan amqp.Return, 1))
	deliveries, err := ch.Consume(queue, consumerPrefix+queue, false, false, false, false, nil)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case delivery, ok := <-deliveries:
			if !ok {
				return errors.New("catalog review queue closed")
			}
			request, decodeErr := messaging.Decode(delivery.Body)
			if decodeErr != nil || request.MessageType != strings.TrimSuffix(queue, ".v1") || !strings.HasPrefix(delivery.ReplyTo, "amq.gen-") || delivery.CorrelationId != request.CorrelationID {
				if err = delivery.Reject(false); err != nil {
					return err
				}
				continue
			}
			bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
			reply, processErr := handle(bounded, db, request)
			cancel()
			if processErr != nil {
				return processErr
			}
			encoded, err := reply.Encode()
			if err != nil {
				return err
			}
			if err = ch.PublishWithContext(ctx, "", delivery.ReplyTo, true, false, amqp.Publishing{ContentType: "application/json", DeliveryMode: amqp.Persistent, MessageId: reply.MessageID, CorrelationId: reply.CorrelationID, Type: reply.MessageType, Body: encoded}); err != nil {
				return err
			}
			select {
			case confirmation, open := <-confirms:
				if !open || !confirmation.Ack {
					return errors.New("RabbitMQ did not confirm catalog review reply")
				}
			case <-ctx.Done():
				return ctx.Err()
			}
			select {
			case returned := <-returns:
				return errors.New("catalog review reply was unroutable: " + returned.RoutingKey)
			default:
			}
			if err = delivery.Ack(false); err != nil {
				return err
			}
		}
	}
}
