package matching

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"

	"price-crawler/services/catalog-comparison/internal/messaging"

	amqp "github.com/rabbitmq/amqp091-go"
)

const consumerName = "catalog-candidate-jobs"

type collectionResult struct {
	OfferID        string  `json:"offer_id"`
	CrawlRequestID string  `json:"crawl_request_id"`
	CandidateID    *string `json:"candidate_id"`
	Outcome        string  `json:"outcome"`
}

type candidateRead struct {
	OfferID        string          `json:"offer_id"`
	CrawlRequestID string          `json:"crawl_request_id"`
	CandidateID    string          `json:"candidate_id"`
	Status         string          `json:"status"`
	Candidate      json.RawMessage `json:"candidate"`
}

type job struct {
	ID, Organization, Offer, Request, Candidate string
	Attempts                                    int
}

func id() (string, error) {
	value, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}
	return value.String(), nil
}

func decodePayload(raw []byte, target any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(target); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return errors.New("trailing payload")
	}
	return nil
}

// RecordCollection commits the inbox and exact candidate job before its delivery is acknowledged.
func RecordCollection(ctx context.Context, db *sql.DB, e messaging.Envelope) error {
	if e.Validate() != nil || e.MessageType != "collection.result" {
		return errors.New("invalid collection envelope")
	}
	var p collectionResult
	// Collection v1 has additional display facts; decode only the identity and outcome.
	if err := json.Unmarshal(e.Payload, &p); err != nil || !validUUID(p.OfferID) || !validUUID(p.CrawlRequestID) {
		return errors.New("invalid collection identity")
	}
	switch p.Outcome {
	case "complete", "partial":
		if p.CandidateID == nil || !validUUID(*p.CandidateID) {
			return errors.New("successful collection has no candidate")
		}
	case "failed", "paused":
		if p.CandidateID != nil {
			return errors.New("failed collection has a candidate")
		}
	default:
		return errors.New("invalid collection outcome")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	fresh, err := messaging.BeginInbox(ctx, tx, e.MessageID, consumerName)
	if err != nil {
		return err
	}
	if !fresh {
		return tx.Commit()
	}
	if p.CandidateID != nil {
		jobID, err := id()
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO pending_match_job(id,organization_id,offer_id,crawl_request_id,candidate_id,status,next_attempt_at) VALUES($1,$2,$3,$4,$5,'pending',now()) ON CONFLICT (organization_id,offer_id,crawl_request_id,candidate_id) DO NOTHING`, jobID, e.Organization, p.OfferID, p.CrawlRequestID, *p.CandidateID)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func claim(ctx context.Context, db *sql.DB) (job, error) {
	var j job
	err := db.QueryRowContext(ctx, `WITH due AS (
 SELECT id FROM pending_match_job
 WHERE status='pending' AND attempts<5 AND next_attempt_at<=now()
 ORDER BY next_attempt_at,created_at FOR UPDATE SKIP LOCKED LIMIT 1
) UPDATE pending_match_job j SET attempts=j.attempts+1,next_attempt_at=now()+interval '30 seconds',updated_at=now()
FROM due WHERE j.id=due.id RETURNING j.id::text,j.organization_id::text,j.offer_id::text,j.crawl_request_id::text,j.candidate_id::text,j.attempts`).Scan(&j.ID, &j.Organization, &j.Offer, &j.Request, &j.Candidate, &j.Attempts)
	return j, err
}

func retry(ctx context.Context, db *sql.DB, j job, code string) error {
	if len(code) > 200 {
		code = code[:200]
	}
	delay := time.Duration(1<<min(j.Attempts, 5)) * time.Second
	if delay > 5*time.Minute {
		delay = 5 * time.Minute
	}
	if j.Attempts >= 5 {
		_, err := db.ExecContext(ctx, `UPDATE pending_match_job SET next_attempt_at=NULL,last_error=$2,updated_at=now() WHERE id=$1 AND status='pending'`, j.ID, code)
		return err
	}
	_, err := db.ExecContext(ctx, `UPDATE pending_match_job SET next_attempt_at=$2,last_error=$3,updated_at=now() WHERE id=$1 AND status='pending'`, j.ID, time.Now().Add(delay), code)
	return err
}

func resolve(ctx context.Context, db *sql.DB, url string, j job) error {
	payload, _ := json.Marshal(map[string]string{"offer_id": j.Offer, "crawl_request_id": j.Request, "candidate_id": j.Candidate})
	messageID, err := id()
	if err != nil {
		return err
	}
	req := messaging.Envelope{MessageID: messageID, MessageType: "candidate.read.requested", SchemaVersion: 1, OccurredAt: time.Now().UTC(), Organization: j.Organization, CorrelationID: messageID, Payload: payload}
	reply, err := requestCandidate(ctx, url, req)
	if err != nil {
		return retry(ctx, db, j, "transport_unavailable")
	}
	if reply.MessageType != "candidate.read.result" || reply.SchemaVersion != 1 || reply.Organization != j.Organization || reply.CorrelationID != req.CorrelationID || reply.CausationID != req.MessageID {
		return retry(ctx, db, j, "invalid_reply_envelope")
	}
	var result candidateRead
	if err = decodePayload(reply.Payload, &result); err != nil || result.OfferID != j.Offer || result.CrawlRequestID != j.Request || result.CandidateID != j.Candidate {
		return retry(ctx, db, j, "invalid_reply_identity")
	}
	if result.Status != "found" {
		switch result.Status {
		case "pending", "not_found", "forbidden":
			return retry(ctx, db, j, result.Status)
		default:
			return retry(ctx, db, j, "invalid_reply_status")
		}
	}
	var c struct {
		OfferID              string                     `json:"offer_id"`
		CrawlRequestID       string                     `json:"crawl_request_id"`
		CandidateID          string                     `json:"candidate_id"`
		NormalizationVersion string                     `json:"normalization_version"`
		Fields               map[string]json.RawMessage `json:"fields"`
		Attributes           []json.RawMessage          `json:"attributes"`
	}
	if json.Unmarshal(result.Candidate, &c) != nil || c.OfferID != j.Offer || c.CrawlRequestID != j.Request || c.CandidateID != j.Candidate || c.NormalizationVersion == "" || len(c.Attributes) > 100 || len(c.Fields) != 5 {
		return retry(ctx, db, j, "invalid_candidate")
	}
	for _, name := range []string{"title", "description", "brand", "manufacturer_model", "gtin"} {
		raw, ok := c.Fields[name]
		if !ok {
			return retry(ctx, db, j, "missing_field")
		}
		var field struct {
			Status          string `json:"status"`
			NormalizedValue any    `json:"normalized_value"`
		}
		if json.Unmarshal(raw, &field) != nil || (field.Status != "valid" && field.NormalizedValue != nil) {
			return retry(ctx, db, j, "invalid_field")
		}
	}
	_, err = db.ExecContext(ctx, `UPDATE pending_match_job SET status='ready',resolved_candidate=$2,next_attempt_at=NULL,last_error=NULL,updated_at=now() WHERE id=$1 AND status='pending'`, j.ID, result.Candidate)
	return err
}

func requestCandidate(parent context.Context, url string, request messaging.Envelope) (messaging.Envelope, error) {
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	conn, err := amqp.Dial(url)
	if err != nil {
		return messaging.Envelope{}, err
	}
	defer func() { _ = conn.Close() }()
	ch, err := conn.Channel()
	if err != nil {
		return messaging.Envelope{}, err
	}
	defer func() { _ = ch.Close() }()
	q, err := ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		return messaging.Envelope{}, err
	}
	replies, err := ch.Consume(q.Name, "", true, true, false, false, nil)
	if err != nil {
		return messaging.Envelope{}, err
	}
	body, err := request.Encode()
	if err != nil {
		return messaging.Envelope{}, err
	}
	if err = ch.PublishWithContext(ctx, "", "candidate.read.requested.v1", true, false, amqp.Publishing{ContentType: "application/json", DeliveryMode: amqp.Persistent, MessageId: request.MessageID, CorrelationId: request.CorrelationID, ReplyTo: q.Name, Body: body}); err != nil {
		return messaging.Envelope{}, err
	}
	for {
		select {
		case <-ctx.Done():
			return messaging.Envelope{}, ctx.Err()
		case delivery, ok := <-replies:
			if !ok {
				return messaging.Envelope{}, errors.New("reply queue closed")
			}
			if delivery.CorrelationId == request.CorrelationID {
				return messaging.Decode(delivery.Body)
			}
		}
	}
}

func Run(ctx context.Context, db *sql.DB, url string, logger *slog.Logger) {
	go consumeCollections(ctx, db, url, logger)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for i := 0; i < 10; i++ {
				j, err := claim(ctx, db)
				if errors.Is(err, sql.ErrNoRows) {
					break
				}
				if err != nil {
					logger.Warn("claim candidate job", "error", err)
					break
				}
				if err = resolve(ctx, db, url, j); err != nil {
					logger.Warn("resolve candidate job", "error", err)
				}
			}
		}
	}
}

func consumeCollections(ctx context.Context, db *sql.DB, url string, logger *slog.Logger) {
	for ctx.Err() == nil {
		err := consumeOnce(ctx, db, url)
		if err != nil && ctx.Err() == nil {
			logger.Warn("collection consumer disconnected", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func consumeOnce(ctx context.Context, db *sql.DB, url string) error {
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
	if _, err = ch.QueueDeclare("collection.result.v1", true, false, false, false, nil); err != nil {
		return err
	}
	if err = ch.Qos(1, 0, false); err != nil {
		return err
	}
	deliveries, err := ch.Consume("collection.result.v1", consumerName, false, false, false, false, nil)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, ok := <-deliveries:
			if !ok {
				return errors.New("collection queue closed")
			}
			e, decodeErr := messaging.Decode(d.Body)
			if decodeErr != nil || e.MessageType != "collection.result" {
				if err = d.Reject(false); err != nil {
					return err
				}
				continue
			}
			bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
			err = RecordCollection(bounded, db, e)
			cancel()
			if err != nil {
				return err
			}
			if err = d.Ack(false); err != nil {
				return err
			}
		}
	}
}

var uuidValidator = validator.New()

func validUUID(value string) bool {
	if uuidValidator.Var(value, "uuid") != nil {
		return false
	}
	parsed, err := uuid.Parse(value)
	return err == nil && parsed.Version() >= 1 && parsed.Version() <= 8 && parsed.Variant() == uuid.RFC4122
}
