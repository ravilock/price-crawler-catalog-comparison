package comparison

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	amqp "github.com/rabbitmq/amqp091-go"
	"price-crawler/services/catalog-comparison/internal/messaging"
)

type groupActor struct {
	Subject string `json:"subject"`
	Role    string `json:"role"`
}

type groupCommand struct {
	Actor            groupActor `json:"actor"`
	Action           string     `json:"action"`
	GroupID          string     `json:"group_id,omitempty"`
	Name             string     `json:"name,omitempty"`
	ProductIDs       []string   `json:"product_ids,omitempty"`
	ProductID        string     `json:"product_id,omitempty"`
	ExpectedRevision int64      `json:"expected_revision,omitempty"`
	Query            string     `json:"query,omitempty"`
	Cursor           string     `json:"cursor,omitempty"`
	Limit            int        `json:"limit,omitempty"`
}

type comparisonGroup struct {
	ID         string   `json:"group_id"`
	Name       string   `json:"name"`
	State      string   `json:"state"`
	Revision   int64    `json:"revision"`
	ProductIDs []string `json:"product_ids"`
}

func groupReply(request messaging.Envelope, payload any) (messaging.Envelope, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return messaging.Envelope{}, err
	}
	id, err := uuid.NewRandom()
	if err != nil {
		return messaging.Envelope{}, err
	}
	resultType := "catalog.group.result"
	if request.MessageType == "comparison.group.detail.requested" {
		resultType = "comparison.group.detail.result"
	}
	return messaging.Envelope{MessageID: id.String(), MessageType: resultType, SchemaVersion: 1, OccurredAt: time.Now().UTC(), Organization: request.Organization, CorrelationID: request.CorrelationID, CausationID: request.MessageID, Payload: body}, nil
}

func decodeGroupCommand(raw []byte) (groupCommand, error) {
	var c groupCommand
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&c); err != nil {
		return c, err
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return c, errors.New("trailing command data")
	}
	return c, nil
}

func groupRole(role string) bool { return role == "manager" || role == "administrator" }
func groupActorValid(a groupActor) bool {
	return strings.TrimSpace(a.Subject) != "" && len(a.Subject) <= 200
}

func handleGroup(ctx context.Context, db *sql.DB, e messaging.Envelope) (messaging.Envelope, error) {
	c, err := decodeGroupCommand(e.Payload)
	if err != nil {
		return groupReply(e, map[string]any{"status": "invalid"})
	}
	if !groupActorValid(c.Actor) || (!groupRole(c.Actor.Role) && c.Action != "groups.search" && c.Action != "group.read" && c.Action != "products.search") {
		return groupReply(e, map[string]any{"status": "forbidden"})
	}
	if !groupRole(c.Actor.Role) && c.Actor.Role != "end_user" && c.Actor.Role != "auditor" {
		return groupReply(e, map[string]any{"status": "forbidden"})
	}
	if c.Action == "products.search" {
		return searchProducts(ctx, db, e, c)
	}
	if c.Action == "groups.search" {
		return searchGroups(ctx, db, e, c)
	}
	if c.Action == "group.read" {
		return readGroup(ctx, db, e, c)
	}
	if c.Actor.Role == "auditor" || c.Actor.Role == "end_user" {
		return groupReply(e, map[string]any{"status": "forbidden"})
	}
	if len(e.IdempotencyKey) == 0 || len(e.IdempotencyKey) > 200 {
		return groupReply(e, map[string]any{"status": "invalid"})
	}
	if c.Action != "group.create" && (!validUUID(c.GroupID) || c.ExpectedRevision < 1) {
		return groupReply(e, map[string]any{"status": "invalid"})
	}
	if c.Action == "group.create" && (strings.TrimSpace(c.Name) == "" || len(c.Name) > 200 || len(c.ProductIDs) < 1 || len(c.ProductIDs) > 100) {
		return groupReply(e, map[string]any{"status": "invalid"})
	}
	if c.Action == "group.products.add" && (len(c.ProductIDs) < 1 || len(c.ProductIDs) > 100) {
		return groupReply(e, map[string]any{"status": "invalid"})
	}
	if c.Action == "group.product.remove" && !validUUID(c.ProductID) {
		return groupReply(e, map[string]any{"status": "invalid"})
	}
	if c.Action != "group.create" && c.Action != "group.products.add" && c.Action != "group.product.remove" && c.Action != "group.validate" {
		return groupReply(e, map[string]any{"status": "invalid"})
	}
	return mutateGroup(ctx, db, e, c)
}

func mutateGroup(ctx context.Context, db *sql.DB, e messaging.Envelope, c groupCommand) (messaging.Envelope, error) {
	body, _ := json.Marshal(c)
	hash := sha256.Sum256(body)
	hashText := hex.EncodeToString(hash[:])
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return messaging.Envelope{}, err
	}
	defer func() { _ = tx.Rollback() }()
	lockKey := e.Organization + ":" + c.Actor.Subject + ":" + c.Action + ":" + e.IdempotencyKey
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, lockKey); err != nil {
		return messaging.Envelope{}, err
	}
	var storedHash string
	var storedResult []byte
	err = tx.QueryRowContext(ctx, `SELECT request_hash,result FROM comparison_group_mutation WHERE organization_id=$1 AND actor_subject=$2 AND operation=$3 AND idempotency_key=$4 FOR UPDATE`, e.Organization, c.Actor.Subject, c.Action, e.IdempotencyKey).Scan(&storedHash, &storedResult)
	if err == nil {
		if storedHash != hashText {
			return groupReply(e, map[string]any{"status": "conflict"})
		}
		var result map[string]any
		if json.Unmarshal(storedResult, &result) != nil {
			return messaging.Envelope{}, errors.New("stored group mutation result is corrupt")
		}
		r, re := groupReply(e, result)
		if re != nil {
			return r, re
		}
		if err = tx.Commit(); err != nil {
			return messaging.Envelope{}, err
		}
		return r, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return messaging.Envelope{}, err
	}
	groupID := c.GroupID
	if c.Action == "group.create" {
		id, e2 := uuid.NewRandom()
		if e2 != nil {
			return messaging.Envelope{}, e2
		}
		groupID = id.String()
	}
	var g comparisonGroup
	if c.Action == "group.create" {
		if err = checkProducts(ctx, tx, e.Organization, c.ProductIDs); err != nil {
			if errors.Is(err, errInvalidProducts) {
				return groupReply(e, map[string]any{"status": "invalid"})
			}
			return messaging.Envelope{}, err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO comparison_group(id,organization_id,created_by,name,state,revision) VALUES($1,$2,$3,$4,'draft',1)`, groupID, e.Organization, c.Actor.Subject, strings.TrimSpace(c.Name))
		if err != nil {
			return messaging.Envelope{}, err
		}
		for _, pid := range c.ProductIDs {
			if _, err = tx.ExecContext(ctx, `INSERT INTO comparison_group_product(organization_id,group_id,product_id) VALUES($1,$2,$3)`, e.Organization, groupID, pid); err != nil {
				return messaging.Envelope{}, err
			}
		}
	} else {
		var state string
		var revision int64
		err = tx.QueryRowContext(ctx, `SELECT name,state,revision FROM comparison_group WHERE organization_id=$1 AND id=$2 FOR UPDATE`, e.Organization, groupID).Scan(&g.Name, &state, &revision)
		if errors.Is(err, sql.ErrNoRows) {
			var exists bool
			if qerr := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM comparison_group WHERE id=$1)`, groupID).Scan(&exists); qerr != nil {
				return messaging.Envelope{}, qerr
			}
			status := "not_found"
			if exists {
				status = "forbidden"
			}
			return groupReply(e, map[string]any{"status": status})
		}
		if err != nil {
			return messaging.Envelope{}, err
		}
		if revision != c.ExpectedRevision {
			return groupReply(e, map[string]any{"status": "conflict"})
		}
		var ids []string
		ids, err = loadProductIDs(ctx, tx, e.Organization, groupID)
		if err != nil {
			return messaging.Envelope{}, err
		}
		switch c.Action {
		case "group.products.add":
			if err = checkProducts(ctx, tx, e.Organization, c.ProductIDs); err != nil {
				if errors.Is(err, errInvalidProducts) {
					return groupReply(e, map[string]any{"status": "invalid"})
				}
				return messaging.Envelope{}, err
			}
			set := map[string]bool{}
			for _, id := range ids {
				set[id] = true
			}
			for _, id := range c.ProductIDs {
				if !set[id] {
					ids = append(ids, id)
					set[id] = true
				}
			}
			if len(ids) > 100 {
				return groupReply(e, map[string]any{"status": "invalid"})
			}
			if state == "validated" {
				for _, id := range c.ProductIDs {
					var canonical bool
					if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM catalog_product WHERE organization_id=$1 AND id=$2)`, e.Organization, id).Scan(&canonical); err != nil {
						return messaging.Envelope{}, err
					}
					if !canonical {
						return groupReply(e, map[string]any{"status": "conflict"})
					}
				}
			}
			for _, id := range c.ProductIDs {
				if _, err = tx.ExecContext(ctx, `INSERT INTO comparison_group_product(organization_id,group_id,product_id) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`, e.Organization, groupID, id); err != nil {
					return messaging.Envelope{}, err
				}
			}
		case "group.product.remove":
			var found bool
			for _, id := range ids {
				if id == c.ProductID {
					found = true
					break
				}
			}
			if !found {
				return groupReply(e, map[string]any{"status": "not_found"})
			}
			if len(ids) <= 1 {
				return groupReply(e, map[string]any{"status": "conflict"})
			}
			if _, err = tx.ExecContext(ctx, `DELETE FROM comparison_group_product WHERE organization_id=$1 AND group_id=$2 AND product_id=$3`, e.Organization, groupID, c.ProductID); err != nil {
				return messaging.Envelope{}, err
			}
		case "group.validate":
			if len(ids) < 1 {
				return groupReply(e, map[string]any{"status": "conflict"})
			}
			state = "validated"
		}
		newRevision := revision + 1
		if _, err = tx.ExecContext(ctx, `UPDATE comparison_group SET state=$3,revision=$4,updated_at=now() WHERE organization_id=$1 AND id=$2`, e.Organization, groupID, state, newRevision); err != nil {
			return messaging.Envelope{}, err
		}
	}
	var state string
	var revision int64
	if err = tx.QueryRowContext(ctx, `SELECT name,state,revision FROM comparison_group WHERE organization_id=$1 AND id=$2`, e.Organization, groupID).Scan(&g.Name, &state, &revision); err != nil {
		return messaging.Envelope{}, err
	}
	ids, err := loadProductIDs(ctx, tx, e.Organization, groupID)
	if err != nil {
		return messaging.Envelope{}, err
	}
	g.ID = groupID
	g.State = state
	g.Revision = revision
	g.ProductIDs = ids
	result := map[string]any{"status": "ok", "group_id": g.ID, "revision": g.Revision, "state": g.State, "product_ids": g.ProductIDs}
	resultJSON, _ := json.Marshal(result)
	if _, err = tx.ExecContext(ctx, `INSERT INTO comparison_group_mutation(organization_id,actor_subject,operation,idempotency_key,request_hash,result) VALUES($1,$2,$3,$4,$5,$6)`, e.Organization, c.Actor.Subject, c.Action, e.IdempotencyKey, hashText, resultJSON); err != nil {
		return messaging.Envelope{}, err
	}
	beforeJSON, _ := json.Marshal(map[string]any{"revision": revision - 1})
	afterJSON, _ := json.Marshal(g)
	auditID := uuid.NewString()
	if _, err = tx.ExecContext(ctx, `INSERT INTO catalog_audit_record(id,organization_id,actor_subject,actor_role,action,entity_type,entity_id,before_value,after_value) VALUES($1,$2,$3,$4,$5,'comparison_group',$6,$7,$8)`, auditID, e.Organization, c.Actor.Subject, c.Actor.Role, "comparison."+c.Action, groupID, beforeJSON, afterJSON); err != nil {
		return messaging.Envelope{}, err
	}
	if err = appendGroupJournal(ctx, tx, e.Organization, g); err != nil {
		return messaging.Envelope{}, err
	}
	if err = enqueueGroupChanged(ctx, tx, e, g); err != nil {
		return messaging.Envelope{}, err
	}
	reply, err := groupReply(e, result)
	if err != nil {
		return reply, err
	}
	if err = tx.Commit(); err != nil {
		return messaging.Envelope{}, err
	}
	return reply, nil
}

var errInvalidProducts = errors.New("invalid tenant products")

func checkProducts(ctx context.Context, tx *sql.Tx, org string, ids []string) error {
	seen := map[string]bool{}
	for _, id := range ids {
		if !validUUID(id) || seen[id] {
			return errInvalidProducts
		}
		seen[id] = true
	}
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM catalog_product WHERE organization_id=$1 AND id=ANY($2::uuid[])`, org, pq.Array(ids)).Scan(&count); err != nil {
		return err
	}
	if count != len(ids) {
		return errInvalidProducts
	}
	return nil
}
func loadProductIDs(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, org, gid string) ([]string, error) {
	rows, err := q.QueryContext(ctx, `SELECT product_id::text FROM comparison_group_product WHERE organization_id=$1 AND group_id=$2 ORDER BY product_id`, org, gid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func appendGroupJournal(ctx context.Context, tx *sql.Tx, org string, g comparisonGroup) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO comparison_membership_journal_counter(organization_id,current_sequence) VALUES($1,0) ON CONFLICT DO NOTHING`, org); err != nil {
		return err
	}
	var seq int64
	if err := tx.QueryRowContext(ctx, `UPDATE comparison_membership_journal_counter SET current_sequence=current_sequence+1 WHERE organization_id=$1 RETURNING current_sequence`, org).Scan(&seq); err != nil {
		return err
	}
	payload, err := json.Marshal(map[string]any{"group_id": g.ID, "revision": g.Revision, "state": g.State, "product_ids": g.ProductIDs})
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO comparison_membership_journal(organization_id,sequence,event_type,entity_id,revision,payload) VALUES($1,$2,'group',$3,$4,$5)`, org, seq, g.ID, g.Revision, payload)
	return err
}

func enqueueGroupChanged(ctx context.Context, tx *sql.Tx, e messaging.Envelope, g comparisonGroup) error {
	payload, err := json.Marshal(map[string]any{"group_id": g.ID, "revision": g.Revision, "state": g.State, "product_ids": g.ProductIDs})
	if err != nil {
		return err
	}
	id := uuid.NewString()
	env := messaging.Envelope{MessageID: id, MessageType: "comparison.group.changed", SchemaVersion: 1, OccurredAt: time.Now().UTC(), Organization: e.Organization, CorrelationID: e.CorrelationID, CausationID: e.MessageID, Payload: payload}
	return messaging.EnqueueOutbox(ctx, tx, env, "comparison.group.changed.v1")
}

func searchProducts(ctx context.Context, db *sql.DB, e messaging.Envelope, c groupCommand) (messaging.Envelope, error) {
	if c.Limit < 1 || c.Limit > 100 || len(c.Query) > 200 || len(c.Cursor) > 200 {
		return groupReply(e, map[string]any{"status": "invalid"})
	}
	offset := 0
	if c.Cursor != "" {
		if _, err := fmt.Sscanf(c.Cursor, "%d", &offset); err != nil || offset < 0 {
			return groupReply(e, map[string]any{"status": "invalid"})
		}
	}
	rows, err := db.QueryContext(ctx, `SELECT id::text,name,brand,manufacturer_model,gtin FROM catalog_product WHERE organization_id=$1 AND ($2='' OR name ILIKE '%'||$2||'%' OR COALESCE(brand,'') ILIKE '%'||$2||'%' OR COALESCE(manufacturer_model,'') ILIKE '%'||$2||'%' OR COALESCE(gtin,'') ILIKE '%'||$2||'%') ORDER BY name,id OFFSET $3 LIMIT $4`, e.Organization, c.Query, offset, c.Limit+1)
	if err != nil {
		return messaging.Envelope{}, err
	}
	defer rows.Close()
	items := []map[string]any{}
	for rows.Next() {
		var id, name string
		var brand, model, gtin sql.NullString
		if err = rows.Scan(&id, &name, &brand, &model, &gtin); err != nil {
			return messaging.Envelope{}, err
		}
		items = append(items, map[string]any{"product_id": id, "name": name, "brand": nullable(brand), "model": nullable(model), "gtin": nullable(gtin)})
	}
	if err = rows.Err(); err != nil {
		return messaging.Envelope{}, err
	}
	more := len(items) > c.Limit
	if more {
		items = items[:c.Limit]
	}
	var next any
	if more {
		next = fmt.Sprint(offset + c.Limit)
	}
	return groupReply(e, map[string]any{"status": "ok", "items": items, "next_cursor": next})
}
func nullable(v sql.NullString) any {
	if !v.Valid {
		return nil
	}
	return v.String
}

func searchGroups(ctx context.Context, db *sql.DB, e messaging.Envelope, c groupCommand) (messaging.Envelope, error) {
	if c.Limit < 1 || c.Limit > 100 || len(c.Query) > 200 || len(c.Cursor) > 200 {
		return groupReply(e, map[string]any{"status": "invalid"})
	}
	offset := 0
	if c.Cursor != "" {
		if _, err := fmt.Sscanf(c.Cursor, "%d", &offset); err != nil || offset < 0 {
			return groupReply(e, map[string]any{"status": "invalid"})
		}
	}
	validatedOnly := c.Actor.Role == "end_user"
	rows, err := db.QueryContext(ctx, `SELECT id::text,name,state,revision FROM comparison_group WHERE organization_id=$1 AND ($2=false OR state='validated') AND ($3='' OR name ILIKE '%'||$3||'%') ORDER BY name,id OFFSET $4 LIMIT $5`, e.Organization, validatedOnly, c.Query, offset, c.Limit+1)
	if err != nil {
		return messaging.Envelope{}, err
	}
	defer rows.Close()
	items := []comparisonGroup{}
	for rows.Next() {
		var g comparisonGroup
		if err = rows.Scan(&g.ID, &g.Name, &g.State, &g.Revision); err != nil {
			return messaging.Envelope{}, err
		}
		g.ProductIDs, err = loadProductIDs(ctx, db, e.Organization, g.ID)
		if err != nil {
			return messaging.Envelope{}, err
		}
		items = append(items, g)
	}
	if err = rows.Err(); err != nil {
		return messaging.Envelope{}, err
	}
	more := len(items) > c.Limit
	if more {
		items = items[:c.Limit]
	}
	var next any
	if more {
		next = fmt.Sprint(offset + c.Limit)
	}
	return groupReply(e, map[string]any{"status": "ok", "items": items, "next_cursor": next})
}

func readGroup(ctx context.Context, db *sql.DB, e messaging.Envelope, c groupCommand) (messaging.Envelope, error) {
	if !validUUID(c.GroupID) {
		return groupReply(e, map[string]any{"status": "invalid"})
	}
	var g comparisonGroup
	err := db.QueryRowContext(ctx, `SELECT id::text,name,state,revision FROM comparison_group WHERE organization_id=$1 AND id=$2`, e.Organization, c.GroupID).Scan(&g.ID, &g.Name, &g.State, &g.Revision)
	if errors.Is(err, sql.ErrNoRows) {
		var exists bool
		if er := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM comparison_group WHERE id=$1)`, c.GroupID).Scan(&exists); er != nil {
			return messaging.Envelope{}, er
		}
		status := "not_found"
		if exists {
			status = "forbidden"
		}
		return groupReply(e, map[string]any{"status": status})
	}
	if err != nil {
		return messaging.Envelope{}, err
	}
	if c.Actor.Role == "end_user" && g.State != "validated" {
		return groupReply(e, map[string]any{"status": "forbidden"})
	}
	g.ProductIDs, err = loadProductIDs(ctx, db, e.Organization, g.ID)
	if err != nil {
		return messaging.Envelope{}, err
	}
	asOf := time.Now().UTC()
	items, err := rankedOffers(ctx, db, e.Organization, g.ProductIDs, asOf)
	if err != nil {
		return messaging.Envelope{}, err
	}
	result := map[string]any{"status": "ok", "group_id": g.ID, "revision": g.Revision, "state": g.State, "product_ids": g.ProductIDs}
	if e.MessageType == "comparison.group.detail.requested" {
		result = map[string]any{"status": "ok", "group_id": g.ID, "name": g.Name, "state": g.State, "revision": g.Revision, "as_of": asOf, "items": items, "next_cursor": nil}
	}
	return groupReply(e, result)
}

type rankedOffer struct {
	OfferID          string     `json:"offer_id"`
	ProductID        string     `json:"product_id"`
	SourceID         string     `json:"source_id"`
	PriceCents       *int64     `json:"price_cents"`
	Availability     string     `json:"availability"`
	ObservedAt       *time.Time `json:"observed_at"`
	Freshness        string     `json:"freshness"`
	CollectionStatus string     `json:"collection_status"`
	Currency         string     `json:"currency"`
	Classification   string     `json:"classification"`
}

func rankedOffers(ctx context.Context, db *sql.DB, org string, products []string, asOf time.Time) ([]rankedOffer, error) {
	rows, err := db.QueryContext(ctx, `SELECT m.offer_id::text,m.product_id::text,p.source_id::text,p.price_cents,p.observation_availability,p.observed_at,p.freshness_window_seconds,p.rankable,p.latest_outcome
		FROM offer_match m JOIN offer_ranking_projection p ON p.organization_id=m.organization_id AND p.offer_id=m.offer_id
		WHERE m.organization_id=$1 AND m.product_id=ANY($2::uuid[]) ORDER BY m.offer_id`, org, pq.Array(products))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []rankedOffer{}
	for rows.Next() {
		var item rankedOffer
		var source, availability, outcome sql.NullString
		var cents, window sql.NullInt64
		var observed sql.NullTime
		var rankable bool
		if err = rows.Scan(&item.OfferID, &item.ProductID, &source, &cents, &availability, &observed, &window, &rankable, &outcome); err != nil {
			return nil, err
		}
		item.SourceID = source.String
		item.Currency = "BRL"
		item.Availability = "unknown"
		item.CollectionStatus = "unknown"
		if outcome.Valid {
			item.CollectionStatus = outcome.String
		}
		item.Freshness = "unknown"
		if observed.Valid {
			value := observed.Time.UTC()
			item.ObservedAt = &value
		}
		if availability.Valid {
			item.Availability = availability.String
		}
		if cents.Valid {
			value := cents.Int64
			item.PriceCents = &value
		}
		current := observed.Valid && window.Valid && window.Int64 > 0 && !observed.Time.After(asOf) && !asOf.After(observed.Time.Add(time.Duration(window.Int64)*time.Second))
		if observed.Valid && window.Valid && window.Int64 > 0 {
			if current {
				item.Freshness = "current"
			} else {
				item.Freshness = "stale"
			}
		}
		switch {
		case !rankable:
			item.Classification = "not_ready"
		case item.Availability == "unavailable":
			item.Classification = "unavailable"
		case !cents.Valid:
			item.Classification = "unverified"
		case !current:
			item.Classification = "stale"
		case item.Availability == "unknown":
			item.Classification = "unknown_availability"
		case item.Availability == "available":
			item.Classification = "ranked"
		default:
			item.Classification = "unverified"
		}
		items = append(items, item)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(items, func(i, j int) bool {
		a, b := items[i], items[j]
		if a.Classification == "ranked" && b.Classification != "ranked" {
			return true
		}
		if a.Classification != "ranked" && b.Classification == "ranked" {
			return false
		}
		if a.Classification == "ranked" && b.Classification == "ranked" && *a.PriceCents != *b.PriceCents {
			return *a.PriceCents < *b.PriceCents
		}
		if a.Classification != b.Classification {
			return a.Classification < b.Classification
		}
		return a.OfferID < b.OfferID
	})
	return items, nil
}

func RunGroupCommands(ctx context.Context, db *sql.DB, url string, logger *slog.Logger) {
	queues := []string{"catalog.products.search.v1", "comparison.group.create.v1", "comparison.group.products.add.v1", "comparison.group.product.remove.v1", "comparison.group.validate.v1", "comparison.groups.search.v1", "comparison.group.read.v1", "comparison.group.detail.requested.v1"}
	for _, q := range queues {
		q := q
		go func() {
			for ctx.Err() == nil {
				if err := consumeGroupQueue(ctx, db, url, q); err != nil && ctx.Err() == nil {
					logger.Warn("catalog group command consumer disconnected", "queue", q, "error", err)
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
				}
			}
		}()
	}
	<-ctx.Done()
}

func consumeGroupQueue(ctx context.Context, db *sql.DB, url, queue string) error {
	conn, err := amqp.Dial(url)
	if err != nil {
		return err
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()
	dead := queue + ".dead"
	if _, err = ch.QueueDeclare(dead, true, false, false, false, nil); err != nil {
		return err
	}
	if _, err = ch.QueueDeclare(queue, true, false, false, false, amqp.Table{"x-dead-letter-exchange": "", "x-dead-letter-routing-key": dead}); err != nil {
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
	deliveries, err := ch.Consume(queue, "catalog-group-"+queue, false, false, false, false, nil)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, ok := <-deliveries:
			if !ok {
				return errors.New("catalog group queue closed")
			}
			request, de := messaging.Decode(d.Body)
			if de != nil || request.MessageType != strings.TrimSuffix(queue, ".v1") || !strings.HasPrefix(d.ReplyTo, "amq.gen-") || d.CorrelationId != request.CorrelationID {
				if err = d.Reject(false); err != nil {
					return err
				}
				continue
			}
			bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
			reply, pe := handleGroup(bounded, db, request)
			cancel()
			if pe != nil {
				return pe
			}
			encoded, err := reply.Encode()
			if err != nil {
				return err
			}
			if err = ch.PublishWithContext(ctx, "", d.ReplyTo, true, false, amqp.Publishing{ContentType: "application/json", DeliveryMode: amqp.Persistent, MessageId: reply.MessageID, CorrelationId: reply.CorrelationID, Type: reply.MessageType, Body: encoded}); err != nil {
				return err
			}
			select {
			case confirm := <-confirms:
				if !confirm.Ack {
					return errors.New("RabbitMQ did not confirm group reply")
				}
			case <-ctx.Done():
				return ctx.Err()
			}
			select {
			case returned := <-returns:
				return fmt.Errorf("group reply unroutable: %s", returned.RoutingKey)
			default:
			}
			if err = d.Ack(false); err != nil {
				return err
			}
		}
	}
}
