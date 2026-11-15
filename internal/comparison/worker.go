package comparison

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"price-crawler/services/catalog-comparison/internal/messaging"
)

const (
	refreshQueue        = "ranking.facts.read.requested.v1"
	sourcePolicyQueue   = "source.policy.changed.v1"
	refreshConsumerName = "catalog-ranking-refresh"
	policyConsumerName  = "catalog-ranking-policy"
	maxRefreshAttempts  = 5
	refreshBatch        = 10
	sweepBatch          = 100
)

// ErrTerminalRefresh reports a non-ok reply that a retry cannot fix. The
// projection stays not ready for ranking until a later collection or policy
// event enqueues a new authoritative read.
var errTerminalRefresh = errors.New("terminal ranking refresh outcome")

type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

type refreshJob struct {
	ID, Organization, OfferID string
	Attempts                  int
}

type rankingReply struct {
	Status                 string       `json:"status"`
	OfferID                string       `json:"offer_id"`
	SourceID               string       `json:"source_id"`
	OfferRevision          int64        `json:"offer_revision"`
	SourceRevision         int64        `json:"source_revision"`
	FreshnessWindowSeconds int          `json:"freshness_window_seconds"`
	LatestCollection       *Collection  `json:"latest_collection"`
	LatestObservation      *Observation `json:"latest_observation"`
	LastVerifiedCashPrice  *Price       `json:"last_verified_cash_price"`
}

// EnqueueRefresh records durable refresh intent in the caller's transaction so
// a collection result cannot be acknowledged without scheduling its read.
func EnqueueRefresh(ctx context.Context, tx execer, org, offerID string) error {
	if tx == nil || !validUUID(org) || !validUUID(offerID) {
		return errors.New("invalid refresh intent")
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO ranking_refresh_job(organization_id,offer_id) VALUES($1,$2)
 ON CONFLICT (organization_id,offer_id) DO UPDATE
 SET attempts=0,next_attempt_at=now(),terminal=false,last_error=NULL,updated_at=now()`, org, offerID)
	return err
}

// InvalidateAll runs at startup: a restarted consumer cannot prove that any
// stored projection is still current, so every projection stops ranking until
// an authoritative read succeeds.
func InvalidateAll(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `WITH invalidated AS (
 UPDATE offer_ranking_projection
 SET rankable=false,last_error='revalidation_required',updated_at=now()
 RETURNING organization_id,offer_id
)
INSERT INTO ranking_refresh_job(organization_id,offer_id)
SELECT organization_id,offer_id FROM invalidated
ON CONFLICT (organization_id,offer_id) DO UPDATE
SET attempts=0,next_attempt_at=now(),terminal=false,last_error=NULL,updated_at=now()`)
	return err
}

// InvalidateSource marks the affected projections not ready and schedules a
// read. A projection that already holds a strictly newer source revision is
// left alone because that component was read after this change.
func InvalidateSource(ctx context.Context, tx execer, org, sourceID string, sourceRevision int64) error {
	if tx == nil || !validUUID(org) || !validUUID(sourceID) || sourceRevision < 1 {
		return errors.New("invalid source policy change")
	}
	_, err := tx.ExecContext(ctx, `WITH affected AS (
 UPDATE offer_ranking_projection
 SET rankable=false,last_error='source_policy_changed',updated_at=now()
 WHERE organization_id=$1 AND source_id=$2 AND source_revision<=$3
 RETURNING organization_id,offer_id
)
INSERT INTO ranking_refresh_job(organization_id,offer_id)
SELECT organization_id,offer_id FROM affected
ON CONFLICT (organization_id,offer_id) DO UPDATE
SET attempts=0,next_attempt_at=now(),terminal=false,last_error=NULL,updated_at=now()`, org, sourceID, sourceRevision)
	return err
}

// MarkNotReady retains historical display facts but removes the projection from
// ranking until a later authoritative read succeeds.
func MarkNotReady(ctx context.Context, db *sql.DB, org, offerID, code string) error {
	if len(code) > 200 {
		code = code[:200]
	}
	_, err := db.ExecContext(ctx, `UPDATE offer_ranking_projection SET rankable=false,last_error=$3,updated_at=now() WHERE organization_id=$1 AND offer_id=$2`, org, offerID, code)
	return err
}

// EnqueueStale re-arms bounded recovery work. It schedules projections that
// have not been authoritatively read inside the revalidation window, re-arms
// jobs whose bounded retries were exhausted, and leaves terminal outcomes
// alone until new work arrives.
func EnqueueStale(ctx context.Context, db *sql.DB, window time.Duration) error {
	seconds := int(window / time.Second)
	if seconds < 1 {
		seconds = 1
	}
	if _, err := db.ExecContext(ctx, `WITH stale AS (
 SELECT p.organization_id,p.offer_id
 FROM offer_ranking_projection p
 WHERE p.validated_at < now() - make_interval(secs => $1)
   AND NOT EXISTS (
     SELECT 1 FROM ranking_refresh_job j
     WHERE j.organization_id=p.organization_id AND j.offer_id=p.offer_id
       AND (j.next_attempt_at IS NOT NULL OR j.terminal)
   )
 ORDER BY p.validated_at
 LIMIT $2
)
INSERT INTO ranking_refresh_job(organization_id,offer_id)
SELECT organization_id,offer_id FROM stale
ON CONFLICT (organization_id,offer_id) DO UPDATE
SET attempts=0,next_attempt_at=now(),terminal=false,last_error=NULL,updated_at=now()`, seconds, sweepBatch); err != nil {
		return err
	}
	// A first read can exhaust its retries before any projection exists. Re-arm
	// that job so a lost collection notification is still recoverable.
	_, err := db.ExecContext(ctx, `UPDATE ranking_refresh_job j
 SET attempts=0,next_attempt_at=now(),last_error=NULL,updated_at=now()
 WHERE j.next_attempt_at IS NULL AND NOT j.terminal
   AND NOT EXISTS (
     SELECT 1 FROM offer_ranking_projection p
     WHERE p.organization_id=j.organization_id AND p.offer_id=j.offer_id
   )`)
	return err
}

func claimRefresh(ctx context.Context, db *sql.DB) (refreshJob, error) {
	var j refreshJob
	err := db.QueryRowContext(ctx, `WITH due AS (
 SELECT id FROM ranking_refresh_job
 WHERE next_attempt_at IS NOT NULL AND next_attempt_at<=now()
 ORDER BY next_attempt_at,created_at
 FOR UPDATE SKIP LOCKED LIMIT 1
)
UPDATE ranking_refresh_job j
SET attempts=j.attempts+1,next_attempt_at=now()+interval '30 seconds',updated_at=now()
FROM due WHERE j.id=due.id
RETURNING j.id::text,j.organization_id::text,j.offer_id::text,j.attempts`).Scan(&j.ID, &j.Organization, &j.OfferID, &j.Attempts)
	return j, err
}

func completeRefresh(ctx context.Context, db *sql.DB, j refreshJob) error {
	_, err := db.ExecContext(ctx, `UPDATE ranking_refresh_job SET attempts=0,next_attempt_at=NULL,terminal=false,last_error=NULL,updated_at=now() WHERE id=$1 AND attempts=$2`, j.ID, j.Attempts)
	return err
}

// terminalRefresh records an outcome that no retry can fix. New work resets the
// flag when it enqueues the next authoritative read.
func terminalRefresh(ctx context.Context, db *sql.DB, j refreshJob, code string) error {
	if len(code) > 200 {
		code = code[:200]
	}
	_, err := db.ExecContext(ctx, `UPDATE ranking_refresh_job SET next_attempt_at=NULL,terminal=true,last_error=$3,updated_at=now() WHERE id=$1 AND attempts=$2`, j.ID, j.Attempts, code)
	return err
}

func retryRefresh(ctx context.Context, db *sql.DB, j refreshJob, code string) error {
	if len(code) > 200 {
		code = code[:200]
	}
	if j.Attempts >= maxRefreshAttempts {
		_, err := db.ExecContext(ctx, `UPDATE ranking_refresh_job SET next_attempt_at=NULL,terminal=false,last_error=$3,updated_at=now() WHERE id=$1 AND attempts=$2`, j.ID, j.Attempts, code)
		return err
	}
	_, err := db.ExecContext(ctx, `UPDATE ranking_refresh_job SET next_attempt_at=$3,last_error=$4,updated_at=now() WHERE id=$1 AND attempts=$2`, j.ID, j.Attempts, time.Now().Add(messaging.RetryDelay(j.Attempts)), code)
	return err
}

func validUUID(value string) bool {
	parsed, err := uuid.Parse(value)
	return err == nil && parsed.String() == value
}

// ReadFacts performs the authenticated bounded request/reply. A non-empty
// status is a well-formed non-ok reply; err covers transport and protocol
// failures only.
func ReadFacts(ctx context.Context, url, org, offerID string) (*Facts, string, error) {
	if !validUUID(org) || !validUUID(offerID) {
		return nil, "", errors.New("invalid ranking read identity")
	}
	payload, err := json.Marshal(map[string]string{"offer_id": offerID})
	if err != nil {
		return nil, "", err
	}
	messageID := uuid.NewString()
	request := messaging.Envelope{MessageID: messageID, MessageType: "ranking.facts.read.requested", SchemaVersion: 1, OccurredAt: time.Now().UTC(), Organization: org, CorrelationID: messageID, Payload: payload}
	reply, err := messaging.RequestReply(ctx, url, refreshQueue, request, 10*time.Second)
	if err != nil {
		return nil, "", err
	}
	return decodeRankingReply(reply, request, org, offerID)
}

// decodeRankingReply validates the correlation chain and the exact ok or non-ok
// payload shape before any fact is stored.
func decodeRankingReply(reply, request messaging.Envelope, org, offerID string) (*Facts, string, error) {
	if reply.MessageType != "ranking.facts.read.result" || reply.SchemaVersion != 1 || reply.Organization != org || reply.CorrelationID != request.CorrelationID || reply.CausationID != request.MessageID {
		return nil, "", errors.New("invalid ranking reply envelope")
	}
	var r rankingReply
	decoder := json.NewDecoder(bytes.NewReader(reply.Payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&r); err != nil || decoder.Decode(new(any)) != io.EOF {
		return nil, "", errors.New("invalid ranking reply payload")
	}
	if r.OfferID != offerID {
		return nil, "", errors.New("ranking reply identity mismatch")
	}
	switch r.Status {
	case "ok":
		facts := Facts{
			OfferID:                r.OfferID,
			SourceID:               r.SourceID,
			OfferRevision:          r.OfferRevision,
			SourceRevision:         r.SourceRevision,
			FreshnessWindowSeconds: r.FreshnessWindowSeconds,
			Collection:             r.LatestCollection,
			Observation:            r.LatestObservation,
			Price:                  r.LastVerifiedCashPrice,
		}
		if err := validateFacts(facts); err != nil {
			return nil, "", fmt.Errorf("invalid ranking facts: %w", err)
		}
		return &facts, "", nil
	case "pending", "not_found", "forbidden", "not_ready", "invalid":
		return nil, r.Status, nil
	default:
		return nil, "", errors.New("invalid ranking reply status")
	}
}

func refresh(ctx context.Context, db *sql.DB, url string, j refreshJob, now time.Time) error {
	facts, status, err := ReadFacts(ctx, url, j.Organization, j.OfferID)
	if err != nil {
		_ = MarkNotReady(ctx, db, j.Organization, j.OfferID, "transport_unavailable")
		return retryRefresh(ctx, db, j, "transport_unavailable")
	}
	if status != "" {
		_ = MarkNotReady(ctx, db, j.Organization, j.OfferID, status)
		switch status {
		case "not_found", "forbidden", "invalid":
			_ = terminalRefresh(ctx, db, j, status)
			return fmt.Errorf("%w: %s", errTerminalRefresh, status)
		default:
			return retryRefresh(ctx, db, j, status)
		}
	}
	if err = Apply(ctx, db, j.Organization, *facts, now); err != nil {
		code := "apply_failed"
		if errors.Is(err, ErrInconsistent) {
			code = "inconsistent"
		}
		_ = MarkNotReady(ctx, db, j.Organization, j.OfferID, code)
		return retryRefresh(ctx, db, j, code)
	}
	return completeRefresh(ctx, db, j)
}

// Run starts startup invalidation, the durable source-policy consumer, bounded
// refresh processing and the revalidation sweep.
func Run(ctx context.Context, db *sql.DB, url string, logger *slog.Logger, revalidateAfter time.Duration) {
	if revalidateAfter <= 0 {
		revalidateAfter = 15 * time.Minute
	}
	startup, cancel := context.WithTimeout(ctx, 30*time.Second)
	err := InvalidateAll(startup, db)
	cancel()
	if err != nil && ctx.Err() == nil {
		logger.Warn("ranking projection startup invalidation failed", "error", err)
	}
	go runSourcePolicy(ctx, db, url, logger)
	sweepEvery := revalidateAfter
	if sweepEvery > 30*time.Second {
		sweepEvery = 30 * time.Second
	}
	if sweepEvery < time.Second {
		sweepEvery = time.Second
	}
	jobs := time.NewTicker(time.Second)
	defer jobs.Stop()
	sweep := time.NewTicker(sweepEvery)
	defer sweep.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-sweep.C:
			bounded, done := context.WithTimeout(ctx, 10*time.Second)
			err := EnqueueStale(bounded, db, revalidateAfter)
			done()
			if err != nil && ctx.Err() == nil {
				logger.Warn("ranking revalidation sweep failed", "error", err)
			}
		case <-jobs.C:
			for i := 0; i < refreshBatch; i++ {
				j, claimErr := claimRefresh(ctx, db)
				if errors.Is(claimErr, sql.ErrNoRows) {
					break
				}
				if claimErr != nil {
					logger.Warn("claim ranking refresh job", "error", claimErr)
					break
				}
				bounded, done := context.WithTimeout(ctx, 15*time.Second)
				err := refresh(bounded, db, url, j, time.Now().UTC())
				done()
				if err != nil && !errors.Is(err, errTerminalRefresh) && ctx.Err() == nil {
					logger.Warn("ranking refresh failed", "organization_id", j.Organization, "offer_id", j.OfferID, "error", err)
				}
			}
		}
	}
}

type policyChanged struct {
	SourceID               string `json:"source_id"`
	SourceRevision         int64  `json:"source_revision"`
	FreshnessWindowSeconds int    `json:"freshness_window_seconds"`
	Active                 bool   `json:"active"`
}

func runSourcePolicy(ctx context.Context, db *sql.DB, url string, logger *slog.Logger) {
	for ctx.Err() == nil {
		if err := consumeSourcePolicy(ctx, db, url); err != nil && ctx.Err() == nil {
			logger.Warn("source policy consumer disconnected", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func consumeSourcePolicy(ctx context.Context, db *sql.DB, url string) error {
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
	if _, err = ch.QueueDeclare(sourcePolicyQueue, true, false, false, false, nil); err != nil {
		return err
	}
	if err = ch.Qos(1, 0, false); err != nil {
		return err
	}
	deliveries, err := ch.Consume(sourcePolicyQueue, policyConsumerName, false, false, false, false, nil)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case delivery, ok := <-deliveries:
			if !ok {
				return errors.New("source policy queue closed")
			}
			e, decodeErr := messaging.Decode(delivery.Body)
			var change policyChanged
			if decodeErr != nil || e.MessageType != "source.policy.changed" || decodePayload(e.Payload, &change) != nil ||
				!validUUID(change.SourceID) || change.SourceRevision < 1 || change.FreshnessWindowSeconds < 1 {
				if err = delivery.Reject(false); err != nil {
					return err
				}
				continue
			}
			bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
			err = recordSourcePolicy(bounded, db, e, change)
			cancel()
			if err != nil {
				return err
			}
			if err = delivery.Ack(false); err != nil {
				return err
			}
		}
	}
}

func recordSourcePolicy(ctx context.Context, db *sql.DB, e messaging.Envelope, change policyChanged) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	fresh, err := messaging.BeginInbox(ctx, tx, e.MessageID, policyConsumerName)
	if err != nil {
		return err
	}
	if !fresh {
		return tx.Commit()
	}
	if err = InvalidateSource(ctx, tx, e.Organization, change.SourceID, change.SourceRevision); err != nil {
		return err
	}
	return tx.Commit()
}

func decodePayload(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return errors.New("trailing payload")
	}
	return nil
}
