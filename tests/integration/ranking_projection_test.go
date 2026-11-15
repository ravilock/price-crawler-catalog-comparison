package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"price-crawler/services/catalog-comparison/internal/comparison"
	"price-crawler/services/catalog-comparison/internal/matching"
	"price-crawler/services/catalog-comparison/internal/messaging"
)

func rankingFacts(offer, source string, now time.Time) comparison.Facts {
	request := "90000000-0000-4000-8000-000000000001"
	observation := "90000000-0000-4000-8000-000000000002"
	return comparison.Facts{
		OfferID:                offer,
		SourceID:               source,
		OfferRevision:          2,
		SourceRevision:         5,
		FreshnessWindowSeconds: 86400,
		Collection:             &comparison.Collection{CrawlRequestID: request, Outcome: "partial", CompletedAt: now.Add(-time.Hour)},
		Observation:            &comparison.Observation{ObservationID: observation, Position: 1, CrawlRequestID: request, Availability: "available", ObservedAt: now.Add(-time.Hour)},
		Price:                  &comparison.Price{ObservationID: observation, Position: 1, CrawlRequestID: request, PriceCents: 12000, Currency: "BRL", ObservedAt: now.Add(-time.Hour)},
	}
}

func TestRankingProjectionMergesIndependentRevisions(t *testing.T) {
	db := openTestDB(t)
	f := NewCatalogFixtures(time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC))
	org, offer, source := f.OrganizationID, f.Offers[0].ID, "90000000-0000-4000-8000-000000000010"
	now := f.Now

	facts := rankingFacts(offer, source, now)
	if err := comparison.Apply(context.Background(), db, org, facts, now); err != nil {
		t.Fatalf("apply initial facts: %v", err)
	}
	projection, found, err := comparison.LoadProjection(context.Background(), db, org, offer)
	if err != nil || !found {
		t.Fatalf("load projection found=%t err=%v", found, err)
	}
	if !projection.Rankable || projection.OfferRevision != 2 || projection.SourceRevision != 5 || projection.FreshnessWindowSeconds != 86400 {
		t.Fatalf("initial projection: %+v", projection)
	}
	if projection.Price == nil || projection.Price.PriceCents != 12000 || !projection.ValidatedAt.Equal(now) {
		t.Fatalf("initial price or validation time: %+v", projection)
	}

	// A delayed older offer component is ignored without touching the source
	// component.
	older := facts
	older.OfferRevision = 1
	older.Observation = &comparison.Observation{ObservationID: "90000000-0000-4000-8000-000000000011", Position: 1, CrawlRequestID: facts.Observation.CrawlRequestID, Availability: "available", ObservedAt: now.Add(-2 * time.Hour)}
	older.Price = &comparison.Price{ObservationID: older.Observation.ObservationID, Position: 1, CrawlRequestID: facts.Observation.CrawlRequestID, PriceCents: 9000, Currency: "BRL", ObservedAt: now.Add(-2 * time.Hour)}
	if err = comparison.Apply(context.Background(), db, org, older, now.Add(time.Minute)); err != nil {
		t.Fatalf("apply older offer facts: %v", err)
	}
	projection, _, _ = comparison.LoadProjection(context.Background(), db, org, offer)
	if projection.OfferRevision != 2 || projection.Price.PriceCents != 12000 {
		t.Fatalf("older offer component overwrote current facts: %+v", projection)
	}

	// A source-policy change advances only the source component.
	newer := facts
	newer.SourceRevision = 6
	newer.FreshnessWindowSeconds = 43200
	if err = comparison.Apply(context.Background(), db, org, newer, now.Add(2*time.Minute)); err != nil {
		t.Fatalf("apply newer source facts: %v", err)
	}
	projection, _, _ = comparison.LoadProjection(context.Background(), db, org, offer)
	if projection.SourceRevision != 6 || projection.FreshnessWindowSeconds != 43200 || projection.OfferRevision != 2 {
		t.Fatalf("source component merge: %+v", projection)
	}

	// An equal revision with different content is an inconsistency, never an
	// overwrite.
	contradictory := facts
	contradictory.Price = &comparison.Price{ObservationID: facts.Observation.ObservationID, Position: 1, CrawlRequestID: facts.Observation.CrawlRequestID, PriceCents: 1, Currency: "BRL", ObservedAt: facts.Observation.ObservedAt}
	if err = comparison.Apply(context.Background(), db, org, contradictory, now.Add(3*time.Minute)); !errors.Is(err, comparison.ErrInconsistent) {
		t.Fatalf("contradictory equal revision err=%v", err)
	}
	projection, _, _ = comparison.LoadProjection(context.Background(), db, org, offer)
	if projection.Price.PriceCents != 12000 {
		t.Fatalf("inconsistent reply was stored: %+v", projection)
	}

	// A failed-only offer has no collection, observation or price; those facts
	// must stay NULL rather than zero or a fabricated currency.
	empty := comparison.Facts{OfferID: f.Offers[1].ID, SourceID: source, OfferRevision: 1, SourceRevision: 1, FreshnessWindowSeconds: 86400}
	if err = comparison.Apply(context.Background(), db, org, empty, now.Add(4*time.Minute)); err != nil {
		t.Fatalf("apply empty facts: %v", err)
	}
	projection, found, err = comparison.LoadProjection(context.Background(), db, org, f.Offers[1].ID)
	if err != nil || !found {
		t.Fatalf("load empty projection found=%t err=%v", found, err)
	}
	if projection.Collection != nil || projection.Observation != nil || projection.Price != nil || !projection.Rankable {
		t.Fatalf("empty facts were not preserved as null: %+v", projection)
	}
}

func TestRankingProjectionInvalidatesSourceAndQueuesRecovery(t *testing.T) {
	db := openTestDB(t)
	f := NewCatalogFixtures(time.Date(2026, 11, 6, 13, 0, 0, 0, time.UTC))
	org, source := f.OrganizationID, "90000000-0000-4000-8000-000000000020"
	offers := []string{f.Offers[0].ID, f.Offers[1].ID, f.Offers[2].ID}
	for i, offer := range offers {
		facts := rankingFacts(offer, source, f.Now)
		if i == 2 {
			facts.SourceRevision = 7
		}
		if err := comparison.Apply(context.Background(), db, org, facts, f.Now); err != nil {
			t.Fatalf("apply offer %d: %v", i, err)
		}
	}

	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = comparison.InvalidateSource(context.Background(), tx, org, source, 5); err != nil {
		t.Fatalf("invalidate source: %v", err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	for i, offer := range offers {
		p, found, loadErr := comparison.LoadProjection(context.Background(), db, org, offer)
		if loadErr != nil || !found {
			t.Fatalf("load offer %d found=%t err=%v", i, found, loadErr)
		}
		if i < 2 {
			if p.Rankable || p.ValidatedAt.IsZero() {
				t.Fatalf("affected projection stayed rankable: %+v", p)
			}
			var code string
			if err = db.QueryRow(`SELECT COALESCE(last_error,'') FROM offer_ranking_projection WHERE organization_id=$1 AND offer_id=$2`, org, offer).Scan(&code); err != nil || code != "source_policy_changed" {
				t.Fatalf("affected projection error code=%q err=%v", code, err)
			}
		} else if !p.Rankable {
			t.Fatalf("newer source revision was invalidated by an older event: %+v", p)
		}
	}
	var jobs int
	if err = db.QueryRow(`SELECT count(*) FROM ranking_refresh_job WHERE organization_id=$1 AND offer_id IN ($2,$3)`, org, offers[0], offers[1]).Scan(&jobs); err != nil || jobs != 2 {
		t.Fatalf("recovery jobs=%d err=%v", jobs, err)
	}

	// Duplicate delivery is idempotent: the same job is reset, not duplicated.
	for i := 0; i < 2; i++ {
		tx, err = db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if err = comparison.EnqueueRefresh(context.Background(), tx, org, offers[0]); err != nil {
			t.Fatalf("enqueue refresh: %v", err)
		}
		if err = tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	if err = db.QueryRow(`SELECT count(*) FROM ranking_refresh_job WHERE organization_id=$1 AND offer_id=$2`, org, offers[0]).Scan(&jobs); err != nil || jobs != 1 {
		t.Fatalf("duplicate enqueue jobs=%d err=%v", jobs, err)
	}

	if err = comparison.MarkNotReady(context.Background(), db, org, offers[2], "inconsistent"); err != nil {
		t.Fatalf("mark not ready: %v", err)
	}
	p, _, _ := comparison.LoadProjection(context.Background(), db, org, offers[2])
	if p.Rankable {
		t.Fatalf("mark not ready left projection rankable: %+v", p)
	}

	if err = comparison.InvalidateAll(context.Background(), db); err != nil {
		t.Fatalf("startup invalidation: %v", err)
	}
	var rankable int
	if err = db.QueryRow(`SELECT count(*) FROM offer_ranking_projection WHERE organization_id=$1 AND rankable`, org).Scan(&rankable); err != nil || rankable != 0 {
		t.Fatalf("startup invalidation left rankable rows=%d err=%v", rankable, err)
	}
}

func TestRankingProjectionRevalidatesStaleRows(t *testing.T) {
	db := openTestDB(t)
	f := NewCatalogFixtures(time.Date(2026, 11, 6, 14, 0, 0, 0, time.UTC))
	org, source := f.OrganizationID, "90000000-0000-4000-8000-000000000030"
	staleOffer, freshOffer := f.Offers[0].ID, f.Offers[1].ID
	now := time.Now().UTC()
	if err := comparison.Apply(context.Background(), db, org, rankingFacts(staleOffer, source, now.Add(-2*time.Hour)), now.Add(-2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := comparison.Apply(context.Background(), db, org, rankingFacts(freshOffer, source, now), now); err != nil {
		t.Fatal(err)
	}
	if err := comparison.EnqueueStale(context.Background(), db, time.Hour); err != nil {
		t.Fatalf("revalidate stale rows: %v", err)
	}
	var staleJobs, freshJobs int
	if err := db.QueryRow(`SELECT count(*) FROM ranking_refresh_job WHERE organization_id=$1 AND offer_id=$2`, org, staleOffer).Scan(&staleJobs); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM ranking_refresh_job WHERE organization_id=$1 AND offer_id=$2`, org, freshOffer).Scan(&freshJobs); err != nil {
		t.Fatal(err)
	}
	if staleJobs != 1 || freshJobs != 0 {
		t.Fatalf("stale jobs=%d fresh jobs=%d", staleJobs, freshJobs)
	}

	// A dead job is re-armed by the sweep, while an outstanding job is skipped.
	if _, err := db.Exec(`UPDATE ranking_refresh_job SET attempts=5,next_attempt_at=NULL WHERE organization_id=$1 AND offer_id=$2`, org, staleOffer); err != nil {
		t.Fatal(err)
	}
	if err := comparison.EnqueueStale(context.Background(), db, time.Hour); err != nil {
		t.Fatalf("re-arm dead job: %v", err)
	}
	var attempts int
	var next sql.NullTime
	if err := db.QueryRow(`SELECT attempts,next_attempt_at FROM ranking_refresh_job WHERE organization_id=$1 AND offer_id=$2`, org, staleOffer).Scan(&attempts, &next); err != nil {
		t.Fatal(err)
	}
	if attempts != 0 || !next.Valid {
		t.Fatalf("dead job not re-armed attempts=%d next=%v", attempts, next)
	}
	if err := comparison.EnqueueStale(context.Background(), db, time.Hour); err != nil {
		t.Fatalf("skip outstanding job: %v", err)
	}
	var jobs int
	if err := db.QueryRow(`SELECT count(*) FROM ranking_refresh_job WHERE organization_id=$1 AND offer_id=$2`, org, staleOffer).Scan(&jobs); err != nil || jobs != 1 {
		t.Fatalf("outstanding job duplicated jobs=%d err=%v", jobs, err)
	}

	// A terminal non-ok outcome is not re-armed by the sweep; only new work
	// (a collection result or source-policy event) may schedule another read.
	terminalOffer := f.Offers[2].ID
	if err := comparison.Apply(context.Background(), db, org, rankingFacts(terminalOffer, source, now.Add(-2*time.Hour)), now.Add(-2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := comparison.MarkNotReady(context.Background(), db, org, terminalOffer, "forbidden"); err != nil {
		t.Fatalf("mark not ready: %v", err)
	}
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = comparison.EnqueueRefresh(context.Background(), tx, org, terminalOffer); err != nil {
		t.Fatalf("enqueue terminal read: %v", err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE ranking_refresh_job SET attempts=1,next_attempt_at=NULL,terminal=true WHERE organization_id=$1 AND offer_id=$2`, org, terminalOffer); err != nil {
		t.Fatal(err)
	}
	if err := comparison.EnqueueStale(context.Background(), db, time.Hour); err != nil {
		t.Fatalf("sweep terminal row: %v", err)
	}
	var terminal bool
	if err := db.QueryRow(`SELECT next_attempt_at,terminal FROM ranking_refresh_job WHERE organization_id=$1 AND offer_id=$2`, org, terminalOffer).Scan(&next, &terminal); err != nil {
		t.Fatal(err)
	}
	if next.Valid || !terminal {
		t.Fatalf("terminal row re-armed next=%v terminal=%t", next, terminal)
	}
	tx, err = db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = comparison.EnqueueRefresh(context.Background(), tx, org, terminalOffer); err != nil {
		t.Fatalf("new work after terminal: %v", err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT next_attempt_at,terminal FROM ranking_refresh_job WHERE organization_id=$1 AND offer_id=$2`, org, terminalOffer).Scan(&next, &terminal); err != nil {
		t.Fatal(err)
	}
	if !next.Valid || terminal {
		t.Fatalf("new work did not clear terminal next=%v terminal=%t", next, terminal)
	}

	// A first read can exhaust its retries before any projection exists. The
	// sweep re-arms that job, but not one that already ended terminally.
	unprojected := "90000000-0000-4000-8000-000000000051"
	if _, err := db.Exec(`INSERT INTO ranking_refresh_job(organization_id,offer_id,attempts,next_attempt_at,terminal) VALUES($1,$2,5,NULL,false),($1,$3,5,NULL,true)`, org, unprojected, "90000000-0000-4000-8000-000000000052"); err != nil {
		t.Fatal(err)
	}
	if err := comparison.EnqueueStale(context.Background(), db, time.Hour); err != nil {
		t.Fatalf("sweep unprojected jobs: %v", err)
	}
	if err := db.QueryRow(`SELECT attempts,next_attempt_at FROM ranking_refresh_job WHERE organization_id=$1 AND offer_id=$2`, org, unprojected).Scan(&attempts, &next); err != nil {
		t.Fatal(err)
	}
	if attempts != 0 || !next.Valid {
		t.Fatalf("exhausted unprojected job not re-armed attempts=%d next=%v", attempts, next)
	}
	if err := db.QueryRow(`SELECT next_attempt_at FROM ranking_refresh_job WHERE organization_id=$1 AND offer_id=$2`, org, "90000000-0000-4000-8000-000000000052").Scan(&next); err != nil {
		t.Fatal(err)
	}
	if next.Valid {
		t.Fatalf("terminal unprojected job re-armed next=%v", next)
	}
}

func TestCollectionResultEnqueuesRankingRefresh(t *testing.T) {
	db := openTestDB(t)
	f := NewCatalogFixtures(time.Date(2026, 11, 6, 15, 0, 0, 0, time.UTC))
	org, offer := f.OrganizationID, f.Offers[0].ID
	request := "90000000-0000-4000-8000-000000000041"
	messageID := "90000000-0000-4000-8000-000000000042"
	payload, err := json.Marshal(map[string]any{"offer_id": offer, "crawl_request_id": request, "outcome": "failed", "currency": "BRL"})
	if err != nil {
		t.Fatal(err)
	}
	e := messaging.Envelope{MessageID: messageID, MessageType: "collection.result", SchemaVersion: 1, OccurredAt: f.Now, Organization: org, CorrelationID: messageID, Payload: payload}
	if err = matching.RecordCollection(context.Background(), db, e); err != nil {
		t.Fatalf("record failed collection: %v", err)
	}
	var jobs, inbox int
	if err = db.QueryRow(`SELECT count(*) FROM ranking_refresh_job WHERE organization_id=$1 AND offer_id=$2`, org, offer).Scan(&jobs); err != nil || jobs != 1 {
		t.Fatalf("ranking job after collection jobs=%d err=%v", jobs, err)
	}
	if err = db.QueryRow(`SELECT count(*) FROM message_inbox WHERE message_id=$1`, messageID).Scan(&inbox); err != nil || inbox != 1 {
		t.Fatalf("collection inbox rows=%d err=%v", inbox, err)
	}
	// A duplicate delivery is acknowledged from the inbox without a second job.
	if err = matching.RecordCollection(context.Background(), db, e); err != nil {
		t.Fatalf("replay collection: %v", err)
	}
	if err = db.QueryRow(`SELECT count(*) FROM ranking_refresh_job WHERE organization_id=$1 AND offer_id=$2`, org, offer).Scan(&jobs); err != nil || jobs != 1 {
		t.Fatalf("replay created another job jobs=%d err=%v", jobs, err)
	}

	// A successful collection records both its candidate job and the ranking
	// refresh in one transaction.
	successRequest := "90000000-0000-4000-8000-000000000043"
	successID := "90000000-0000-4000-8000-000000000044"
	candidate := "90000000-0000-4000-8000-000000000045"
	payload, err = json.Marshal(map[string]any{"offer_id": f.Offers[1].ID, "crawl_request_id": successRequest, "candidate_id": candidate, "outcome": "partial"})
	if err != nil {
		t.Fatal(err)
	}
	success := messaging.Envelope{MessageID: successID, MessageType: "collection.result", SchemaVersion: 1, OccurredAt: f.Now, Organization: org, CorrelationID: successID, Payload: payload}
	if err = matching.RecordCollection(context.Background(), db, success); err != nil {
		t.Fatalf("record successful collection: %v", err)
	}
	var pending int
	if err = db.QueryRow(`SELECT count(*) FROM pending_match_job WHERE organization_id=$1 AND offer_id=$2`, org, f.Offers[1].ID).Scan(&pending); err != nil || pending != 1 {
		t.Fatalf("pending match jobs=%d err=%v", pending, err)
	}
	if err = db.QueryRow(`SELECT count(*) FROM ranking_refresh_job WHERE organization_id=$1 AND offer_id=$2`, org, f.Offers[1].ID).Scan(&jobs); err != nil || jobs != 1 {
		t.Fatalf("successful collection ranking job jobs=%d err=%v", jobs, err)
	}
}
