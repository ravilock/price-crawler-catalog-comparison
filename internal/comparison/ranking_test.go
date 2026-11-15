package comparison

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"price-crawler/services/catalog-comparison/internal/messaging"
)

const (
	offerID      = "00000000-0000-4000-8000-000000000401"
	sourceID     = "00000000-0000-4000-8000-000000000402"
	requestID    = "00000000-0000-4000-8000-000000000403"
	observationA = "00000000-0000-4000-8000-000000000404"
	observationB = "00000000-0000-4000-8000-000000000405"
)

var baseTime = time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)

func validFacts() Facts {
	return Facts{
		OfferID:                offerID,
		SourceID:               sourceID,
		OfferRevision:          2,
		SourceRevision:         5,
		FreshnessWindowSeconds: 86400,
		Collection:             &Collection{CrawlRequestID: requestID, Outcome: "partial", CompletedAt: baseTime},
		Observation:            &Observation{ObservationID: observationB, Position: 2, CrawlRequestID: requestID, Availability: "available", ObservedAt: baseTime.Add(time.Minute)},
		Price:                  &Price{ObservationID: observationB, Position: 2, CrawlRequestID: requestID, PriceCents: 13000, Currency: "BRL", ObservedAt: baseTime.Add(time.Minute)},
	}
}

func TestMergeAppliesIndependentComponents(t *testing.T) {
	current := Projection{
		OrganizationID:         "00000000-0000-4000-8000-000000000406",
		OfferID:                offerID,
		SourceID:               sourceID,
		OfferRevision:          2,
		SourceRevision:         5,
		FreshnessWindowSeconds: 86400,
		Collection:             &Collection{CrawlRequestID: requestID, Outcome: "partial", CompletedAt: baseTime},
		Observation:            &Observation{ObservationID: observationA, Position: 1, CrawlRequestID: requestID, Availability: "available", ObservedAt: baseTime},
		Price:                  &Price{ObservationID: observationA, Position: 1, CrawlRequestID: requestID, PriceCents: 12000, Currency: "BRL", ObservedAt: baseTime},
	}

	// A newer offer component with an older source component only advances the
	// offer half of the projection.
	newerOffer := validFacts()
	newerOffer.OfferRevision = 3
	newerOffer.SourceRevision = 4
	newerOffer.FreshnessWindowSeconds = 43200
	merged, err := Merge(&current, newerOffer)
	if err != nil {
		t.Fatalf("merge newer offer: %v", err)
	}
	if merged.OfferRevision != 3 || merged.Observation.ObservationID != observationB || merged.Price.PriceCents != 13000 {
		t.Fatalf("offer component not applied: %+v", merged)
	}
	if merged.SourceRevision != 5 || merged.FreshnessWindowSeconds != 86400 {
		t.Fatalf("older source component overwrote current window: %+v", merged)
	}
	if !merged.Rankable {
		t.Fatal("successful merge must restore rankability")
	}

	// A newer source component with an older offer component only advances the
	// source half.
	newerSource := validFacts()
	newerSource.OfferRevision = 1
	newerSource.SourceRevision = 6
	newerSource.FreshnessWindowSeconds = 43200
	merged, err = Merge(&current, newerSource)
	if err != nil {
		t.Fatalf("merge newer source: %v", err)
	}
	if merged.OfferRevision != 2 || merged.Observation.ObservationID != observationA {
		t.Fatalf("older offer component overwrote current facts: %+v", merged)
	}
	if merged.SourceRevision != 6 || merged.FreshnessWindowSeconds != 43200 {
		t.Fatalf("source component not applied: %+v", merged)
	}

	// Identical equal revisions are an idempotent replay, not an inconsistency.
	merged, err = Merge(&merged, newerOffer)
	if err != nil {
		t.Fatalf("idempotent merge: %v", err)
	}
	if merged.OfferRevision != 3 || merged.SourceRevision != 6 {
		t.Fatalf("idempotent merge changed revisions: %+v", merged)
	}
}

func TestMergeRejectsInconsistentEqualRevision(t *testing.T) {
	current := Projection{OfferID: offerID, SourceID: sourceID, OfferRevision: 2, SourceRevision: 5, FreshnessWindowSeconds: 86400, Rankable: true}

	differentPrice := validFacts()
	differentPrice.Price = &Price{ObservationID: observationB, Position: 2, CrawlRequestID: requestID, PriceCents: 999, Currency: "BRL", ObservedAt: baseTime.Add(time.Minute)}
	if _, err := Merge(&current, differentPrice); !errors.Is(err, ErrInconsistent) {
		t.Fatalf("equal offer revision with different facts err=%v", err)
	}

	differentWindow := validFacts()
	differentWindow.OfferRevision = 2
	differentWindow.FreshnessWindowSeconds = 1000
	if _, err := Merge(&current, differentWindow); !errors.Is(err, ErrInconsistent) {
		t.Fatalf("equal source revision with different window err=%v", err)
	}

	differentSource := validFacts()
	differentSource.SourceID = "00000000-0000-4000-8000-000000000407"
	if _, err := Merge(&current, differentSource); !errors.Is(err, ErrInconsistent) {
		t.Fatalf("source identity change err=%v", err)
	}
}

func TestMergeIgnoresOlderComponents(t *testing.T) {
	current := Projection{
		OfferID:                offerID,
		SourceID:               sourceID,
		OfferRevision:          3,
		SourceRevision:         7,
		FreshnessWindowSeconds: 86400,
		Observation:            &Observation{ObservationID: observationB, Position: 2, CrawlRequestID: requestID, Availability: "available", ObservedAt: baseTime},
		Price:                  &Price{ObservationID: observationB, Position: 2, CrawlRequestID: requestID, PriceCents: 13000, Currency: "BRL", ObservedAt: baseTime},
		Rankable:               true,
	}
	stale := validFacts()
	stale.OfferRevision = 2
	stale.SourceRevision = 5
	stale.FreshnessWindowSeconds = 43200
	stale.Observation = &Observation{ObservationID: observationA, Position: 1, CrawlRequestID: requestID, Availability: "available", ObservedAt: baseTime}
	stale.Price = &Price{ObservationID: observationA, Position: 1, CrawlRequestID: requestID, PriceCents: 12000, Currency: "BRL", ObservedAt: baseTime}
	merged, err := Merge(&current, stale)
	if err != nil {
		t.Fatalf("stale merge: %v", err)
	}
	if merged.OfferRevision != 3 || merged.Observation.ObservationID != observationB || merged.Price.PriceCents != 13000 {
		t.Fatalf("stale offer component was applied: %+v", merged)
	}
	if merged.SourceRevision != 7 || merged.FreshnessWindowSeconds != 86400 {
		t.Fatalf("stale source component was applied: %+v", merged)
	}
}

func TestValidateFactsRejectsCrossFieldViolations(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Facts)
	}{
		{"price without observation", func(f *Facts) { f.Observation = nil }},
		{"price newer than observation", func(f *Facts) { f.Price.Position = 3 }},
		{"equal position with different identity", func(f *Facts) { f.Price.ObservationID = observationA }},
		{"equal position with different time", func(f *Facts) { f.Price.ObservedAt = baseTime.Add(2 * time.Minute) }},
		{"non BRL price", func(f *Facts) { f.Price.Currency = "USD" }},
		{"negative price", func(f *Facts) { f.Price.PriceCents = -1 }},
		{"unknown collection outcome", func(f *Facts) { f.Collection.Outcome = "done" }},
		{"invalid observation availability", func(f *Facts) { f.Observation.Availability = "in_stock" }},
		{"zero revision", func(f *Facts) { f.OfferRevision = 0 }},
		{"non uuid offer", func(f *Facts) { f.OfferID = "not-a-uuid" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := validFacts()
			tc.mutate(&f)
			if err := validateFacts(f); err == nil {
				t.Fatal("expected validation failure")
			}
		})
	}
	zero := validFacts()
	zero.Price.PriceCents = 0
	if err := validateFacts(zero); err != nil {
		t.Fatalf("zero is a valid price: %v", err)
	}
}

func TestDecodeRankingReplyEnforcesCorrelationAndShape(t *testing.T) {
	request := messaging.Envelope{MessageID: "00000000-0000-4000-8000-000000000408", CorrelationID: "00000000-0000-4000-8000-000000000409"}
	okPayload := map[string]any{
		"status":                   "ok",
		"offer_id":                 offerID,
		"source_id":                sourceID,
		"offer_revision":           2,
		"source_revision":          5,
		"freshness_window_seconds": 86400,
		"latest_collection":        map[string]any{"crawl_request_id": requestID, "outcome": "partial", "completed_at": baseTime},
		"latest_observation":       map[string]any{"observation_id": observationB, "position": 2, "crawl_request_id": requestID, "availability": "available", "observed_at": baseTime.Add(time.Minute)},
		"last_verified_cash_price": map[string]any{"observation_id": observationB, "position": 2, "crawl_request_id": requestID, "price_cents": 13000, "currency": "BRL", "observed_at": baseTime.Add(time.Minute)},
	}
	reply := func(payload map[string]any) messaging.Envelope {
		body, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		return messaging.Envelope{MessageType: "ranking.facts.read.result", SchemaVersion: 1, Organization: "00000000-0000-4000-8000-000000000410", CorrelationID: request.CorrelationID, CausationID: request.MessageID, Payload: body}
	}

	facts, status, err := decodeRankingReply(reply(okPayload), request, "00000000-0000-4000-8000-000000000410", offerID)
	if err != nil || status != "" || facts == nil || facts.Price.PriceCents != 13000 {
		t.Fatalf("ok reply facts=%+v status=%q err=%v", facts, status, err)
	}

	for _, value := range []string{"pending", "not_found", "forbidden", "not_ready", "invalid"} {
		_, status, err := decodeRankingReply(reply(map[string]any{"status": value, "offer_id": offerID}), request, "00000000-0000-4000-8000-000000000410", offerID)
		if err != nil || status != value {
			t.Fatalf("non-ok %s status=%q err=%v", value, status, err)
		}
	}

	tests := []struct {
		name   string
		mutate func(*messaging.Envelope)
	}{
		{"wrong organization", func(e *messaging.Envelope) { e.Organization = "00000000-0000-4000-8000-000000000411" }},
		{"wrong correlation", func(e *messaging.Envelope) { e.CorrelationID = "00000000-0000-4000-8000-000000000412" }},
		{"wrong causation", func(e *messaging.Envelope) { e.CausationID = "00000000-0000-4000-8000-000000000413" }},
		{"wrong type", func(e *messaging.Envelope) { e.MessageType = "ranking.facts.read.requested" }},
		{"wrong schema", func(e *messaging.Envelope) { e.SchemaVersion = 2 }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bad := reply(okPayload)
			tc.mutate(&bad)
			if _, _, err := decodeRankingReply(bad, request, "00000000-0000-4000-8000-000000000410", offerID); err == nil {
				t.Fatal("expected envelope rejection")
			}
		})
	}

	unknown := reply(okPayload)
	unknown.Payload = append(unknown.Payload[:len(unknown.Payload)-1], []byte(`,"unexpected":true}`)...)
	if _, _, err := decodeRankingReply(unknown, request, "00000000-0000-4000-8000-000000000410", offerID); err == nil {
		t.Fatal("expected unknown field rejection")
	}

	if _, _, err := decodeRankingReply(reply(okPayload), request, "00000000-0000-4000-8000-000000000410", "00000000-0000-4000-8000-000000000414"); err == nil {
		t.Fatal("expected offer identity rejection")
	}

	newerPrice := map[string]any{}
	for key, value := range okPayload {
		newerPrice[key] = value
	}
	newerPrice["last_verified_cash_price"] = map[string]any{"observation_id": observationA, "position": 3, "crawl_request_id": requestID, "price_cents": 13000, "currency": "BRL", "observed_at": baseTime}
	if _, _, err := decodeRankingReply(reply(newerPrice), request, "00000000-0000-4000-8000-000000000410", offerID); err == nil {
		t.Fatal("expected cross-field price rejection")
	}
}

func TestRankableAtRequiresRecentAuthoritativeRead(t *testing.T) {
	now := baseTime
	p := Projection{Rankable: true, ValidatedAt: now.Add(-time.Minute)}
	if !p.RankableAt(now, 5*time.Minute) {
		t.Fatal("recent read must rank")
	}
	if !p.RankableAt(now, time.Minute) {
		t.Fatal("age equal to the window is still fresh")
	}
	if p.RankableAt(now, 30*time.Second) {
		t.Fatal("age greater than the window must be stale")
	}
	if p.RankableAt(now, 0) {
		t.Fatal("zero window must not rank")
	}
	p.Rankable = false
	if p.RankableAt(now, 5*time.Minute) {
		t.Fatal("not ready projection must not rank")
	}
}
