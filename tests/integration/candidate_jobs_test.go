package integration

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"price-crawler/services/catalog-comparison/internal/matching"
	"price-crawler/services/catalog-comparison/internal/messaging"
)

func TestCollectionCandidateJobIsExactAndIdempotent(t *testing.T) {
	db := openTestDB(t)
	org := "10000000-0000-4000-8000-000000000001"
	offer := "30000000-0000-4000-8000-000000000001"
	request := "40000000-0000-4000-8000-000000000001"
	candidate := "50000000-0000-4000-8000-000000000001"
	event := messaging.Envelope{MessageID: "60000000-0000-4000-8000-000000000001", MessageType: "collection.result", SchemaVersion: 1, OccurredAt: time.Now(), Organization: org, CorrelationID: request, Payload: json.RawMessage(`{"offer_id":"` + offer + `","crawl_request_id":"` + request + `","candidate_id":"` + candidate + `","outcome":"complete"}`)}
	for i := 0; i < 2; i++ {
		if err := matching.RecordCollection(context.Background(), db, event); err != nil {
			t.Fatal(err)
		}
	}
	var inbox, jobs int
	if err := db.QueryRow(`SELECT count(*) FROM message_inbox WHERE message_id=$1`, event.MessageID).Scan(&inbox); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM pending_match_job WHERE organization_id=$1 AND offer_id=$2 AND crawl_request_id=$3 AND candidate_id=$4`, org, offer, request, candidate).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if inbox != 1 || jobs != 1 {
		t.Fatalf("inbox=%d jobs=%d", inbox, jobs)
	}
	failed := event
	failed.MessageID = "60000000-0000-4000-8000-000000000002"
	failed.Payload = json.RawMessage(`{"offer_id":"` + offer + `","crawl_request_id":"40000000-0000-4000-8000-000000000002","outcome":"failed"}`)
	if err := matching.RecordCollection(context.Background(), db, failed); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM pending_match_job`).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if jobs != 1 {
		t.Fatalf("failed collection created job: %d", jobs)
	}
}
