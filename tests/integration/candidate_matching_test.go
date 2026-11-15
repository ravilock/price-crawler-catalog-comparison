package integration

import (
	"context"
	"encoding/json"
	"testing"

	"price-crawler/services/catalog-comparison/internal/matching"
)

func TestCandidateReplayMatchesExactResolvedTupleOnlyOnce(t *testing.T) {
	db := openTestDB(t)
	org, offer := "10000000-0000-4000-8000-000000000071", "30000000-0000-4000-8000-000000000071"
	request, candidate, product := "40000000-0000-4000-8000-000000000071", "50000000-0000-4000-8000-000000000071", "20000000-0000-4000-8000-000000000071"
	if _, err := db.Exec(`INSERT INTO catalog_product(id,organization_id,name,gtin,brand,manufacturer_model) VALUES($1,$2,'Widget','7891234567895','Acme','W-1')`, product, org); err != nil {
		t.Fatal(err)
	}
	resolved := map[string]any{"offer_id": offer, "crawl_request_id": request, "candidate_id": candidate, "fields": map[string]any{"gtin": map[string]any{"status": "valid", "normalized_value": "7891234567895"}}, "attributes": []any{}}
	raw, _ := json.Marshal(resolved)
	job := "60000000-0000-4000-8000-000000000071"
	if _, err := db.Exec(`INSERT INTO pending_match_job(id,organization_id,offer_id,crawl_request_id,candidate_id,status,attempts,next_attempt_at,resolved_candidate) VALUES($1,$2,$3,$4,$5,'ready',1,NULL,$6)`, job, org, offer, request, candidate, raw); err != nil {
		t.Fatal(err)
	}
	if err := matching.ProcessReady(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM offer_match WHERE organization_id=$1 AND offer_id=$2 AND product_id=$3`, org, offer, product).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("association count = %d", count)
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM pending_match_job WHERE id=$1`, job).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "matched" {
		t.Fatalf("job status = %s", status)
	}
	if err := matching.ProcessReady(context.Background(), db); err == nil {
		t.Fatal("replayed matched job was processed again")
	}
	if err := db.QueryRow(`SELECT count(*) FROM offer_match WHERE organization_id=$1 AND offer_id=$2`, org, offer).Scan(&count); err != nil || count != 1 {
		t.Fatalf("association replay count=%d err=%v", count, err)
	}
}
