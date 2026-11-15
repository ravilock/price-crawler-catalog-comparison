package integration

import (
	"github.com/lib/pq"
	"os"
	"testing"
	"time"
)

func TestCatalogPersistenceTenantAndAuditConstraints(t *testing.T) {
	db := openTestDB(t)
	f := NewCatalogFixtures(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	org, foreign := f.OrganizationID, f.ForeignOrganizationID
	product, foreignProduct := f.Products[0].ID, f.Products[3].ID
	offer := f.Offers[0].ID
	suggestion := "40000000-0000-4000-8000-000000000001"
	pending := "40000000-0000-4000-8000-000000000003"
	decision := "50000000-0000-4000-8000-000000000001"
	for _, v := range []struct {
		query string
		args  []any
	}{
		{`INSERT INTO catalog_product(id,organization_id,name) VALUES($1,$2,'Widget'),($3,$4,'Foreign')`, []any{product, org, foreignProduct, foreign}},
		{`INSERT INTO match_suggestion(id,organization_id,offer_id,candidate_id,proposed_product_id) VALUES($1,$2,$3,$4,$5)`, []any{suggestion, org, offer, "60000000-0000-4000-8000-000000000001", product}},
		{`INSERT INTO match_suggestion(id,organization_id,offer_id,candidate_id) VALUES($1,$2,$3,$4)`, []any{pending, org, offer, "60000000-0000-4000-8000-000000000001"}},
		{`INSERT INTO review_decision(id,organization_id,suggestion_id,offer_id,decision,product_id,reviewer_subject) VALUES($1,$2,$3,$4,'approve',$5,'manager')`, []any{decision, org, suggestion, offer, product}},
		{`INSERT INTO offer_match(organization_id,offer_id,product_id,method,review_decision_id) VALUES($1,$2,$3,'review',$4)`, []any{org, offer, product, decision}},
		{`INSERT INTO comparison_group(id,organization_id,created_by) VALUES($1,$2,'manager')`, []any{"70000000-0000-4000-8000-000000000001", org}},
		{`INSERT INTO catalog_audit_record(id,organization_id,actor_subject,actor_role,action,entity_type,entity_id,after_value) VALUES($1,$2,'manager','manager','match.review','offer',$3,'{}')`, []any{"80000000-0000-4000-8000-000000000001", org, offer}},
	} {
		if _, err := db.Exec(v.query, v.args...); err != nil {
			t.Fatal(err)
		}
	}
	tests := []struct {
		name, query string
		args        []any
	}{
		{"foreign suggested product", `INSERT INTO match_suggestion(id,organization_id,offer_id,candidate_id,proposed_product_id) VALUES($1,$2,$3,$4,$5)`, []any{"40000000-0000-4000-8000-000000000002", org, offer, "60000000-0000-4000-8000-000000000001", foreignProduct}},
		{"foreign association", `INSERT INTO offer_match(organization_id,offer_id,product_id,method) VALUES($1,$2,$3,'gtin')`, []any{org, "30000000-0000-4000-8000-000000000009", foreignProduct}},
		{"review required", `INSERT INTO offer_match(organization_id,offer_id,product_id,method) VALUES($1,$2,$3,'review')`, []any{org, "30000000-0000-4000-8000-000000000010", product}},
		{"decision offer mismatch", `INSERT INTO offer_match(organization_id,offer_id,product_id,method,review_decision_id) VALUES($1,$2,$3,'review',$4)`, []any{org, "30000000-0000-4000-8000-000000000011", product, decision}},
		{"one current association", `INSERT INTO offer_match(organization_id,offer_id,product_id,method) VALUES($1,$2,$3,'gtin')`, []any{org, offer, product}},
		{"duplicate decision", `INSERT INTO review_decision(id,organization_id,suggestion_id,offer_id,decision,reviewer_subject) VALUES($1,$2,$3,$4,'reject','manager')`, []any{"50000000-0000-4000-8000-000000000002", org, suggestion, offer}},
		{"rejection cannot select product", `INSERT INTO review_decision(id,organization_id,suggestion_id,offer_id,decision,product_id,reviewer_subject) VALUES($1,$2,$3,$4,'reject',$5,'manager')`, []any{"50000000-0000-4000-8000-000000000002", org, pending, offer, product}},
		{"approval requires product", `INSERT INTO review_decision(id,organization_id,suggestion_id,offer_id,decision,reviewer_subject) VALUES($1,$2,$3,$4,'approve','manager')`, []any{"50000000-0000-4000-8000-000000000002", org, pending, offer}},
		{"immutable decision", `UPDATE review_decision SET reviewer_subject='other' WHERE id=$1`, []any{decision}},
		{"delete decision", `DELETE FROM review_decision WHERE id=$1`, []any{decision}},
		{"immutable audit", `UPDATE catalog_audit_record SET actor_subject='other' WHERE organization_id=$1`, []any{org}},
		{"positive revision", `UPDATE offer_match SET revision=0 WHERE organization_id=$1 AND offer_id=$2`, []any{org, offer}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := db.Exec(tc.query, tc.args...)
			if err == nil {
				t.Fatal("expected database constraint rejection")
			}
			if _, ok := err.(*pq.Error); !ok {
				t.Fatalf("unexpected database error: %v", err)
			}
		})
	}
}

func TestCatalogMigrationPreservesExistingMessages(t *testing.T) {
	db := openTestDB(t)
	down, err := os.ReadFile("../../migrations/003_pending_match_jobs.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(string(down)); err != nil {
		t.Fatal(err)
	}
	down, err = os.ReadFile("../../migrations/002_catalog_identity.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(string(down)); err != nil {
		t.Fatal(err)
	}
	id := "90000000-0000-4000-8000-000000000001"
	if _, err = db.Exec(`INSERT INTO message_inbox(message_id,consumer_name) VALUES($1,'catalog')`, id); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`INSERT INTO message_outbox(message_id,destination,message_type,organization_id,correlation_id,payload) VALUES($1,'queue','event',$1,$1,'{}')`, id); err != nil {
		t.Fatal(err)
	}
	up, err := os.ReadFile("../../migrations/002_catalog_identity.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(string(up)); err != nil {
		t.Fatal(err)
	}
	up, err = os.ReadFile("../../migrations/003_pending_match_jobs.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(string(up)); err != nil {
		t.Fatal(err)
	}
	var inbox, outbox int
	if err = db.QueryRow(`SELECT (SELECT count(*) FROM message_inbox WHERE message_id=$1),(SELECT count(*) FROM message_outbox WHERE message_id=$1)`, id).Scan(&inbox, &outbox); err != nil || inbox != 1 || outbox != 1 {
		t.Fatalf("inbox=%d outbox=%d err=%v", inbox, outbox, err)
	}
}
