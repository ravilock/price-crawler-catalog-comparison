package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"price-crawler/services/catalog-comparison/internal/messaging"
	"price-crawler/services/catalog-comparison/internal/review"
)

func TestReviewApprovalSelectionAndCreationAreAtomicAndIdempotent(t *testing.T) {
	db := openTestDB(t)
	f := NewCatalogFixtures(time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC))
	seedReviewProduct(t, db, f.OrganizationID, f.Products[0])
	seedReviewSuggestion(t, db, f.OrganizationID, f.Offers[1].ID, "60000000-0000-4000-8000-000000000001", "40000000-0000-4000-8000-000000000001")
	seedReviewSuggestion(t, db, f.OrganizationID, f.Offers[2].ID, "60000000-0000-4000-8000-000000000002", "40000000-0000-4000-8000-000000000002")

	selected := reviewEnvelope(t, f.OrganizationID, f.ManagerSubject, "approve", "select", f.Products[0].ID, "")
	selectedResult, err := review.Decide(context.Background(), db, selected)
	if err != nil || selectedResult["status"] != "ok" || selectedResult["product_id"] != f.Products[0].ID {
		t.Fatalf("select existing product: result=%v err=%v", selectedResult, err)
	}
	firstDecision := selectedResult["decision_id"]
	selected.MessageID = "70000000-0000-4000-8000-000000000002"
	selected.CorrelationID = selected.MessageID
	replayed, err := review.Decide(context.Background(), db, selected)
	if err != nil || replayed["status"] != "ok" || replayed["decision_id"] != firstDecision {
		t.Fatalf("idempotent replay: result=%v err=%v", replayed, err)
	}

	created := reviewEnvelope(t, f.OrganizationID, f.ManagerSubject, "approve", "create", "Northstar Router N-5 rev B", "")
	createdResult, err := review.Decide(context.Background(), db, created)
	if err != nil || createdResult["status"] != "ok" || createdResult["product_id"] == nil {
		t.Fatalf("create canonical product: result=%v err=%v", createdResult, err)
	}

	var matches, decisions, audits, associationEvents, auditEvents int
	if err = db.QueryRow(`SELECT count(*) FROM offer_match WHERE organization_id=$1 AND method='review'`, f.OrganizationID).Scan(&matches); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow(`SELECT count(*) FROM review_decision WHERE organization_id=$1 AND decision='approve'`, f.OrganizationID).Scan(&decisions); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow(`SELECT count(*) FROM catalog_audit_record WHERE organization_id=$1 AND action='catalog.review.approve'`, f.OrganizationID).Scan(&audits); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow(`SELECT count(*) FROM message_outbox WHERE organization_id=$1 AND message_type='catalog.offer.association.changed'`, f.OrganizationID).Scan(&associationEvents); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow(`SELECT count(*) FROM message_outbox WHERE organization_id=$1 AND message_type='audit.event.recorded'`, f.OrganizationID).Scan(&auditEvents); err != nil {
		t.Fatal(err)
	}
	if matches != 2 || decisions != 2 || audits != 2 || associationEvents != 2 || auditEvents != 2 {
		t.Fatalf("non-atomic review result matches=%d decisions=%d audits=%d association_events=%d audit_events=%d", matches, decisions, audits, associationEvents, auditEvents)
	}
}

func TestReviewRejectionRecordsDecisionWithoutAssociation(t *testing.T) {
	db := openTestDB(t)
	f := NewCatalogFixtures(time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC))
	offer := f.Offers[1].ID
	suggestionID := "40000000-0000-4000-8000-000000000003"
	candidateID := "60000000-0000-4000-8000-000000000003"
	seedReviewSuggestion(t, db, f.OrganizationID, offer, candidateID, suggestionID)
	request := reviewEnvelope(t, f.OrganizationID, f.ManagerSubject, "reject", "", "", "")
	result, err := review.Decide(context.Background(), db, request)
	if err != nil || result["status"] != "ok" || result["product_id"] != nil {
		t.Fatalf("reject suggestion: result=%v err=%v", result, err)
	}
	var associations, rejections, rejectedJobs int
	if err = db.QueryRow(`SELECT count(*) FROM offer_match WHERE organization_id=$1 AND offer_id=$2`, f.OrganizationID, offer).Scan(&associations); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow(`SELECT count(*) FROM review_decision WHERE organization_id=$1 AND suggestion_id=$2 AND decision='reject'`, f.OrganizationID, suggestionID).Scan(&rejections); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow(`SELECT count(*) FROM pending_match_job WHERE organization_id=$1 AND offer_id=$2 AND candidate_id=$3 AND status='rejected' AND decision_id IS NOT NULL`, f.OrganizationID, offer, candidateID).Scan(&rejectedJobs); err != nil {
		t.Fatal(err)
	}
	if associations != 0 || rejections != 1 || rejectedJobs != 1 {
		t.Fatalf("rejection result associations=%d decisions=%d rejected_jobs=%d", associations, rejections, rejectedJobs)
	}
}

func seedReviewProduct(t *testing.T, db *sql.DB, organization string, product ProductFixture) {
	t.Helper()
	_, err := db.Exec(`INSERT INTO catalog_product(id,organization_id,name,gtin,brand,manufacturer_model) VALUES($1,$2,$3,$4,$5,$6)`, product.ID, organization, product.Name, product.GTIN, product.Brand, product.Model)
	if err != nil {
		t.Fatal(err)
	}
}

func seedReviewSuggestion(t *testing.T, db *sql.DB, organization, offer, candidate, suggestion string) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO match_suggestion(id,organization_id,offer_id,candidate_id,title) VALUES($1,$2,$3,$4,'Example offer')`, suggestion, organization, offer, candidate); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO pending_match_job(id,organization_id,offer_id,crawl_request_id,candidate_id,status) VALUES($1,$2,$3,$4,$5,'review')`, "80000000-0000-4000-8000-"+suggestion[len(suggestion)-12:], organization, offer, "50000000-0000-4000-8000-"+suggestion[len(suggestion)-12:], candidate); err != nil {
		t.Fatal(err)
	}
}

func reviewEnvelope(t *testing.T, organization, subject, decision, selectionKind, productID, suggestionID string) messaging.Envelope {
	t.Helper()
	if suggestionID == "" {
		if decision == "reject" {
			suggestionID = "40000000-0000-4000-8000-000000000003"
		} else if selectionKind == "select" {
			suggestionID = "40000000-0000-4000-8000-000000000001"
		} else {
			suggestionID = "40000000-0000-4000-8000-000000000002"
		}
	}
	payload := map[string]any{"actor": map[string]string{"subject": subject, "role": "manager"}, "suggestion_id": suggestionID, "decision": decision}
	if decision == "approve" && selectionKind == "select" {
		payload["selection"] = map[string]string{"kind": "select", "product_id": productID}
	}
	if decision == "approve" && selectionKind == "create" {
		payload["selection"] = map[string]any{"kind": "create", "product": map[string]any{"name": productID, "brand": "Northstar", "model": "N-5B", "gtin": nil}}
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return messaging.Envelope{MessageID: "70000000-0000-4000-8000-000000000001", MessageType: "catalog.review.decide.requested", SchemaVersion: 1, OccurredAt: time.Now().UTC(), Organization: organization, CorrelationID: "70000000-0000-4000-8000-000000000001", IdempotencyKey: "review-key-" + suggestionID, Payload: encoded}
}
