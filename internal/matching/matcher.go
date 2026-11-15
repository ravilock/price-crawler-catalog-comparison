package matching

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"unicode"

	"github.com/google/uuid"
)

type identityField struct {
	Status          string `json:"status"`
	NormalizedValue any    `json:"normalized_value"`
}

type resolvedCandidate struct {
	OfferID        string                   `json:"offer_id"`
	CrawlRequestID string                   `json:"crawl_request_id"`
	CandidateID    string                   `json:"candidate_id"`
	Fields         map[string]identityField `json:"fields"`
	Attributes     []json.RawMessage        `json:"attributes"`
}

type candidateAttribute struct {
	Name  string `json:"name"`
	Value any    `json:"value"`
	Field struct {
		Status          string `json:"status"`
		NormalizedValue any    `json:"normalized_value"`
	} `json:"field"`
}

type canonicalProduct struct {
	ID, Name, GTIN, Brand, Model string
}

var nonWord = regexp.MustCompile(`[^\p{L}\p{N}]+`)

func normalizedText(value string) string {
	return strings.TrimSpace(nonWord.ReplaceAllString(strings.ToLower(value), " "))
}

func normalizedGTIN(value string) string {
	var digits strings.Builder
	for _, r := range value {
		if unicode.IsDigit(r) {
			digits.WriteRune(r)
		} else if !unicode.IsSpace(r) && r != '-' {
			return ""
		}
	}
	return digits.String()
}

func validStringField(fields map[string]identityField, name string) string {
	field, ok := fields[name]
	if !ok || field.Status != "valid" || field.NormalizedValue == nil {
		return ""
	}
	value, ok := field.NormalizedValue.(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(value)
}

func hasVariableVariant(attributes []json.RawMessage) bool {
	for _, raw := range attributes {
		var attribute candidateAttribute
		if json.Unmarshal(raw, &attribute) != nil {
			continue
		}
		name := normalizedText(attribute.Name)
		value := ""
		if attribute.Field.Status == "valid" {
			if text, ok := attribute.Field.NormalizedValue.(string); ok {
				value = normalizedText(text)
			}
		}
		if value == "" {
			if text, ok := attribute.Value.(string); ok {
				value = normalizedText(text)
			}
		}
		variantAttribute := strings.Contains(name, "assembly") || strings.Contains(name, "configuration") || strings.Contains(name, "composition") || strings.Contains(name, "variant")
		truthy := attribute.Value == true || value == "true" || value == "yes"
		if variantAttribute && (truthy || strings.Contains(value, "assembled") || strings.Contains(value, "configurable") || strings.Contains(value, "variable") || strings.Contains(value, "kit") || strings.Contains(value, "bundle")) {
			return true
		}
		if strings.Contains(value, "composition variable") || strings.Contains(value, "configurable") || strings.Contains(value, "assembled product") {
			return true
		}
	}
	return false
}

// matchReady claims one resolved candidate and atomically records either a unique
// exact association or a review suggestion. It never reads a newer offer candidate.
func matchReady(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var jobID, organizationID, offerID, requestID, candidateID string
	var raw json.RawMessage
	err = tx.QueryRowContext(ctx, `SELECT id::text,organization_id::text,offer_id::text,crawl_request_id::text,candidate_id::text,resolved_candidate
		FROM pending_match_job WHERE status='ready' ORDER BY created_at,id FOR UPDATE SKIP LOCKED LIMIT 1`).
		Scan(&jobID, &organizationID, &offerID, &requestID, &candidateID, &raw)
	if err != nil {
		return err
	}
	var candidate resolvedCandidate
	if err = json.Unmarshal(raw, &candidate); err != nil || candidate.OfferID != offerID || candidate.CrawlRequestID != requestID || candidate.CandidateID != candidateID {
		return failReady(ctx, tx, jobID, "candidate_identity_mismatch")
	}
	if _, err = uuid.Parse(candidate.CrawlRequestID); err != nil {
		return failReady(ctx, tx, jobID, "invalid_candidate_identity")
	}
	products, err := loadProducts(ctx, tx, organizationID)
	if err != nil {
		return err
	}
	gtin := normalizedGTIN(validStringField(candidate.Fields, "gtin"))
	brand := normalizedText(validStringField(candidate.Fields, "brand"))
	model := normalizedText(validStringField(candidate.Fields, "manufacturer_model"))
	var exact *canonicalProduct
	gtinAmbiguous := false
	if !hasVariableVariant(candidate.Attributes) {
		if gtin != "" {
			exact, gtinAmbiguous = uniqueProduct(products, func(p canonicalProduct) bool { return normalizedGTIN(p.GTIN) == gtin })
		}
		if exact == nil && !gtinAmbiguous && brand != "" && model != "" {
			exact, _ = uniqueProduct(products, func(p canonicalProduct) bool {
				return normalizedText(p.Brand) == brand && normalizedText(p.Model) == model
			})
		}
	}
	if exact != nil {
		var lockResult any
		if err = tx.QueryRowContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1 || ':' || $2,0))`, organizationID, offerID).Scan(&lockResult); err != nil {
			return err
		}
		var existingProduct string
		err = tx.QueryRowContext(ctx, `SELECT product_id::text FROM offer_match WHERE organization_id=$1 AND offer_id=$2 FOR UPDATE`, organizationID, offerID).Scan(&existingProduct)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if errors.Is(err, sql.ErrNoRows) {
			method := "brand_model"
			if gtin != "" && normalizedGTIN(exact.GTIN) == gtin {
				method = "gtin"
			}
			_, err = tx.ExecContext(ctx, `INSERT INTO offer_match(organization_id,offer_id,product_id,method)
				VALUES($1,$2,$3,$4) ON CONFLICT (organization_id,offer_id) DO NOTHING`, organizationID, offerID, exact.ID, method)
			if err != nil {
				return err
			}
			err = setJobStatus(ctx, tx, jobID, "matched")
			if err != nil {
				return err
			}
			return tx.Commit()
		}
		if existingProduct == exact.ID {
			if err = setJobStatus(ctx, tx, jobID, "matched"); err != nil {
				return err
			}
			return tx.Commit()
		}
	}
	proposed := titleSuggestion(candidate, products)
	var proposedID any
	if proposed != nil {
		proposedID = proposed.ID
	}
	suggestionID, err := id()
	if err != nil {
		return err
	}
	title := validStringField(candidate.Fields, "title")
	_, err = tx.ExecContext(ctx, `INSERT INTO match_suggestion(id,organization_id,offer_id,candidate_id,proposed_product_id,title)
		VALUES($1,$2,$3,$4,$5,NULLIF(left($6,200),'')) ON CONFLICT DO NOTHING`, suggestionID, organizationID, offerID, candidateID, proposedID, title)
	if err != nil {
		return err
	}
	if err = setJobStatus(ctx, tx, jobID, "review"); err != nil {
		return err
	}
	return tx.Commit()
}

// ProcessReady applies one durable resolved candidate job. It is also the
// deterministic service boundary used by integration scenarios.
func ProcessReady(ctx context.Context, db *sql.DB) error { return matchReady(ctx, db) }

type queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func loadProducts(ctx context.Context, q queryer, organizationID string) ([]canonicalProduct, error) {
	rows, err := q.QueryContext(ctx, `SELECT id::text,name,COALESCE(gtin,''),COALESCE(brand,''),COALESCE(manufacturer_model,'')
		FROM catalog_product WHERE organization_id=$1 ORDER BY id`, organizationID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var products []canonicalProduct
	for rows.Next() {
		var p canonicalProduct
		if err := rows.Scan(&p.ID, &p.Name, &p.GTIN, &p.Brand, &p.Model); err != nil {
			return nil, err
		}
		products = append(products, p)
	}
	return products, rows.Err()
}

func uniqueProduct(products []canonicalProduct, matches func(canonicalProduct) bool) (*canonicalProduct, bool) {
	var result *canonicalProduct
	count := 0
	for i := range products {
		if !matches(products[i]) {
			continue
		}
		count++
		if count == 1 {
			result = &products[i]
		}
	}
	return result, count > 1
}

func titleSuggestion(candidate resolvedCandidate, products []canonicalProduct) *canonicalProduct {
	title := normalizedText(validStringField(candidate.Fields, "title"))
	if title == "" {
		return nil
	}
	queryTerms := terms(title)
	var best *canonicalProduct
	bestScore := 0.0
	tie := false
	for i := range products {
		score := jaccard(queryTerms, terms(normalizedText(products[i].Name)))
		if score < 0.5 {
			continue
		}
		if score > bestScore {
			best, bestScore, tie = &products[i], score, false
		} else if score == bestScore {
			tie = true
		}
	}
	if tie {
		return nil
	}
	return best
}

func terms(value string) map[string]struct{} {
	set := make(map[string]struct{})
	for _, term := range strings.Fields(value) {
		if len(term) > 1 {
			set[term] = struct{}{}
		}
	}
	return set
}

func jaccard(a, b map[string]struct{}) float64 {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}
	intersection := 0
	for term := range a {
		if _, ok := b[term]; ok {
			intersection++
		}
	}
	return float64(intersection) / float64(len(a)+len(b)-intersection)
}

func setJobStatus(ctx context.Context, tx *sql.Tx, jobID, status string) error {
	_, err := tx.ExecContext(ctx, `UPDATE pending_match_job SET status=$2,resolved_candidate=NULL,next_attempt_at=NULL,last_error=NULL,updated_at=now() WHERE id=$1 AND status='ready'`, jobID, status)
	return err
}

func failReady(ctx context.Context, tx *sql.Tx, jobID, code string) error {
	if _, err := tx.ExecContext(ctx, `UPDATE pending_match_job SET status='failed',resolved_candidate=NULL,next_attempt_at=NULL,last_error=$2,updated_at=now() WHERE id=$1 AND status='ready'`, jobID, code); err != nil {
		return err
	}
	return tx.Commit()
}
