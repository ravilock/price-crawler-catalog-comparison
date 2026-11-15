// Package comparison owns the catalog ranking projection and its recovery.
package comparison

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// ErrInconsistent reports a reply that contradicts the stored component at an
// equal revision. The projection stays not rankable until a read succeeds.
var ErrInconsistent = errors.New("ranking facts are inconsistent")

// Collection is the latest terminal collection outcome.
type Collection struct {
	CrawlRequestID string    `json:"crawl_request_id"`
	Outcome        string    `json:"outcome"`
	CompletedAt    time.Time `json:"completed_at"`
}

// Observation is the newest stored observation including its position.
type Observation struct {
	ObservationID  string    `json:"observation_id"`
	Position       int64     `json:"position"`
	CrawlRequestID string    `json:"crawl_request_id"`
	Availability   string    `json:"availability"`
	ObservedAt     time.Time `json:"observed_at"`
}

// Price is the newest verified BRL cash observation.
type Price struct {
	ObservationID  string    `json:"observation_id"`
	Position       int64     `json:"position"`
	CrawlRequestID string    `json:"crawl_request_id"`
	PriceCents     int64     `json:"price_cents"`
	Currency       string    `json:"currency"`
	ObservedAt     time.Time `json:"observed_at"`
}

// Facts is one authorized authoritative crawler read.
type Facts struct {
	OfferID                string
	SourceID               string
	OfferRevision          int64
	SourceRevision         int64
	FreshnessWindowSeconds int
	Collection             *Collection
	Observation            *Observation
	Price                  *Price
}

// Projection is the replaceable catalog read model. Historical display facts
// are retained when the crawler becomes unavailable; Rankable gates ranking.
type Projection struct {
	OrganizationID         string
	OfferID                string
	SourceID               string
	OfferRevision          int64
	SourceRevision         int64
	FreshnessWindowSeconds int
	Collection             *Collection
	Observation            *Observation
	Price                  *Price
	Rankable               bool
	ValidatedAt            time.Time
}

// RankableAt requires a recent successful authoritative read. T039 uses this
// before serving a comparison so a lost notification cannot present an old
// freshness window as current.
func (p Projection) RankableAt(now time.Time, maxAge time.Duration) bool {
	return p.Rankable && !p.ValidatedAt.IsZero() && maxAge > 0 && now.Sub(p.ValidatedAt) <= maxAge
}

func sameTime(a, b time.Time) bool { return a.UTC().Equal(b.UTC()) }

func validTime(value time.Time) bool { return !value.IsZero() }

func validateFacts(f Facts) error {
	if !validUUID(f.OfferID) || !validUUID(f.SourceID) || f.OfferRevision < 1 || f.SourceRevision < 1 || f.FreshnessWindowSeconds < 1 {
		return errors.New("invalid ranking identity or revision")
	}
	if f.Collection != nil {
		c := f.Collection
		if !validUUID(c.CrawlRequestID) || !validTime(c.CompletedAt) {
			return errors.New("invalid latest collection")
		}
		switch c.Outcome {
		case "complete", "partial", "failed", "paused":
		default:
			return errors.New("invalid latest collection outcome")
		}
	}
	if o := f.Observation; o != nil {
		if !validUUID(o.ObservationID) || !validUUID(o.CrawlRequestID) || o.Position < 1 || !validTime(o.ObservedAt) {
			return errors.New("invalid latest observation")
		}
		switch o.Availability {
		case "available", "unavailable", "unknown":
		default:
			return errors.New("invalid latest observation availability")
		}
	}
	if p := f.Price; p != nil {
		if !validUUID(p.ObservationID) || !validUUID(p.CrawlRequestID) || p.Position < 1 || p.PriceCents < 0 || p.Currency != "BRL" || !validTime(p.ObservedAt) {
			return errors.New("invalid verified cash price")
		}
		if f.Observation == nil || p.Position > f.Observation.Position {
			return errors.New("verified price is newer than the latest observation")
		}
		if p.Position == f.Observation.Position && (p.ObservationID != f.Observation.ObservationID || p.CrawlRequestID != f.Observation.CrawlRequestID || !sameTime(p.ObservedAt, f.Observation.ObservedAt)) {
			return errors.New("verified price identity contradicts the latest observation")
		}
	}
	return nil
}

func sameOfferComponent(p Projection, f Facts) bool {
	if (p.Collection == nil) != (f.Collection == nil) || (p.Observation == nil) != (f.Observation == nil) || (p.Price == nil) != (f.Price == nil) {
		return false
	}
	if p.Collection != nil {
		if p.Collection.CrawlRequestID != f.Collection.CrawlRequestID || p.Collection.Outcome != f.Collection.Outcome || !sameTime(p.Collection.CompletedAt, f.Collection.CompletedAt) {
			return false
		}
	}
	if p.Observation != nil {
		o := f.Observation
		if p.Observation.ObservationID != o.ObservationID || p.Observation.Position != o.Position || p.Observation.CrawlRequestID != o.CrawlRequestID || p.Observation.Availability != o.Availability || !sameTime(p.Observation.ObservedAt, o.ObservedAt) {
			return false
		}
	}
	if p.Price != nil {
		q := f.Price
		if p.Price.ObservationID != q.ObservationID || p.Price.Position != q.Position || p.Price.CrawlRequestID != q.CrawlRequestID || p.Price.PriceCents != q.PriceCents || p.Price.Currency != q.Currency || !sameTime(p.Price.ObservedAt, q.ObservedAt) {
			return false
		}
	}
	return true
}

// Merge applies independent components by their own revision. Older components
// are ignored, equal revisions must match exactly, and a differing equal
// revision is an inconsistency rather than an overwrite.
func Merge(current *Projection, incoming Facts) (Projection, error) {
	if err := validateFacts(incoming); err != nil {
		return Projection{}, err
	}
	if current == nil {
		return Projection{
			OfferID:                incoming.OfferID,
			SourceID:               incoming.SourceID,
			OfferRevision:          incoming.OfferRevision,
			SourceRevision:         incoming.SourceRevision,
			FreshnessWindowSeconds: incoming.FreshnessWindowSeconds,
			Collection:             incoming.Collection,
			Observation:            incoming.Observation,
			Price:                  incoming.Price,
			Rankable:               true,
		}, nil
	}
	merged := *current
	if incoming.SourceID != current.SourceID {
		return Projection{}, fmt.Errorf("%w: source identity changed", ErrInconsistent)
	}
	switch {
	case incoming.OfferRevision > current.OfferRevision:
		merged.OfferRevision = incoming.OfferRevision
		merged.Collection = incoming.Collection
		merged.Observation = incoming.Observation
		merged.Price = incoming.Price
	case incoming.OfferRevision == current.OfferRevision:
		if !sameOfferComponent(merged, incoming) {
			return Projection{}, fmt.Errorf("%w: equal offer revision has different facts", ErrInconsistent)
		}
	}
	switch {
	case incoming.SourceRevision > current.SourceRevision:
		merged.SourceRevision = incoming.SourceRevision
		merged.FreshnessWindowSeconds = incoming.FreshnessWindowSeconds
	case incoming.SourceRevision == current.SourceRevision:
		if incoming.FreshnessWindowSeconds != current.FreshnessWindowSeconds {
			return Projection{}, fmt.Errorf("%w: equal source revision has a different window", ErrInconsistent)
		}
	}
	merged.Rankable = true
	return merged, nil
}

type scanner interface {
	Scan(dest ...any) error
}

func scanProjection(row scanner, p *Projection) error {
	var collectionID, collectionOutcome, observationID, observationRequest, availability, priceID, priceRequest sql.NullString
	var collectionAt, observationAt, priceAt sql.NullTime
	var observationPosition, pricePosition, priceCents sql.NullInt64
	if err := row.Scan(&p.OrganizationID, &p.OfferID, &p.SourceID, &p.OfferRevision, &p.SourceRevision, &p.FreshnessWindowSeconds,
		&collectionID, &collectionOutcome, &collectionAt,
		&observationID, &observationPosition, &observationRequest, &availability, &observationAt,
		&priceID, &pricePosition, &priceRequest, &priceCents, &priceAt,
		&p.Rankable, &p.ValidatedAt); err != nil {
		return err
	}
	if collectionID.Valid && collectionOutcome.Valid && collectionAt.Valid {
		p.Collection = &Collection{CrawlRequestID: collectionID.String, Outcome: collectionOutcome.String, CompletedAt: collectionAt.Time.UTC()}
	}
	if observationID.Valid && observationPosition.Valid && observationRequest.Valid && availability.Valid && observationAt.Valid {
		p.Observation = &Observation{ObservationID: observationID.String, Position: observationPosition.Int64, CrawlRequestID: observationRequest.String, Availability: availability.String, ObservedAt: observationAt.Time.UTC()}
	}
	if priceID.Valid && pricePosition.Valid && priceRequest.Valid && priceCents.Valid && priceAt.Valid {
		p.Price = &Price{ObservationID: priceID.String, Position: pricePosition.Int64, CrawlRequestID: priceRequest.String, PriceCents: priceCents.Int64, Currency: "BRL", ObservedAt: priceAt.Time.UTC()}
	}
	return nil
}

const projectionColumns = `organization_id::text,offer_id::text,source_id::text,offer_revision,source_revision,freshness_window_seconds,
 latest_crawl_request_id::text,latest_outcome,latest_completed_at,
 observation_id::text,observation_position,observation_crawl_request_id::text,observation_availability,observed_at,
 price_observation_id::text,price_position,price_crawl_request_id::text,price_cents,price_observed_at,
 rankable,validated_at`

// LoadProjection reads one tenant-scoped projection.
func LoadProjection(ctx context.Context, db *sql.DB, org, offerID string) (Projection, bool, error) {
	var p Projection
	err := scanProjection(db.QueryRowContext(ctx, `SELECT `+projectionColumns+` FROM offer_ranking_projection WHERE organization_id=$1 AND offer_id=$2`, org, offerID), &p)
	if errors.Is(err, sql.ErrNoRows) {
		return Projection{}, false, nil
	}
	if err != nil {
		return Projection{}, false, err
	}
	return p, true, nil
}

// Apply commits one authorized read, merging strictly newer components.
func Apply(ctx context.Context, db *sql.DB, org string, incoming Facts, now time.Time) error {
	if err := validateFacts(incoming); err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var current Projection
	loadErr := scanProjection(tx.QueryRowContext(ctx, `SELECT `+projectionColumns+` FROM offer_ranking_projection WHERE organization_id=$1 AND offer_id=$2 FOR UPDATE`, org, incoming.OfferID), &current)
	var merged Projection
	if errors.Is(loadErr, sql.ErrNoRows) {
		merged, err = Merge(nil, incoming)
	} else if loadErr != nil {
		return loadErr
	} else {
		merged, err = Merge(&current, incoming)
	}
	if err != nil {
		return err
	}
	merged.OrganizationID = org
	merged.ValidatedAt = now.UTC()
	if loadErr != nil {
		_, err = tx.ExecContext(ctx, `INSERT INTO offer_ranking_projection(organization_id,offer_id,source_id,offer_revision,source_revision,freshness_window_seconds,
		 latest_crawl_request_id,latest_outcome,latest_completed_at,
		 observation_id,observation_position,observation_crawl_request_id,observation_availability,observed_at,
		 price_observation_id,price_position,price_crawl_request_id,price_cents,price_currency,price_observed_at,
		 rankable,last_error,validated_at,updated_at)
		 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,NULL,$22,now())`,
			org, merged.OfferID, merged.SourceID, merged.OfferRevision, merged.SourceRevision, merged.FreshnessWindowSeconds,
			nullString(collectionID(merged.Collection)), nullString(collectionOutcome(merged.Collection)), nullTime(collectionAt(merged.Collection)),
			nullString(observationID(merged.Observation)), nullInt(observationPosition(merged.Observation)), nullString(observationRequest(merged.Observation)), nullString(observationAvailability(merged.Observation)), nullTime(observationAt(merged.Observation)),
			nullString(priceID(merged.Price)), nullInt(pricePosition(merged.Price)), nullString(priceRequest(merged.Price)), priceCentsValue(merged.Price), priceCurrency(merged.Price), nullTime(priceAt(merged.Price)),
			merged.Rankable, merged.ValidatedAt)
	} else {
		_, err = tx.ExecContext(ctx, `UPDATE offer_ranking_projection SET source_id=$3,offer_revision=$4,source_revision=$5,freshness_window_seconds=$6,
		 latest_crawl_request_id=$7,latest_outcome=$8,latest_completed_at=$9,
		 observation_id=$10,observation_position=$11,observation_crawl_request_id=$12,observation_availability=$13,observed_at=$14,
		 price_observation_id=$15,price_position=$16,price_crawl_request_id=$17,price_cents=$18,price_currency=$19,price_observed_at=$20,
		 rankable=$21,last_error=NULL,validated_at=$22,updated_at=now()
		 WHERE organization_id=$1 AND offer_id=$2`,
			org, merged.OfferID, merged.SourceID, merged.OfferRevision, merged.SourceRevision, merged.FreshnessWindowSeconds,
			nullString(collectionID(merged.Collection)), nullString(collectionOutcome(merged.Collection)), nullTime(collectionAt(merged.Collection)),
			nullString(observationID(merged.Observation)), nullInt(observationPosition(merged.Observation)), nullString(observationRequest(merged.Observation)), nullString(observationAvailability(merged.Observation)), nullTime(observationAt(merged.Observation)),
			nullString(priceID(merged.Price)), nullInt(pricePosition(merged.Price)), nullString(priceRequest(merged.Price)), priceCentsValue(merged.Price), priceCurrency(merged.Price), nullTime(priceAt(merged.Price)),
			merged.Rankable, merged.ValidatedAt)
	}
	if err != nil {
		return err
	}
	return tx.Commit()
}

func nullString(v string) any {
	if v == "" {
		return nil
	}
	return v
}
func nullInt(v int64) any {
	if v <= 0 {
		return nil
	}
	return v
}
func nullTime(v time.Time) any {
	if v.IsZero() {
		return nil
	}
	return v
}
func collectionID(c *Collection) string {
	if c == nil {
		return ""
	}
	return c.CrawlRequestID
}
func collectionOutcome(c *Collection) string {
	if c == nil {
		return ""
	}
	return c.Outcome
}
func collectionAt(c *Collection) time.Time {
	if c == nil {
		return time.Time{}
	}
	return c.CompletedAt
}
func observationID(o *Observation) string {
	if o == nil {
		return ""
	}
	return o.ObservationID
}
func observationPosition(o *Observation) int64 {
	if o == nil {
		return 0
	}
	return o.Position
}
func observationRequest(o *Observation) string {
	if o == nil {
		return ""
	}
	return o.CrawlRequestID
}
func observationAvailability(o *Observation) string {
	if o == nil {
		return ""
	}
	return o.Availability
}
func observationAt(o *Observation) time.Time {
	if o == nil {
		return time.Time{}
	}
	return o.ObservedAt
}
func priceID(p *Price) string {
	if p == nil {
		return ""
	}
	return p.ObservationID
}
func pricePosition(p *Price) int64 {
	if p == nil {
		return 0
	}
	return p.Position
}
func priceRequest(p *Price) string {
	if p == nil {
		return ""
	}
	return p.CrawlRequestID
}

// priceCentsValue keeps zero a valid price while a missing price stays NULL.
func priceCentsValue(p *Price) any {
	if p == nil {
		return nil
	}
	return p.PriceCents
}

// priceCurrency keeps a missing price fully NULL so the projection constraint
// cannot store a currency without an amount.
func priceCurrency(p *Price) any {
	if p == nil {
		return nil
	}
	return p.Currency
}
func priceAt(p *Price) time.Time {
	if p == nil {
		return time.Time{}
	}
	return p.ObservedAt
}
