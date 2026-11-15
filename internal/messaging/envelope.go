package messaging

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"io"
	"regexp"
	"slices"
	"time"
)

const MaxEnvelopeBytes = 1 << 20

var (
	messageTypePattern = regexp.MustCompile(`^[a-z][a-z0-9]*(\.[a-z][a-z0-9]*)+\.v[1-9][0-9]*$`)
)

// These message types carry their version in the routing key rather than the envelope.
var unversionedMessageTypes = []string{
	"collection.result",
	"candidate.read.requested",
	"candidate.read.result",
	"catalog.review.list.requested",
	"catalog.review.list.result",
	"catalog.review.decide.requested",
	"catalog.review.decide.result",
	"catalog.offer.association.changed",
	"audit.event.recorded",
	"ranking.facts.read.requested",
	"ranking.facts.read.result",
	"source.policy.changed",
	"catalog.products.search",
	"comparison.group.create",
	"comparison.group.products.add",
	"comparison.group.product.remove",
	"comparison.group.validate",
	"comparison.groups.search",
	"comparison.group.read",
	"catalog.group.result",
	"comparison.group.changed",
	"comparison.group.detail.requested",
	"comparison.group.detail.result",
}

// Envelope is the tenant-scoped, versioned transport boundary shared by workers.
type Envelope struct {
	MessageID      string          `json:"message_id"`
	MessageType    string          `json:"message_type"`
	SchemaVersion  int             `json:"schema_version"`
	OccurredAt     time.Time       `json:"occurred_at"`
	Organization   string          `json:"organization_id"`
	CorrelationID  string          `json:"correlation_id"`
	CausationID    string          `json:"causation_id,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	Payload        json.RawMessage `json:"payload"`
}

// Validate checks required envelope fields, the known major version, and bounded JSON payload shape.
func (e Envelope) Validate() error {
	if !validUUID(e.MessageID) {
		return errors.New("message_id must be a UUID")
	}
	if !messageTypePattern.MatchString(e.MessageType) && !slices.Contains(unversionedMessageTypes, e.MessageType) {
		return errors.New("message_type must be a supported dotted message name")
	}
	if e.SchemaVersion != 1 {
		return fmt.Errorf("unsupported envelope schema_version %d", e.SchemaVersion)
	}
	if e.OccurredAt.IsZero() {
		return errors.New("occurred_at is required")
	}
	if !validUUID(e.Organization) {
		return errors.New("organization_id must be a UUID")
	}
	if !validUUID(e.CorrelationID) {
		return errors.New("correlation_id must be a UUID")
	}
	if e.CausationID != "" && !validUUID(e.CausationID) {
		return errors.New("causation_id must be a UUID when present")
	}
	if len(e.IdempotencyKey) > 200 {
		return errors.New("idempotency_key must not exceed 200 characters")
	}
	if len(e.Payload) == 0 || len(e.Payload) > MaxEnvelopeBytes {
		return errors.New("payload is required and must be bounded")
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(e.Payload, &object); err != nil || object == nil {
		return errors.New("payload must be a JSON object")
	}
	return nil
}

// Decode validates a bounded JSON envelope and rejects unknown fields or trailing data.
func Decode(data []byte) (Envelope, error) {
	if len(data) == 0 || len(data) > MaxEnvelopeBytes {
		return Envelope{}, errors.New("message envelope is empty or exceeds size limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var envelope Envelope
	if err := decoder.Decode(&envelope); err != nil {
		return Envelope{}, fmt.Errorf("decode message envelope: %w", err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return Envelope{}, errors.New("message envelope contains trailing JSON data")
	}
	if err := envelope.Validate(); err != nil {
		return Envelope{}, err
	}
	return envelope, nil
}

func (e Envelope) Encode() ([]byte, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}
	data, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("encode message envelope: %w", err)
	}
	if len(data) > MaxEnvelopeBytes {
		return nil, errors.New("encoded envelope exceeds size limit")
	}
	return data, nil
}

var uuidValidator = validator.New()

func validUUID(value string) bool {
	if uuidValidator.Var(value, "uuid") != nil {
		return false
	}
	parsed, err := uuid.Parse(value)
	return err == nil && parsed.Version() >= 1 && parsed.Version() <= 8 && parsed.Variant() == uuid.RFC4122
}
