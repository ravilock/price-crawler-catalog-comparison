package messaging

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"time"
)

const MaxEnvelopeBytes = 1 << 20

var (
	uuidPattern        = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	messageTypePattern = regexp.MustCompile(`^[a-z][a-z0-9]*(\.[a-z][a-z0-9]*)+\.v[1-9][0-9]*$`)
)

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
	if !uuidPattern.MatchString(e.MessageID) {
		return errors.New("message_id must be a UUID")
	}
	if !messageTypePattern.MatchString(e.MessageType) {
		return errors.New("message_type must be a dotted, versioned name")
	}
	if e.SchemaVersion != 1 {
		return fmt.Errorf("unsupported envelope schema_version %d", e.SchemaVersion)
	}
	if e.OccurredAt.IsZero() {
		return errors.New("occurred_at is required")
	}
	if !uuidPattern.MatchString(e.Organization) {
		return errors.New("organization_id must be a UUID")
	}
	if !uuidPattern.MatchString(e.CorrelationID) {
		return errors.New("correlation_id must be a UUID")
	}
	if e.CausationID != "" && !uuidPattern.MatchString(e.CausationID) {
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
