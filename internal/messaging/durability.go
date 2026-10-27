package messaging

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"
)

const MaxDeliveryAttempts = 5
const MaxRetryDelay = 5 * time.Minute

type OutboxMessage struct {
	MessageID, Destination, MessageType, OrganizationID, CorrelationID string
	Payload                                                            []byte
	Attempts                                                           int
}

func ClaimOutbox(ctx context.Context, tx *sql.Tx, limit int) (messages []OutboxMessage, resultErr error) {
	if tx == nil || limit < 1 || limit > 100 {
		return nil, errors.New("transaction and batch size from 1 to 100 are required")
	}
	rows, err := tx.QueryContext(ctx, `WITH eligible AS (SELECT message_id FROM message_outbox WHERE published_at IS NULL AND dead_lettered_at IS NULL AND available_at<=now() AND (claimed_until IS NULL OR claimed_until<now()) ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT $1) UPDATE message_outbox o SET claimed_until=now()+interval '1 minute' FROM eligible e WHERE o.message_id=e.message_id RETURNING o.message_id::text,o.destination,o.message_type,o.organization_id::text,o.correlation_id::text,o.payload,o.attempts`, limit)
	if err != nil {
		return nil, fmt.Errorf("claim outbox batch: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); resultErr == nil && closeErr != nil {
			resultErr = fmt.Errorf("close claimed outbox rows: %w", closeErr)
		}
	}()
	messages = make([]OutboxMessage, 0, limit)
	for rows.Next() {
		var m OutboxMessage
		if err = rows.Scan(&m.MessageID, &m.Destination, &m.MessageType, &m.OrganizationID, &m.CorrelationID, &m.Payload, &m.Attempts); err != nil {
			return nil, err
		}
		messages = append(messages, m)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return messages, nil
}

func MarkOutboxPublished(ctx context.Context, tx *sql.Tx, messageID string) error {
	result, err := tx.ExecContext(ctx, `UPDATE message_outbox SET published_at=now(),claimed_until=NULL WHERE message_id=$1 AND published_at IS NULL`, messageID)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n != 1 {
		return errors.New("outbox message was not pending")
	}
	return nil
}

func ScheduleOutboxRetry(ctx context.Context, tx *sql.Tx, messageID string, attempts int, cause string, now time.Time) error {
	if len(cause) > 1000 {
		cause = cause[:1000]
	}
	if attempts >= MaxDeliveryAttempts {
		_, err := tx.ExecContext(ctx, `UPDATE message_outbox SET attempts=$2,last_error=$3,dead_lettered_at=$4,claimed_until=NULL WHERE message_id=$1 AND published_at IS NULL`, messageID, attempts, cause, now)
		return err
	}
	_, err := tx.ExecContext(ctx, `UPDATE message_outbox SET attempts=$2,last_error=$3,available_at=$4,claimed_until=NULL WHERE message_id=$1 AND published_at IS NULL`, messageID, attempts, cause, now.Add(RetryDelay(attempts)))
	return err
}

// EnqueueOutbox must be called with the business write's transaction.
func EnqueueOutbox(ctx context.Context, tx *sql.Tx, e Envelope, destination string) error {
	if tx == nil || destination == "" {
		return errors.New("transaction and destination are required")
	}
	body, err := e.Encode()
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO message_outbox(message_id, destination, message_type, organization_id, correlation_id, payload) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (message_id) DO NOTHING`, e.MessageID, destination, e.MessageType, e.Organization, e.CorrelationID, body)
	if err != nil {
		return fmt.Errorf("insert transactional outbox row: %w", err)
	}
	return nil
}

// BeginInbox records a delivery inside the caller's transaction. false means it was already applied.
func BeginInbox(ctx context.Context, tx *sql.Tx, messageID, consumer string) (bool, error) {
	if tx == nil || messageID == "" || consumer == "" {
		return false, errors.New("transaction, message ID, and consumer are required")
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO message_inbox(message_id, consumer_name) VALUES ($1,$2) ON CONFLICT DO NOTHING`, messageID, consumer)
	if err != nil {
		return false, fmt.Errorf("record inbox delivery: %w", err)
	}
	n, err := result.RowsAffected()
	return n == 1, err
}

func RetryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > MaxDeliveryAttempts {
		attempt = MaxDeliveryAttempts
	}
	base := time.Second * time.Duration(1<<(attempt-1))
	if base > MaxRetryDelay {
		base = MaxRetryDelay
	}
	return base/2 + time.Duration(rand.Int64N(int64(base/2)+1))
}
