CREATE TABLE IF NOT EXISTS message_outbox (
  message_id uuid PRIMARY KEY, destination text NOT NULL, message_type text NOT NULL,
  organization_id uuid NOT NULL, correlation_id uuid NOT NULL, payload jsonb NOT NULL,
  attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0), available_at timestamptz NOT NULL DEFAULT now(),
  claimed_until timestamptz, dead_lettered_at timestamptz, last_error text,
  published_at timestamptz, created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS message_outbox_pending_idx ON message_outbox(available_at, created_at) WHERE published_at IS NULL;
CREATE TABLE IF NOT EXISTS message_inbox (
  message_id uuid NOT NULL, consumer_name text NOT NULL, received_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(message_id, consumer_name)
);
