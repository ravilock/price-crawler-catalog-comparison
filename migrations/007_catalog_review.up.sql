ALTER TABLE match_suggestion
    ADD COLUMN title text CHECK (title IS NULL OR length(title) BETWEEN 1 AND 200);

ALTER TABLE pending_match_job
    DROP CONSTRAINT pending_match_job_status_check,
    ADD CONSTRAINT pending_match_job_status_check
        CHECK (status IN ('pending', 'ready', 'matched', 'review', 'rejected', 'failed')),
    DROP CONSTRAINT pending_match_job_decision_state,
    ADD CONSTRAINT pending_match_job_decision_state
        CHECK (
            (decision_id IS NULL OR status IN ('matched', 'rejected'))
            AND (status <> 'rejected' OR decision_id IS NOT NULL)
        );

CREATE TABLE catalog_review_snapshot (
    organization_id uuid NOT NULL,
    token_hash bytea NOT NULL,
    actor_subject text NOT NULL CHECK (length(trim(actor_subject)) BETWEEN 1 AND 200),
    page_limit integer NOT NULL CHECK (page_limit BETWEEN 1 AND 100),
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (organization_id, token_hash),
    CHECK (expires_at > created_at)
);

CREATE TABLE catalog_review_snapshot_item (
    organization_id uuid NOT NULL,
    snapshot_token_hash bytea NOT NULL,
    ordinal integer NOT NULL CHECK (ordinal >= 0),
    item jsonb NOT NULL,
    PRIMARY KEY (organization_id, snapshot_token_hash, ordinal),
    FOREIGN KEY (organization_id, snapshot_token_hash)
        REFERENCES catalog_review_snapshot (organization_id, token_hash)
);

CREATE TABLE catalog_review_cursor (
    token_hash bytea PRIMARY KEY,
    organization_id uuid NOT NULL,
    snapshot_token_hash bytea NOT NULL,
    actor_subject text NOT NULL CHECK (length(trim(actor_subject)) BETWEEN 1 AND 200),
    offset_items integer NOT NULL CHECK (offset_items >= 0),
    page_limit integer NOT NULL CHECK (page_limit BETWEEN 1 AND 100),
    expires_at timestamptz NOT NULL,
    FOREIGN KEY (organization_id, snapshot_token_hash)
        REFERENCES catalog_review_snapshot (organization_id, token_hash)
);

CREATE TABLE catalog_review_idempotency (
    organization_id uuid NOT NULL,
    actor_subject text NOT NULL CHECK (length(trim(actor_subject)) BETWEEN 1 AND 200),
    operation text NOT NULL CHECK (operation = 'review.decide'),
    idempotency_key text NOT NULL CHECK (length(idempotency_key) BETWEEN 1 AND 200),
    request_hash text NOT NULL CHECK (length(request_hash) = 64),
    result jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (organization_id, actor_subject, operation, idempotency_key)
);
