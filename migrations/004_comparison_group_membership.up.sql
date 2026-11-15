ALTER TABLE comparison_group
    ADD COLUMN name text NOT NULL DEFAULT 'Untitled'
        CHECK (length(trim(name)) BETWEEN 1 AND 200),
    ADD COLUMN state text NOT NULL DEFAULT 'draft'
        CHECK (state IN ('draft', 'validated')),
    ADD COLUMN revision bigint NOT NULL DEFAULT 1
        CHECK (revision > 0);

CREATE TABLE comparison_group_product (
    organization_id uuid NOT NULL,
    group_id uuid NOT NULL,
    product_id uuid NOT NULL,
    added_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (organization_id, group_id, product_id),
    FOREIGN KEY (organization_id, group_id)
        REFERENCES comparison_group (organization_id, id),
    FOREIGN KEY (organization_id, product_id)
        REFERENCES catalog_product (organization_id, id)
);
CREATE INDEX comparison_group_product_product_idx
    ON comparison_group_product (organization_id, product_id, group_id);

CREATE TABLE comparison_group_mutation (
    organization_id uuid NOT NULL,
    actor_subject text NOT NULL CHECK (length(trim(actor_subject)) BETWEEN 1 AND 200),
    operation text NOT NULL CHECK (length(trim(operation)) BETWEEN 1 AND 100),
    idempotency_key text NOT NULL CHECK (length(idempotency_key) BETWEEN 1 AND 200),
    request_hash text NOT NULL CHECK (length(request_hash) = 64),
    result jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (organization_id, actor_subject, operation, idempotency_key)
);

CREATE TABLE comparison_membership_journal_counter (
    organization_id uuid PRIMARY KEY,
    current_sequence bigint NOT NULL DEFAULT 0 CHECK (current_sequence >= 0)
);

CREATE TABLE comparison_membership_journal (
    organization_id uuid NOT NULL,
    sequence bigint NOT NULL CHECK (sequence > 0),
    event_type text NOT NULL CHECK (event_type IN ('group', 'offer_association')),
    entity_id uuid NOT NULL,
    revision bigint NOT NULL CHECK (revision > 0),
    payload jsonb NOT NULL,
    occurred_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (organization_id, sequence)
);

CREATE TABLE comparison_membership_snapshot (
    organization_id uuid NOT NULL,
    token_hash bytea NOT NULL,
    watermark bigint NOT NULL CHECK (watermark >= 0),
    rows jsonb NOT NULL CHECK (jsonb_typeof(rows) = 'array'),
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (organization_id, token_hash),
    CHECK (expires_at > created_at)
);
CREATE INDEX comparison_membership_snapshot_expiry_idx
    ON comparison_membership_snapshot (expires_at);
