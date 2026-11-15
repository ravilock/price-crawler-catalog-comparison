ALTER TABLE review_decision
    ADD CONSTRAINT review_decision_tenant_offer_id_unique
    UNIQUE (organization_id, id, offer_id);

CREATE TABLE pending_match_job (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    offer_id uuid NOT NULL,
    crawl_request_id uuid NOT NULL,
    candidate_id uuid NOT NULL,
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'ready', 'matched', 'review', 'failed')),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0 AND attempts <= 5),
    next_attempt_at timestamptz,
    last_error text,
    decision_id uuid,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT pending_match_job_identity_unique
        UNIQUE (organization_id, offer_id, crawl_request_id, candidate_id),
    CONSTRAINT pending_match_job_tenant_id_unique
        UNIQUE (organization_id, id),
    CONSTRAINT pending_match_job_decision_state
        CHECK (
            (status IN ('matched', 'review') AND decision_id IS NOT NULL)
            OR (status NOT IN ('matched', 'review') AND decision_id IS NULL)
        ),
    CONSTRAINT pending_match_job_decision_fk
        FOREIGN KEY (organization_id, decision_id, offer_id)
        REFERENCES review_decision (organization_id, id, offer_id)
);

CREATE INDEX pending_match_job_due_idx
    ON pending_match_job (next_attempt_at, created_at)
    WHERE status = 'pending';
