-- Crawler-owned offers and candidates are external IDs; no cross-service FK.
CREATE TABLE catalog_product (
 id uuid PRIMARY KEY,
 organization_id uuid NOT NULL,
 name text NOT NULL CHECK (length(trim(name)) BETWEEN 1 AND 200),
 gtin text,
 brand text,
 manufacturer_model text,
 created_at timestamptz NOT NULL DEFAULT now(),
 updated_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE (organization_id, id)
);

CREATE TABLE match_suggestion (
 id uuid PRIMARY KEY,
 organization_id uuid NOT NULL,
 offer_id uuid NOT NULL,
 candidate_id uuid NOT NULL,
 proposed_product_id uuid,
 created_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE (organization_id, id, offer_id),
 FOREIGN KEY (organization_id, proposed_product_id) REFERENCES catalog_product(organization_id, id)
);
CREATE INDEX match_suggestion_offer_idx ON match_suggestion(organization_id, offer_id);

-- Decisions are immutable and bind the offer, product and reviewer together.
CREATE TABLE review_decision (
 id uuid PRIMARY KEY,
 organization_id uuid NOT NULL,
 suggestion_id uuid NOT NULL,
 offer_id uuid NOT NULL,
 decision text NOT NULL CHECK (decision IN ('approve', 'reject')),
 product_id uuid,
 reviewer_subject text NOT NULL CHECK (length(trim(reviewer_subject)) BETWEEN 1 AND 200),
 decided_at timestamptz NOT NULL DEFAULT now(),
 CHECK ((decision = 'approve' AND product_id IS NOT NULL) OR (decision = 'reject' AND product_id IS NULL)),
 UNIQUE (organization_id, suggestion_id),
 UNIQUE (organization_id, id, offer_id, product_id),
 FOREIGN KEY (organization_id, suggestion_id, offer_id) REFERENCES match_suggestion(organization_id, id, offer_id),
 FOREIGN KEY (organization_id, product_id) REFERENCES catalog_product(organization_id, id)
);
CREATE FUNCTION reject_review_decision_change() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 RAISE EXCEPTION 'review decisions are append-only';
END $$;
CREATE TRIGGER review_decision_append_only BEFORE UPDATE OR DELETE ON review_decision
 FOR EACH ROW EXECUTE FUNCTION reject_review_decision_change();

CREATE TABLE offer_match (
 organization_id uuid NOT NULL,
 offer_id uuid NOT NULL,
 product_id uuid NOT NULL,
 method text NOT NULL CHECK (method IN ('gtin', 'brand_model', 'review')),
 review_decision_id uuid,
 revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
 created_at timestamptz NOT NULL DEFAULT now(),
 updated_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY (organization_id, offer_id),
 CHECK ((method = 'review' AND review_decision_id IS NOT NULL) OR (method <> 'review' AND review_decision_id IS NULL)),
 FOREIGN KEY (organization_id, product_id) REFERENCES catalog_product(organization_id, id),
 FOREIGN KEY (organization_id, review_decision_id, offer_id, product_id) REFERENCES review_decision(organization_id, id, offer_id, product_id)
);
CREATE INDEX offer_match_product_idx ON offer_match(organization_id, product_id);

-- The owning service writes audit and outbox entries in the same transaction
-- as the business change; the write handlers are delivered by later tasks.
CREATE TABLE catalog_audit_record (
 id uuid PRIMARY KEY,
 organization_id uuid NOT NULL,
 actor_subject text NOT NULL CHECK (length(trim(actor_subject)) BETWEEN 1 AND 200),
 actor_role text NOT NULL CHECK (actor_role IN ('administrator', 'manager', 'end_user', 'auditor', 'system')),
 action text NOT NULL CHECK (length(trim(action)) BETWEEN 1 AND 200),
 entity_type text NOT NULL CHECK (length(trim(entity_type)) BETWEEN 1 AND 200),
 entity_id uuid NOT NULL,
 before_value jsonb,
 after_value jsonb NOT NULL,
 occurred_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE (organization_id, id)
);
CREATE FUNCTION reject_catalog_audit_change() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 RAISE EXCEPTION 'catalog audit records are append-only';
END $$;
CREATE TRIGGER catalog_audit_append_only BEFORE UPDATE OR DELETE ON catalog_audit_record
 FOR EACH ROW EXECUTE FUNCTION reject_catalog_audit_change();
CREATE INDEX catalog_audit_tenant_time_idx ON catalog_audit_record(organization_id, occurred_at, id);

-- Group revision, membership, idempotency and journal are introduced by T126.
CREATE TABLE comparison_group (
 id uuid PRIMARY KEY,
 organization_id uuid NOT NULL,
 created_by text NOT NULL CHECK (length(trim(created_by)) BETWEEN 1 AND 200),
 created_at timestamptz NOT NULL DEFAULT now(),
 updated_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE (organization_id, id)
);
