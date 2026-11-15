-- T158: catalog-owned ranking projection. Crawler keeps authoritative offers
-- and observations; this is a replaceable read model merged by independent
-- offer and source revisions.
CREATE TABLE offer_ranking_projection (
    organization_id uuid NOT NULL,
    offer_id uuid NOT NULL,
    source_id uuid NOT NULL,
    offer_revision bigint NOT NULL CHECK (offer_revision > 0),
    source_revision bigint NOT NULL CHECK (source_revision > 0),
    freshness_window_seconds integer NOT NULL CHECK (freshness_window_seconds > 0),
    latest_crawl_request_id uuid,
    latest_outcome text CHECK (latest_outcome IN ('complete', 'partial', 'failed', 'paused')),
    latest_completed_at timestamptz,
    observation_id uuid,
    observation_position bigint CHECK (observation_position > 0),
    observation_crawl_request_id uuid,
    observation_availability text CHECK (observation_availability IN ('available', 'unavailable', 'unknown')),
    observed_at timestamptz,
    price_observation_id uuid,
    price_position bigint CHECK (price_position > 0),
    price_crawl_request_id uuid,
    price_cents bigint CHECK (price_cents >= 0),
    price_currency text CHECK (price_currency = 'BRL'),
    price_observed_at timestamptz,
    rankable boolean NOT NULL DEFAULT false,
    last_error text,
    validated_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (organization_id, offer_id),
    CONSTRAINT projection_collection_fields CHECK (
        (latest_crawl_request_id IS NULL AND latest_outcome IS NULL AND latest_completed_at IS NULL)
        OR (latest_crawl_request_id IS NOT NULL AND latest_outcome IS NOT NULL AND latest_completed_at IS NOT NULL)
    ),
    CONSTRAINT projection_observation_fields CHECK (
        (observation_id IS NULL AND observation_position IS NULL AND observation_crawl_request_id IS NULL
         AND observation_availability IS NULL AND observed_at IS NULL)
        OR (observation_id IS NOT NULL AND observation_position IS NOT NULL AND observation_crawl_request_id IS NOT NULL
            AND observation_availability IS NOT NULL AND observed_at IS NOT NULL)
    ),
    CONSTRAINT projection_price_fields CHECK (
        (price_observation_id IS NULL AND price_position IS NULL AND price_crawl_request_id IS NULL
         AND price_cents IS NULL AND price_currency IS NULL AND price_observed_at IS NULL)
        OR (price_observation_id IS NOT NULL AND price_position IS NOT NULL AND price_crawl_request_id IS NOT NULL
            AND price_cents IS NOT NULL AND price_currency = 'BRL' AND price_observed_at IS NOT NULL)
    ),
    CONSTRAINT projection_price_has_observation CHECK (
        price_observation_id IS NULL OR observation_id IS NOT NULL
    ),
    CONSTRAINT projection_price_position_bound CHECK (
        price_position IS NULL OR price_position <= observation_position
    ),
    CONSTRAINT projection_price_position_identity CHECK (
        price_position IS NULL OR price_position < observation_position
        OR (price_observation_id = observation_id
            AND price_crawl_request_id = observation_crawl_request_id
            AND price_observed_at = observed_at)
    )
);

CREATE INDEX offer_ranking_projection_source_idx
    ON offer_ranking_projection(organization_id, source_id, source_revision);

CREATE INDEX offer_ranking_projection_validation_idx
    ON offer_ranking_projection(validated_at);

-- Durable refresh intent with bounded retries and one outstanding job per
-- offer. A stale job is reset when new work arrives.
CREATE TABLE ranking_refresh_job (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id uuid NOT NULL,
    offer_id uuid NOT NULL,
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at timestamptz,
    terminal boolean NOT NULL DEFAULT false,
    last_error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (organization_id, offer_id)
);

CREATE INDEX ranking_refresh_job_due_idx
    ON ranking_refresh_job(next_attempt_at)
    WHERE next_attempt_at IS NOT NULL;
