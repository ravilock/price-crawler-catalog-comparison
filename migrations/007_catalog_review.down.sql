DROP TABLE catalog_review_idempotency;
DROP TABLE catalog_review_cursor;
DROP TABLE catalog_review_snapshot_item;
DROP TABLE catalog_review_snapshot;

ALTER TABLE pending_match_job
    DROP CONSTRAINT pending_match_job_decision_state,
    DROP CONSTRAINT pending_match_job_status_check,
    ADD CONSTRAINT pending_match_job_status_check
        CHECK (status IN ('pending', 'ready', 'matched', 'review', 'failed')),
    ADD CONSTRAINT pending_match_job_decision_state
        CHECK (decision_id IS NULL OR status = 'matched');

ALTER TABLE match_suggestion DROP COLUMN title;
