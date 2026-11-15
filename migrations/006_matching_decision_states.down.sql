ALTER TABLE pending_match_job
    DROP CONSTRAINT pending_match_job_decision_state,
    ADD CONSTRAINT pending_match_job_decision_state
        CHECK (
            (status IN ('matched', 'review') AND decision_id IS NOT NULL)
            OR (status NOT IN ('matched', 'review') AND decision_id IS NULL)
        );
