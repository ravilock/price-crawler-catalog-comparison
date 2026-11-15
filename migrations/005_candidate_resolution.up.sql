ALTER TABLE pending_match_job
    ADD COLUMN resolved_candidate jsonb,
    ADD CONSTRAINT pending_match_job_ready_candidate CHECK (
        (status = 'ready' AND resolved_candidate IS NOT NULL)
        OR (status <> 'ready' AND resolved_candidate IS NULL)
    );
