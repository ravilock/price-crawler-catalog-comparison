ALTER TABLE pending_match_job
    DROP CONSTRAINT pending_match_job_ready_candidate,
    DROP COLUMN resolved_candidate;
