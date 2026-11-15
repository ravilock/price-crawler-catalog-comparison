-- Automatic matches have no human review decision, and pending review suggestions
-- do not have a decision until a manager acts. A decision may only accompany a
-- completed match; its foreign key remains tenant/offer scoped.
ALTER TABLE pending_match_job
    DROP CONSTRAINT pending_match_job_decision_state,
    ADD CONSTRAINT pending_match_job_decision_state
        CHECK (decision_id IS NULL OR status = 'matched');
