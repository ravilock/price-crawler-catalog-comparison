DROP TABLE pending_match_job;

ALTER TABLE review_decision
    DROP CONSTRAINT review_decision_tenant_offer_id_unique;
