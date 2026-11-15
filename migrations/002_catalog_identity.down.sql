-- Destructive rollback: catalog entities and review history cannot be recovered.
DROP TABLE comparison_group;
DROP TRIGGER catalog_audit_append_only ON catalog_audit_record;
DROP FUNCTION reject_catalog_audit_change();
DROP TABLE catalog_audit_record;
DROP TABLE offer_match;
DROP TRIGGER review_decision_append_only ON review_decision;
DROP FUNCTION reject_review_decision_change();
DROP TABLE review_decision;
DROP TABLE match_suggestion;
DROP TABLE catalog_product;
