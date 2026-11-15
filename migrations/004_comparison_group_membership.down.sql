DROP TABLE comparison_membership_snapshot;
DROP TABLE comparison_membership_journal;
DROP TABLE comparison_membership_journal_counter;
DROP TABLE comparison_group_mutation;
DROP TABLE comparison_group_product;

ALTER TABLE comparison_group
    DROP COLUMN revision,
    DROP COLUMN state,
    DROP COLUMN name;
