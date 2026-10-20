-- ============================================================
-- Step 3: UPDATE + DELETE on typed columns.
-- The UPDATE flows through the canal before/after images (the
-- whole 34-column row travels per event); the DELETE removes the
-- streaming row.
-- ============================================================

UPDATE `type_matrix`
SET `c_varchar` = 'Updated via canal', `c_tinyint` = 42, `c_decimal` = 999.9900
WHERE `id` = 1000;

DELETE FROM `type_matrix` WHERE `id` = 2000;
