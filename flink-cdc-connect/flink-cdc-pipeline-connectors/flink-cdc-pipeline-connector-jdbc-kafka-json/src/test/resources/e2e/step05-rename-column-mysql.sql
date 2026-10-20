-- ============================================================
-- Step 5: RENAME COLUMN — one of the matrix's typed columns is
-- renamed; the UPDATE that follows carries the new column name.
-- ============================================================

ALTER TABLE `type_matrix` RENAME COLUMN `c_char` TO `c_char2`;

UPDATE `type_matrix` SET `c_char2` = 'ZZTop' WHERE `id` = 3000;
