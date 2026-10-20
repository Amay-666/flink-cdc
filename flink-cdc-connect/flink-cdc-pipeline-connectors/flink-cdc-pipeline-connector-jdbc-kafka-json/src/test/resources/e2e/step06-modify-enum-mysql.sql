-- ============================================================
-- Step 6: MODIFY COLUMN — the ENUM list is REPLACED (MySQL's whole-
-- list semantics): 'a','b','c' becomes 'a','b','c','d'.
-- Row 4000 stores the new label 'd'; TiCDC renders its 1-based
-- index "4", which the connector must map back using the NEW list
-- (the DDL text carried it) — arriving in Doris as 'd'.
-- ============================================================

ALTER TABLE `type_matrix` MODIFY COLUMN `c_enum` ENUM('a','b','c','d');

INSERT INTO `type_matrix` (`id`, `c_enum`) VALUES (4000, 'd');
