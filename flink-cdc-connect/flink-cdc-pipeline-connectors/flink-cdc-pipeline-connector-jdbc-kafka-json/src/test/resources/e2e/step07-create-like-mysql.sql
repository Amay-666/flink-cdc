-- ============================================================
-- Step 7: CREATE TABLE LIKE + column type change fallback —
-- the DDL text carries no column definitions, so the source-side
-- handler resolves the schema (in-memory store first, JDBC target
-- fallback). Row 7000 is written into the new table and verified
-- in Doris. Row 8000 carries a numeric c_text, then c_text is
-- changed from TEXT to INT — Doris cannot convert STRING to INT
-- in place, so the applier falls back to the temp-column swap
-- (add, cast, drop, rename).
-- ============================================================

CREATE TABLE `type_matrix_like` LIKE `type_matrix`;

INSERT INTO `type_matrix_like` (`id`, `c_varchar`) VALUES (7000, 'like-created');

INSERT INTO `type_matrix_like` (`id`, `c_varchar`, `c_text`) VALUES (8000, 'numeric-text', '12345');

ALTER TABLE `type_matrix_like` MODIFY COLUMN `c_text` INT;
