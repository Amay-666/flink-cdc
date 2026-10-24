-- ============================================================
-- Step 8: DROP COLUMN — one of the matrix's typed columns is
-- dropped; row 6000 flows without it.
-- ============================================================

ALTER TABLE `type_matrix_renamed` DROP COLUMN `c_text`;

INSERT INTO `type_matrix_renamed` (`id`, `c_varchar`) VALUES (6000, 'after-drop');
