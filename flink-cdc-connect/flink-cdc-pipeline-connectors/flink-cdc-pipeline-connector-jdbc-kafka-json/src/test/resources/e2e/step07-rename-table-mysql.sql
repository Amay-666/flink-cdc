-- ============================================================
-- Step 7: RENAME TABLE — the wide matrix table is renamed; row
-- 5000 flows into the renamed table.
-- ============================================================

RENAME TABLE `type_matrix` TO `type_matrix_renamed`;

INSERT INTO `type_matrix_renamed` (`id`, `c_varchar`) VALUES (5000, 'renamed-row');
