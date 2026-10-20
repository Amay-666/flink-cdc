-- ============================================================
-- Step 4: ADD COLUMN — a new column on the wide matrix table;
-- row 3000 exercises an INSERT that carries it.
-- ============================================================

ALTER TABLE `type_matrix` ADD COLUMN `extra` VARCHAR(50) AFTER `c_varchar`;

INSERT INTO `type_matrix` (`id`, `c_varchar`, `extra`) VALUES (3000, 'row three', 'brand-new');
