-- ============================================================
-- The single-table E2E: the full type matrix table carries the
-- whole lifecycle — snapshot, stream, UPDATE, DELETE, ADD COLUMN,
-- RENAME COLUMN, MODIFY COLUMN (the ENUM list replacement), TABLE
-- RENAME, DROP COLUMN, TRUNCATE and DROP.
--
-- Simplified DDL — no AUTO_INCREMENT, no DEFAULT (Druid DDL parser
-- may not support MySQL-specific syntax).
-- ============================================================

DROP TABLE IF EXISTS `type_matrix`;
DROP TABLE IF EXISTS `type_matrix_renamed`;
DROP TABLE IF EXISTS `type_matrix_like`;

CREATE TABLE `type_matrix` (
    `id` BIGINT NOT NULL,
    `c_tinyint` TINYINT,
    `c_tinyint_u` TINYINT UNSIGNED,
    `c_smallint` SMALLINT,
    `c_smallint_u` SMALLINT UNSIGNED,
    `c_mediumint` MEDIUMINT,
    `c_mediumint_u` MEDIUMINT UNSIGNED,
    `c_int` INT,
    `c_int_u` INT UNSIGNED,
    `c_bigint` BIGINT,
    `c_bigint_u` BIGINT UNSIGNED,
    `c_float` FLOAT,
    `c_double` DOUBLE,
    `c_decimal` DECIMAL(20,4),
    `c_bit1` BIT(1),
    `c_bit8` BIT(8),
    `c_bool` BOOLEAN,
    `c_date` DATE,
    `c_time` TIME,
    `c_time_p` TIME(3),
    `c_datetime` DATETIME,
    `c_datetime_p3` DATETIME(3),
    `c_datetime_p6` DATETIME(6),
    `c_timestamp` TIMESTAMP,
    `c_year` YEAR,
    `c_json` JSON,
    `c_enum` ENUM('a','b','c'),
    `c_set` SET('x','y','z'),
    `c_char` CHAR(8),
    `c_varchar` VARCHAR(100),
    `c_text` TEXT,
    `c_binary` BINARY(8),
    `c_varbinary` VARBINARY(16),
    `c_blob` BLOB,
    PRIMARY KEY (`id`)
);

-- Row 1000 is inserted BEFORE Flink starts: it exercises the JDBC
-- snapshot path (rs.getObject per type).
INSERT INTO `type_matrix` VALUES
(1000, -128, 200, -32768, 65535, -8388608, 16777215, -2147483648, 4294967295,
 -9223372036854775808, 18446744073709551615, 1.25, 3.141592653589793, 123456789.1234,
 b'1', b'10101010', 1, '2026-01-31', '13:45:01', '13:45:01.123', '2026-01-31 10:20:30',
 '2026-01-31 10:20:30.123', '2026-01-31 10:20:30.123456', '2026-01-31 10:20:30', 2026,
 '{"k":1}', 'b', 'x,z', 'hello', '你好🌍', 'text value',
 X'0102030405AABBCC', X'CAFEBABE', X'DEADBEEF');
