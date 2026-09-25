--
-- TC030: column description of types the DB2 SAMPLE schema does not cover
--
-- Covers the column types that were missing when comparing the catalog-based
-- column description (SYSCAT.COLUMNS) against SQLDescribeCol/SQLColAttribute
-- during IMPORT FOREIGN SCHEMA: CODEUNITS32 strings (SYSCAT.COLUMNS.LENGTH is
-- in bytes, STRINGUNITSLENGTH in characters), CHAR FOR BIT DATA (a CHARACTER
-- column with CODEPAGE 0, imported as bytea), LONG VARGRAPHIC (CCSID 1200) and
-- TIMESTAMP(0) (SCALE 0, no fractional seconds), plus a VARBINARY column. The
-- binary columns are also written through the FDW, with values containing
-- zero bytes, and read back as raw bytes (not DB2's hex text). The \d+ output records the
-- imported db2type/db2size/db2bytes/db2chars/db2scale/db2ccsid options, so any
-- change in how these columns are described shows up as a diff.
--
-- Requires DB2INST1.COLDESCTEST to pre-exist in the SAMPLE database:
--   CREATE TABLE DB2INST1.COLDESCTEST (
--     ID        INTEGER NOT NULL PRIMARY KEY,
--     TXT_CU32  VARCHAR(20 CODEUNITS32),
--     CHR_CU32  CHAR(5 CODEUNITS32),
--     BIN_CHAR  CHAR(8) FOR BIT DATA,
--     TXT_LVG   LONG VARGRAPHIC,
--     TS0       TIMESTAMP(0),
--     BIN_VAR   VARBINARY(16)
--   );
--
--   INSERT INTO DB2INST1.COLDESCTEST VALUES
--     (1, 'Grüße 日本 😀', 'äöü', X'DEADBEEF00010203', 'Grüße aus München', '2026-01-02-03.04.05', BX'00FF0010');
--   INSERT INTO DB2INST1.COLDESCTEST VALUES
--     (2, NULL, NULL, NULL, NULL, NULL, NULL);
--
-- (Already picked up as sample.coldesctest by tcstart.sql's IMPORT FOREIGN
-- SCHEMA, same as any other pre-existing DB2INST1 table.)
--
\d+ sample.coldesctest;
--
-- content round trip, including 4-byte UTF-8 in the CODEUNITS32 column and
-- the blank padding of CHAR(5 CODEUNITS32)
SELECT id, txt_cu32, chr_cu32, bin_char, txt_lvg, ts0, bin_var
FROM sample.coldesctest ORDER BY id;
--
SELECT id,
       txt_cu32 = 'Grüße 日本 😀'           AS cu32_matches,
       length(txt_cu32)                     AS cu32_chars,
       bin_char = '\xdeadbeef00010203'::bytea AS bin_matches,
       txt_lvg  = 'Grüße aus München'       AS lvg_matches,
       ts0      = '2026-01-02 03:04:05'     AS ts0_matches,
       bin_var  = '\x00ff0010'::bytea         AS binvar_matches
FROM sample.coldesctest WHERE id = 1;
--
-- binary write path: zero bytes must survive (they used to be cut off by
-- binding with SQL_NTS), CHAR(8) FOR BIT DATA pads short values with blanks
-- (0x20); bytea constants in UPDATEs are pushed down as BX'...' literals
INSERT INTO sample.coldesctest (id, bin_char, bin_var) VALUES (3, '\x0001020300000000', '\x00');
INSERT INTO sample.coldesctest (id, bin_char, bin_var) VALUES (4, '\xff00', '\x');
UPDATE sample.coldesctest SET bin_var = '\x0a000b000c' WHERE id = 3;
UPDATE sample.coldesctest SET bin_char = NULL WHERE id = 4;
SELECT id, bin_char, bin_var, length(bin_var) AS binvar_len
FROM sample.coldesctest WHERE id IN (3, 4) ORDER BY id;
-- bytea constant in a pushed-down WHERE clause
SELECT id FROM sample.coldesctest WHERE bin_var = '\x0a000b000c';
DELETE FROM sample.coldesctest WHERE id IN (3, 4);
SELECT count(*) AS rows_left FROM sample.coldesctest;
--
-- END of TC030
--
