--
-- TC020: CLOB round-trip correctness
--
-- Regression test for bugs in db2GetLob.c's chunked LOB fetch that
-- corrupted multi-byte CLOB content:
--  1. column->colType (the raw DB2 CLI SQL_CLOB constant, -99) was
--     compared against the FDW's internal nonSQLType enum value
--     DB2_CLOB (24) instead, so CLOB columns were always fetched via
--     SQL_C_BINARY (no CCSID conversion) rather than SQL_C_CHAR.
--  2. db2CheckErr() maps SQL_SUCCESS_WITH_INFO to SQL_SUCCESS, so the
--     chunked-fetch loop's "while (rc == SQL_SUCCESS_WITH_INFO)" never
--     iterated past the first LOB_CHUNK_SIZE (8192 byte) chunk,
--     silently truncating any larger CLOB/BLOB value.
--  3. The realloc() call for subsequent chunks did not reserve a byte
--     for the eventual string terminator: a 1-byte heap overflow for
--     any LOB spanning more than one chunk.
--
-- Requires DB2INST1.CLOBTEST(ID INTEGER NOT NULL PRIMARY KEY, TXT CLOB(10K))
-- to pre-exist in the SAMPLE database, populated with:
--   INSERT INTO DB2INST1.CLOBTEST (ID, TXT) VALUES
--     (1, 'Gruesse aus Muenchen: Grüße, 日本語テスト 😀'),
--     (2, REPEAT('x', 8191) || 'ü' || REPEAT('y', 100));
-- Row 2 places a 2-byte UTF-8 character exactly across the FDW's
-- 8192-byte LOB fetch chunk boundary.
-- (Already picked up as sample.clobtest by tcstart.sql's IMPORT FOREIGN
-- SCHEMA, same as any other pre-existing DB2INST1 table.)
--
\d+ sample.clobtest;
--
-- short, single-chunk multi-byte content (Latin-1 accents, CJK, emoji)
SELECT id, length(txt) AS charlen, octet_length(txt) AS bytelen,
       txt = 'Gruesse aus Muenchen: Grüße, 日本語テスト 😀' AS content_matches
FROM sample.clobtest WHERE id = 1;
--
-- multi-chunk content with a multi-byte character straddling the
-- 8192-byte LOB_CHUNK_SIZE boundary
SELECT id, length(txt) AS charlen, octet_length(txt) AS bytelen,
       txt = (repeat('x', 8191) || 'ü' || repeat('y', 100)) AS content_matches
FROM sample.clobtest WHERE id = 2;
--
-- END of TC020
--
