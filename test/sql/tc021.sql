--
-- TC021: CLOB/DBCLOB Unicode round-trip correctness (CODEUNITS16/32, UTF-16)
--
-- Verifies that db2_fdw correctly receives DB2 character-large-object data
-- regardless of the DB2-side storage CCSID, in particular DB2's genuinely
-- UTF-16-encoded (CCSID 1200) DBCLOB type.
--
-- Background: "CLOB(n CODEUNITS16)" is not valid DB2 LUW syntax -- CODEUNITS16
-- is only accepted for GRAPHIC/VARGRAPHIC/DBCLOB columns, while CLOB/VARCHAR/
-- CHAR only accept CODEUNITS32 (both are pure length-unit qualifiers on top of
-- the table's normal CCSID, e.g. 1208/UTF-8; they do not change the on-wire
-- encoding). The DB2 type that is actually stored as UTF-16 on the wire
-- (CCSID 1200, confirmed via SYSCAT.COLUMNS.CODEPAGE) is DBCLOB. This test
-- exercises that type directly, alongside a plain CLOB and a CODEUNITS32 CLOB
-- for comparison, so a regression in CCSID-1200 transcoding is caught even
-- though it is invisible to callers that only ever use CLOB.
--
-- Requires DB2INST1.CLOBUTF16 to pre-exist in the SAMPLE database:
--   CREATE TABLE DB2INST1.CLOBUTF16 (
--     ID          INTEGER NOT NULL PRIMARY KEY,
--     TXT_ASCII   CLOB(1K),
--     TXT_UTF8    CLOB(1K),
--     TXT_CU32    CLOB(1K CODEUNITS32),
--     TXT_DBCLOB  DBCLOB(20K)
--   );
--
--   INSERT INTO DB2INST1.CLOBUTF16 (ID, TXT_ASCII, TXT_UTF8, TXT_CU32, TXT_DBCLOB) VALUES
--     (1, 'Hello DB2!', 'Hello DB2!', 'Hello DB2!', 'Hello DB2!');
--   INSERT INTO DB2INST1.CLOBUTF16 (ID, TXT_ASCII, TXT_UTF8, TXT_CU32, TXT_DBCLOB) VALUES
--     (2, 'Gruesse aus Muenchen', 'Grüße aus München, 日本語テスト', 'Grüße aus München, 日本語テスト', 'Grüße aus München, 日本語テスト');
--   INSERT INTO DB2INST1.CLOBUTF16 (ID, TXT_ASCII, TXT_UTF8, TXT_CU32, TXT_DBCLOB) VALUES
--     (3, 'astral test', '😀🎉 astral test 𝄞', '😀🎉 astral test 𝄞', '😀🎉 astral test 𝄞');
--   INSERT INTO DB2INST1.CLOBUTF16 (ID, TXT_ASCII, TXT_UTF8, TXT_CU32, TXT_DBCLOB) VALUES
--     (4, NULL, NULL, NULL, NULL);
--   INSERT INTO DB2INST1.CLOBUTF16 (ID, TXT_ASCII, TXT_UTF8, TXT_CU32, TXT_DBCLOB) VALUES
--     (5, 'chunk boundary', 'chunk boundary', 'chunk boundary',
--      REPEAT('x', 8191) || 'ü' || REPEAT('y', 100));
--
-- Row 3 uses characters outside the Unicode BMP (require a UTF-16 surrogate
-- pair on the DB2 side, i.e. 2 CODEUNITS16 units / 4 bytes each), which
-- specifically exercises DBCLOB's UTF-16 code-unit semantics rather than
-- just its byte-level CCSID.
-- Row 5 places a 2-byte UTF-8 character exactly across the FDW's 8192-byte
-- LOB fetch chunk boundary, sourced from the CCSID-1200 DBCLOB column (the
-- multi-CCSID analogue of TC020's plain-CLOB chunk-boundary case).
-- (Already picked up as sample.clobutf16 by tcstart.sql's IMPORT FOREIGN
-- SCHEMA, same as any other pre-existing DB2INST1 table.)
--
\d+ sample.clobutf16;
--
-- plain ASCII, across all four columns
SELECT id,
       txt_ascii  = 'Hello DB2!' AS ascii_matches,
       txt_utf8   = 'Hello DB2!' AS utf8_matches,
       txt_cu32   = 'Hello DB2!' AS cu32_matches,
       txt_dbclob = 'Hello DB2!' AS dbclob_matches
FROM sample.clobutf16 WHERE id = 1;
--
-- multi-byte UTF-8 BMP content (German umlauts, Japanese), including in the
-- CCSID-1200 DBCLOB column
SELECT id,
       txt_utf8   = 'Grüße aus München, 日本語テスト' AS utf8_matches,
       txt_cu32   = 'Grüße aus München, 日本語テスト' AS cu32_matches,
       txt_dbclob = 'Grüße aus München, 日本語テスト' AS dbclob_matches
FROM sample.clobutf16 WHERE id = 2;
--
-- astral-plane characters (surrogate pairs on the DB2 UTF-16 side)
SELECT id,
       txt_utf8   = '😀🎉 astral test 𝄞' AS utf8_matches,
       txt_cu32   = '😀🎉 astral test 𝄞' AS cu32_matches,
       txt_dbclob = '😀🎉 astral test 𝄞' AS dbclob_matches
FROM sample.clobutf16 WHERE id = 3;
--
-- NULL handling
SELECT id, txt_ascii, txt_utf8, txt_cu32, txt_dbclob
FROM sample.clobutf16 WHERE id = 4;
--
-- multi-chunk content with a multi-byte character straddling the 8192-byte
-- LOB_CHUNK_SIZE boundary, sourced from the CCSID-1200 DBCLOB column
SELECT id, length(txt_dbclob) AS charlen, octet_length(txt_dbclob) AS bytelen,
       txt_dbclob = (repeat('x', 8191) || 'ü' || repeat('y', 100)) AS content_matches
FROM sample.clobutf16 WHERE id = 5;
--
-- END of TC021
--
