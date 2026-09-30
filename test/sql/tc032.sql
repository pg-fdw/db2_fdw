--
-- TC032: IMPORT FOREIGN SCHEMA with DB2 names that differ only in case
--
-- DB2 folds ordinary identifiers to upper case, but delimited identifiers keep
-- their case, so FDWCASE, "fdwcase" and "FdwCase" are three different schemas,
-- and a schema can hold MYTAB, "MyTab" and "mytab" side by side.
-- IMPORT FOREIGN SCHEMA resolves the remote schema name and every LIMIT TO /
-- EXCEPT entry the same way: an exact match wins, otherwise the name matches
-- ignoring case, but only if that is unambiguous. Each imported table must
-- describe the right DB2 table (columns) and point to it (schema/table options).
--
-- Requires these DB2 objects to pre-exist in the SAMPLE database:
--   CREATE SCHEMA FDWCASE;
--   CREATE SCHEMA "fdwcase";
--   CREATE SCHEMA "FdwCase";
--   CREATE SCHEMA FDWUNIQ;
--   CREATE TABLE FDWCASE.T1        (ID INTEGER NOT NULL PRIMARY KEY, SRC VARCHAR(30));
--   CREATE TABLE FDWCASE.MYTAB     (ID INTEGER NOT NULL PRIMARY KEY, SRC VARCHAR(30));
--   CREATE TABLE FDWCASE."MyTab"   (ID INTEGER NOT NULL PRIMARY KEY, SRC VARCHAR(30), EXTRA INTEGER);
--   CREATE TABLE FDWCASE."mytab"   (ID INTEGER NOT NULL PRIMARY KEY, SRC VARCHAR(30), D DATE);
--   CREATE TABLE "fdwcase".T1      (ID INTEGER NOT NULL PRIMARY KEY, SRC VARCHAR(30), LOWERONLY INTEGER);
--   CREATE TABLE "FdwCase".T1      (ID INTEGER NOT NULL PRIMARY KEY, SRC VARCHAR(30), MIXEDONLY CHAR(3));
--   CREATE TABLE FDWUNIQ.ORDERS    (ID INTEGER NOT NULL PRIMARY KEY, SRC VARCHAR(30));
--   CREATE TABLE FDWUNIQ."Items"   (ID INTEGER NOT NULL PRIMARY KEY, SRC VARCHAR(30));
--   INSERT INTO FDWCASE.T1        VALUES (1, 'FDWCASE.T1');
--   INSERT INTO FDWCASE.MYTAB     VALUES (1, 'FDWCASE.MYTAB');
--   INSERT INTO FDWCASE."MyTab"   VALUES (1, 'FDWCASE.MyTab', 42);
--   INSERT INTO FDWCASE."mytab"   VALUES (1, 'FDWCASE.mytab', '2026-09-30');
--   INSERT INTO "fdwcase".T1      VALUES (1, 'fdwcase.T1', 7);
--   INSERT INTO "FdwCase".T1      VALUES (1, 'FdwCase.T1', 'abc');
--   INSERT INTO FDWUNIQ.ORDERS    VALUES (1, 'FDWUNIQ.ORDERS');
--   INSERT INTO FDWUNIQ."Items"   VALUES (1, 'FDWUNIQ.Items');
--
CREATE SCHEMA tc032;
-- the imported foreign tables of a local schema: DB2 schema/table options and columns
CREATE FUNCTION tc032.imported(nsp name)
RETURNS TABLE (foreign_table name, options text, columns text) LANGUAGE sql AS $$
  SELECT c.relname, array_to_string(ft.ftoptions, ', '),
         (SELECT string_agg(a.attname, ', ' ORDER BY a.attnum) FROM pg_attribute a
          WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped)
  FROM pg_foreign_table ft
  JOIN pg_class c ON c.oid = ft.ftrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname = nsp
  ORDER BY c.relname COLLATE "C"
$$;
--
-- exact schema matches: each of the three schemas gets its own T1
CREATE SCHEMA tc032_upper;
IMPORT FOREIGN SCHEMA "FDWCASE" LIMIT TO ("T1") FROM SERVER sample INTO tc032_upper;
SELECT * FROM tc032.imported('tc032_upper');
SELECT * FROM tc032_upper.t1;
CREATE SCHEMA tc032_lower;
IMPORT FOREIGN SCHEMA fdwcase FROM SERVER sample INTO tc032_lower;
SELECT * FROM tc032.imported('tc032_lower');
SELECT * FROM tc032_lower.t1;
-- the generated DB2 SQL must quote the lowercase schema, or it would hit FDWCASE.T1
INSERT INTO tc032_lower.t1 VALUES (2, 'inserted', 8);
UPDATE tc032_lower.t1 SET loweronly = 9 WHERE id = 2;
SELECT * FROM tc032_lower.t1 ORDER BY id;
DELETE FROM tc032_lower.t1 WHERE id = 2;
SELECT * FROM tc032_lower.t1 ORDER BY id;
CREATE SCHEMA tc032_mixed;
IMPORT FOREIGN SCHEMA "FdwCase" FROM SERVER sample INTO tc032_mixed;
SELECT * FROM tc032.imported('tc032_mixed');
SELECT * FROM tc032_mixed.t1;
--
-- no exact match and more than one ignoring case: ambiguous
CREATE SCHEMA tc032_err;
IMPORT FOREIGN SCHEMA "FDWcase" FROM SERVER sample INTO tc032_err;
-- no match at all: nothing to import
IMPORT FOREIGN SCHEMA "FDWNONE" FROM SERVER sample INTO tc032_err;
SELECT * FROM tc032.imported('tc032_err');
--
-- no exact match, but a unique one ignoring case: fdwuniq finds FDWUNIQ,
-- and the schema option is the DB2 catalog name
CREATE SCHEMA tc032_uniq;
IMPORT FOREIGN SCHEMA fdwuniq FROM SERVER sample INTO tc032_uniq;
SELECT * FROM tc032.imported('tc032_uniq');
SELECT * FROM tc032_uniq.orders;
SELECT * FROM tc032_uniq."Items";
-- LIMIT TO: items finds "Items", ORDERS is not imported
DROP SCHEMA tc032_uniq CASCADE;
CREATE SCHEMA tc032_uniq;
IMPORT FOREIGN SCHEMA fdwuniq LIMIT TO (items) FROM SERVER sample INTO tc032_uniq;
SELECT * FROM tc032.imported('tc032_uniq');
SELECT * FROM tc032_uniq."Items";
--
-- tables MYTAB, "MyTab" and "mytab" in one schema
-- case 'keep': all four tables, each with its own columns
CREATE SCHEMA tc032_keep;
IMPORT FOREIGN SCHEMA "FDWCASE" FROM SERVER sample INTO tc032_keep OPTIONS (case 'keep');
SELECT * FROM tc032.imported('tc032_keep');
SELECT * FROM tc032_keep."MYTAB";
SELECT * FROM tc032_keep."MyTab";
SELECT * FROM tc032_keep.mytab;
-- case 'smart' (the default) folds MYTAB to mytab, which collides with "mytab"
IMPORT FOREIGN SCHEMA "FDWCASE" FROM SERVER sample INTO tc032_err;
-- ... and so does case 'lower' with "MyTab"
IMPORT FOREIGN SCHEMA "FDWCASE" LIMIT TO ("MYTAB", "MyTab") FROM SERVER sample INTO tc032_err OPTIONS (case 'lower');
SELECT * FROM tc032.imported('tc032_err');
-- LIMIT TO picks the exact table
CREATE SCHEMA tc032_limit1;
IMPORT FOREIGN SCHEMA "FDWCASE" LIMIT TO (mytab) FROM SERVER sample INTO tc032_limit1;
SELECT * FROM tc032.imported('tc032_limit1');
SELECT * FROM tc032_limit1.mytab;
CREATE SCHEMA tc032_limit2;
IMPORT FOREIGN SCHEMA "FDWCASE" LIMIT TO ("MYTAB", "MyTab", t1) FROM SERVER sample INTO tc032_limit2;
SELECT * FROM tc032.imported('tc032_limit2');
SELECT * FROM tc032_limit2.mytab;
SELECT * FROM tc032_limit2."MyTab";
SELECT * FROM tc032_limit2.t1;
-- LIMIT TO with no exact match and more than one ignoring case: ambiguous
IMPORT FOREIGN SCHEMA "FDWCASE" LIMIT TO ("myTAB") FROM SERVER sample INTO tc032_err;
-- EXCEPT excludes exactly the named tables
CREATE SCHEMA tc032_except;
IMPORT FOREIGN SCHEMA "FDWCASE" EXCEPT ("MYTAB", mytab) FROM SERVER sample INTO tc032_except;
SELECT * FROM tc032.imported('tc032_except');
SELECT * FROM tc032_except."MyTab";
-- excluding "mytab" while importing MYTAB as mytab cannot work with case folding
IMPORT FOREIGN SCHEMA "FDWCASE" EXCEPT (mytab, "MyTab") FROM SERVER sample INTO tc032_err;
SELECT * FROM tc032.imported('tc032_err');
--
DROP SCHEMA tc032_upper, tc032_lower, tc032_mixed, tc032_err, tc032_uniq, tc032_keep,
            tc032_limit1, tc032_limit2, tc032_except, tc032 CASCADE;
--
-- END of TC032
--
