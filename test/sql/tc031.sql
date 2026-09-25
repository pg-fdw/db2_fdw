--
-- TC031: RETURNING and UPDATEs that are not pushed down
--
-- DB2 has no RETURNING clause; db2_fdw runs such a statement as
--   SELECT ... FROM NEW TABLE (INSERT/UPDATE ...)  or
--   SELECT ... FROM OLD TABLE (DELETE ...)
-- and fetches the returned row like a query result.
--
-- An UPDATE that cannot be pushed down to DB2 as a whole (here: because of a
-- condition only PostgreSQL can evaluate) needs the whole row of every
-- scanned row, as does a local row trigger. DB2 has no ROW() constructor,
-- so the scan selects all columns and PostgreSQL builds the row itself.
--
-- Uses DB2INST1.COLDESCTEST (see tc030.sql) and restores its content.
--
-- INSERT ... RETURNING, including binary values with zero bytes
INSERT INTO sample.coldesctest (id, txt_cu32, bin_char, bin_var, ts0)
VALUES (5, 'fünf', '\x00010203deadbeef', '\x00ff00', '2026-09-25 12:00:00')
RETURNING id, txt_cu32, bin_char, bin_var, ts0;
INSERT INTO sample.coldesctest (id, txt_cu32) VALUES (6, 'sechs'), (7, 'sieben')
RETURNING *;
--
-- UPDATE with a local condition: not pushed down, scan needs the whole row
EXPLAIN (COSTS OFF)
UPDATE sample.coldesctest SET txt_cu32 = txt_cu32 || '!' WHERE id >= 5 AND txt_cu32 ~ '^s';
UPDATE sample.coldesctest SET txt_cu32 = txt_cu32 || '!' WHERE id >= 5 AND txt_cu32 ~ '^s';
SELECT id, txt_cu32 FROM sample.coldesctest WHERE id >= 5 ORDER BY id;
--
-- UPDATE ... RETURNING, with expressions over the returned row
UPDATE sample.coldesctest SET bin_var = '\x0a000b' WHERE id = 5
RETURNING id, bin_var, length(bin_var) AS len, txt_cu32 || '?' AS expr;
UPDATE sample.coldesctest SET ts0 = ts0 + interval '1 day' WHERE id = 5
RETURNING id, ts0;
--
-- local row triggers get the complete old and new row
CREATE FUNCTION sample.tc031_trg() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'UPDATE' THEN
    RAISE NOTICE 'tc031 %: old (%, %, %) new (%, %, %)', TG_OP,
      OLD.id, OLD.txt_cu32, OLD.bin_var, NEW.id, NEW.txt_cu32, NEW.bin_var;
    RETURN NEW;
  END IF;
  RAISE NOTICE 'tc031 %: old (%, %, %)', TG_OP, OLD.id, OLD.txt_cu32, OLD.bin_var;
  RETURN OLD;
END $$;
CREATE FUNCTION sample.tc031_trg_ins() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE NOTICE 'tc031 %: new (%, %, %)', TG_OP, NEW.id, NEW.txt_cu32, NEW.ts0;
  RETURN NEW;
END $$;
CREATE TRIGGER tc031_trg AFTER UPDATE OR DELETE ON sample.coldesctest
  FOR EACH ROW EXECUTE FUNCTION sample.tc031_trg();
UPDATE sample.coldesctest SET bin_var = '\x0b' WHERE id = 6;
DELETE FROM sample.coldesctest WHERE id = 7;
DROP TRIGGER tc031_trg ON sample.coldesctest;
--
-- INSERT through partition routing (foreign table as partition) and COPY do
-- not go through the planner's modify path, but use BeginForeignInsert
CREATE TABLE sample.tc031_parted (LIKE sample.coldesctest) PARTITION BY RANGE (id);
CREATE TABLE sample.tc031_parted_local PARTITION OF sample.tc031_parted FOR VALUES FROM (MINVALUE) TO (5);
ALTER TABLE sample.tc031_parted ATTACH PARTITION sample.coldesctest FOR VALUES FROM (5) TO (MAXVALUE);
INSERT INTO sample.tc031_parted (id, txt_cu32, bin_var) VALUES (10, 'zehn', '\x00ff')
RETURNING id, txt_cu32, bin_var, tableoid::regclass;
INSERT INTO sample.tc031_parted (id, txt_cu32) VALUES (3, 'drei'), (11, 'elf')
RETURNING tc031_parted, tableoid::regclass;
ALTER TABLE sample.tc031_parted DETACH PARTITION sample.coldesctest;
DROP TABLE sample.tc031_parted;
-- plain COPY (the "key" column must not be taken from a non-existing old row)
COPY sample.coldesctest (id, txt_cu32) FROM stdin;
12	zwölf
\.
-- COPY with a local row trigger, which gets the complete new row
CREATE TRIGGER tc031_trg AFTER INSERT ON sample.coldesctest
  FOR EACH ROW EXECUTE FUNCTION sample.tc031_trg_ins();
COPY sample.coldesctest (id, txt_cu32, ts0) FROM stdin;
13	dreizehn	2026-09-25 08:00:00
\.
DROP TRIGGER tc031_trg ON sample.coldesctest;
--
-- DELETE ... RETURNING restores the table content (sorted, DB2 decides the row order)
WITH deleted AS (
  DELETE FROM sample.coldesctest WHERE id >= 5 RETURNING id, txt_cu32, bin_char, bin_var
)
SELECT * FROM deleted ORDER BY id;
SELECT count(*) AS rows_left FROM sample.coldesctest;
DROP FUNCTION sample.tc031_trg();
DROP FUNCTION sample.tc031_trg_ins();
--
-- END of TC031
--
