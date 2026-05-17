--
-- TC006: UPDATE/DELETE against a key column and revert to baseline
--
-- For UPDATE/DELETE, db2_fdw requires at least one primary key column marked
-- with the column option "key".
-- The DB2 SAMPLE.ORG table uses DEPTNUMB as its primary key.
ALTER FOREIGN TABLE sample.org
  ALTER COLUMN deptnumb OPTIONS (ADD key 'true');

\d+ sample.org;

-- Baseline row
SELECT deptnumb, deptname, location FROM sample.org WHERE deptnumb = 10;

-- UPDATE + verify
UPDATE sample.org SET location = 'New York City' WHERE deptnumb = 10;
SELECT deptnumb, deptname, location FROM sample.org WHERE deptnumb = 10;

-- Revert + verify
UPDATE sample.org SET location = 'New York' WHERE deptnumb = 10;
SELECT deptnumb, deptname, location FROM sample.org WHERE deptnumb = 10;

-- DELETE + INSERT to test DELETE/INSERT pair (choose a row we can reconstruct deterministically)
SELECT * FROM sample.org WHERE deptnumb = 84;
DELETE FROM sample.org WHERE deptnumb = 84;
SELECT * FROM sample.org WHERE deptnumb = 84;

INSERT INTO sample.org (deptnumb, deptname, manager, division, location)
VALUES (84, 'Mountain', 290, 'Western', 'Denver');
SELECT * FROM sample.org WHERE deptnumb = 84;

--
-- END of TC006
--
