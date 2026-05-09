--
-- TC007: Transaction / rollback semantics for foreign table modifications
--

SELECT deptnumb, location FROM sample.org WHERE deptnumb = 15;

BEGIN;
  UPDATE sample.org SET location = 'Boston (temp)' WHERE deptnumb = 15;
  SELECT deptnumb, location FROM sample.org WHERE deptnumb = 15;
ROLLBACK;

-- Should be back to original
SELECT deptnumb, location FROM sample.org WHERE deptnumb = 15;
--
-- END of TC007
--
