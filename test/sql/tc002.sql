-- 
-- TC002: Importing a single foreign table from a remote schema
--
\d+ sample.act;
SELECT * FROM sample.act;
-- drop foreign table act
DROP FOREIGN TABLE IF EXISTS sample.act;
-- import it using limit to
IMPORT FOREIGN SCHEMA "DB2INST1" LIMIT TO ("ACT") FROM SERVER sample INTO sample;
-- test its working again
\d+ sample.act;
SELECT * FROM sample.act;
--
-- END of TC002
--