--
-- TC015: TABLESAMPLE and PERCENTILE
--
\d+ sample.tbcfileentry

\d+ sample.tbcfileentry_sample

EXPLAIN (analyze,verbose)  SELECT * from sample.tbcfileentry_sample;

--EXPLAIN (analyze,verbose) SELECT PERCENTILE_CONT(0.5)
--       WITHIN GROUP (ORDER BY tbcfientid)
--FROM (
--    SELECT tbcfientid
--    FROM sample.tbcfileentry
--);

--
-- END of TC015
--
