--
-- TC015: TABLESAMPLE and PERCENTILE
--
\d+ sample.tbcfileentry
EXPLAIN (analyze,verbose)  SELECT * from sample.tbcfileentry;

EXPLAIN (analyze,verbose)  SELECT * from sample.tbcfileentry TABLESAMPLE SYSTEM (0.1);

EXPLAIN (analyze,verbose) SELECT PERCENTILE_CONT(0.5)
       WITHIN GROUP (ORDER BY tbcfientid)
FROM (
    SELECT *
    FROM sample.tbcfileentry
) x;

--SELECT PERCENTILE_CONT(0.5)
--       WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont 
--       FROM sample.tbcfileentry;
--
-- END of TC015
--
