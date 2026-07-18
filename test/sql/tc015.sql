--
-- TC015: TABLESAMPLE and PERCENTILE
--
\d+ sample.tbcfileentry

\d+ sample.tbcfileentry_sample

EXPLAIN (analyze,verbose)  SELECT * from sample.tbcfileentry_sample;

EXPLAIN (verbose) SELECT PERCENTILE_CONT(0.5)
       WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont
       FROM sample.tbcfileentry;

SELECT PERCENTILE_CONT(0.5)
       WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont
       FROM sample.tbcfileentry;

SELECT PERCENTILE_DISC(0.5)
       WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Disc
       FROM sample.tbcfileentry;

--
-- END of TC015
--
