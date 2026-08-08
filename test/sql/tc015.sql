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

EXPLAIN (verbose)
SELECT PERCENTILE_CONT(0.11) WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont_0_1
     , PERCENTILE_CONT(0.12) WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont_0_2
     , PERCENTILE_CONT(0.13) WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont_0_3
     , PERCENTILE_CONT(0.14) WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont_0_4
     , PERCENTILE_CONT(0.15) WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont_0_5
     , PERCENTILE_CONT(0.16) WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont_0_6
     , PERCENTILE_CONT(0.17) WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont_0_7
     , PERCENTILE_CONT(0.18) WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont_0_8
     , PERCENTILE_CONT(0.19) WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont_0_9
     , PERCENTILE_CONT(0.20) WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont_1_0
     , PERCENTILE_CONT(0.21) WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont_1_1
       FROM sample.tbcfileentry;

SELECT PERCENTILE_CONT(0.11) WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont_0_1
     , PERCENTILE_CONT(0.12) WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont_0_2
     , PERCENTILE_CONT(0.13) WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont_0_3
     , PERCENTILE_CONT(0.14) WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont_0_4
     , PERCENTILE_CONT(0.15) WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont_0_5
     , PERCENTILE_CONT(0.16) WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont_0_6
     , PERCENTILE_CONT(0.17) WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont_0_7
     , PERCENTILE_CONT(0.18) WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont_0_8
     , PERCENTILE_CONT(0.19) WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont_0_9
     , PERCENTILE_CONT(0.20) WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont_1_0
     , PERCENTILE_CONT(0.21) WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont_1_1
       FROM sample.tbcfileentry;

EXPLAIN (verbose)
SELECT PERCENTILE_DISC(0.11) WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont_0_1
     , PERCENTILE_DISC(0.12) WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont_0_2
     , PERCENTILE_DISC(0.13) WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont_0_3
     , PERCENTILE_DISC(0.14) WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont_0_4
     , PERCENTILE_DISC(0.15) WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont_0_5
     , PERCENTILE_DISC(0.16) WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont_0_6
     , PERCENTILE_DISC(0.17) WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont_0_7
     , PERCENTILE_DISC(0.18) WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont_0_8
     , PERCENTILE_DISC(0.19) WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont_0_9
     , PERCENTILE_DISC(0.20) WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont_1_0
     , PERCENTILE_DISC(0.21) WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont_1_1
       FROM sample.tbcfileentry;

SELECT PERCENTILE_DISC(0.11) WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont_0_1
     , PERCENTILE_DISC(0.12) WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont_0_2
     , PERCENTILE_DISC(0.13) WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont_0_3
     , PERCENTILE_DISC(0.14) WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont_0_4
     , PERCENTILE_DISC(0.15) WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont_0_5
     , PERCENTILE_DISC(0.16) WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont_0_6
     , PERCENTILE_DISC(0.17) WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont_0_7
     , PERCENTILE_DISC(0.18) WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont_0_8
     , PERCENTILE_DISC(0.19) WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont_0_9
     , PERCENTILE_DISC(0.20) WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont_1_0
     , PERCENTILE_DISC(0.21) WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont_1_1
       FROM sample.tbcfileentry;
--
-- END of TC015
--