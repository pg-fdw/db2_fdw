--
-- TC015: TABLESAMPLE and PERCENTILE
--
\d+ sample.tbcfileentry

SELECT * from sample.tbcfileentry TABLESAMPLE SYSTEM (0.1);

SELECT PERCENTILE_CONT(0.5)
       WITHIN GROUP (ORDER BY tbcfientid) AS Perc_Cont 
       FROM sample.tbcfileentry;
--
-- END of TC015
--
