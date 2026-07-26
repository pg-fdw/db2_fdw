--
-- TC019: test using a query as table for a fdw table
--
CREATE FOREIGN TABLE sample.percent (
    percent NUMERIC
)
SERVER sample
OPTIONS ( table '(SELECT PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY TBCFIENTID) AS percent FROM DB2INST1.TBCFILEENTRY)');
--
\d+ sample.percent;
--
explain (analyze,verbose) select * from sample.percent;
select * from sample.percent;
--
-- END of TC019
--
