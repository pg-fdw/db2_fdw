--
-- TC019: test using a query as table for a fdw table
--
DROP FOREIGN TABLE IF EXISTS sample.percent;
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

DROP FOREIGN TABLE IF EXISTS sample.percent3;
CREATE FOREIGN TABLE sample.percent3 (
    p10 NUMERIC
   ,p11 NUMERIC
   ,p12 NUMERIC
   ,p13 NUMERIC
)
SERVER sample
OPTIONS ( table '(SELECT PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY TBCFIENTID) AS p10'\
                ',PERCENTILE_CONT(0.11) WITHIN GROUP (ORDER BY TBCFIENTID) AS p11'\
                ',PERCENTILE_CONT(0.12) WITHIN GROUP (ORDER BY TBCFIENTID) AS p12'\
                ',PERCENTILE_CONT(0.13) WITHIN GROUP (ORDER BY TBCFIENTID) AS p13'\
                ' FROM DB2INST1.TBCFILEENTRY)');
--
\d+ sample.percent3;
--
explain (analyze,verbose) select * from sample.percent3;
select * from sample.percent3;

--
-- END of TC019
--