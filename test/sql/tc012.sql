--
-- TC012: PREPARE/EXECUTE parameter binding
--
PREPARE act_by_no(int) AS
  SELECT actno, actkwd, actdesc FROM sample.act WHERE actno = $1;

EXPLAIN (analyze,verbose) EXECUTE act_by_no(10);
EXECUTE act_by_no(10);

DEALLOCATE act_by_no;

--
-- END of TC012
--
