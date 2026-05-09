--
-- TC011: ORDER BY with LIMIT/OFFSET
--
EXPLAIN (analyze,verbose) SELECT empno, lastname, salary FROM sample.employee ORDER BY salary DESC, empno LIMIT 5 OFFSET 2;

SELECT empno, lastname, salary FROM sample.employee ORDER BY salary DESC, empno LIMIT 5 OFFSET 2;

--
-- END of TC011
--
