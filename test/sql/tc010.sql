--
-- TC010: Pushdown predicates (IN/BETWEEN/OR/IS NULL)
--
EXPLAIN (analyze,verbose) SELECT empno, lastname, salary FROM sample.employee WHERE workdept IN ('A00','B01','E21') AND salary BETWEEN 40000 AND 70000 AND (lastname LIKE 'L%' OR lastname LIKE 'G%');

SELECT empno, lastname, salary FROM sample.employee WHERE workdept IN ('A00','B01','E21') AND salary BETWEEN 40000 AND 70000 AND (lastname LIKE 'L%' OR lastname LIKE 'G%') ORDER BY empno;

-- NULL handling sanity check
SELECT
  count(*) AS total,
  count(*) FILTER (WHERE comm IS NULL) AS comm_is_null,
  count(*) FILTER (WHERE comm IS NOT NULL) AS comm_is_not_null
FROM sample.employee;

--
-- END of TC010
--
