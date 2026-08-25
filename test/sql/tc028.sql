--
-- TC028: keep base restrictions out of HAVING during aggregate pushdown
--
-- Recursive expression deparsing below a pushed-down aggregate must retain
-- the underlying scan relation.  Otherwise EMPNO is registered as a bogus
-- runtime parameter and setrefs.c cannot resolve it against count(*).
-- The grouped upper state must also keep the base restriction in WHERE only;
-- repeating EMPNO in HAVING makes DB2 reject the query with SQL0119N.
--
EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(*)
FROM sample.employee src
WHERE src.empno::text IN ('000010', '000020', '000030');

SELECT count(*) AS filtered_employee_rows
FROM sample.employee src
WHERE src.empno::text IN ('000010', '000020', '000030');

-- Exercise the complete upper-path chain as well: base WHERE, GROUP BY, a real
-- HAVING clause and ORDER BY must each retain their own SQL-clause position.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT src.workdept, count(*) AS employee_count
FROM sample.employee src
WHERE src.empno::text IN ('000010', '000020', '000030')
GROUP BY src.workdept
HAVING count(*) >= 1
ORDER BY src.workdept;

SELECT src.workdept, count(*) AS employee_count
FROM sample.employee src
WHERE src.empno::text IN ('000010', '000020', '000030')
GROUP BY src.workdept
HAVING count(*) >= 1
ORDER BY src.workdept;

--
-- END of TC028
--
