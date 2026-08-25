--
-- TC024: keep set operations and operations above them local
--
-- Set-operation pushdown is not implemented.  These queries mix a DB2 foreign
-- table with a local table and must therefore never create a foreign upper path
-- for UNION, DISTINCT, aggregation or ORDER BY.
--
CREATE TEMP TABLE tc024_local_employee (
  empno varchar(6)
);

INSERT INTO tc024_local_employee (empno) VALUES ('000010'), ('LOCAL1');

EXPLAIN (VERBOSE)
SELECT DISTINCT empno
FROM (
  SELECT empno FROM sample.employee
  UNION ALL
  SELECT empno FROM tc024_local_employee
) AS employee_scope
ORDER BY empno;

-- DISTINCT and window processing are local too; a following ORDER BY must not
-- revive an unsupported foreign upper path.
EXPLAIN (VERBOSE)
SELECT DISTINCT empno
FROM sample.employee
ORDER BY empno;

EXPLAIN (VERBOSE)
SELECT empno, row_number() OVER (ORDER BY empno) AS row_number
FROM sample.employee
ORDER BY empno;

SELECT DISTINCT empno
FROM (
  SELECT empno FROM sample.employee WHERE empno = '000010'
  UNION ALL
  SELECT empno FROM tc024_local_employee
) AS employee_scope
ORDER BY empno;

CREATE TEMP TABLE tc024_scope (
  empno varchar(6)
);

INSERT INTO tc024_scope (empno) VALUES ('000010');

EXPLAIN (VERBOSE)
SELECT source_name, row_count
FROM (
  SELECT 'DB2'::text AS source_name, count(*) AS row_count
  FROM sample.employee
  WHERE empno = ANY (ARRAY(SELECT empno FROM tc024_scope))
  UNION ALL
  SELECT 'LOCAL'::text, count(*)
  FROM tc024_local_employee
) AS source_counts
ORDER BY source_name;

SELECT source_name, row_count
FROM (
  SELECT 'DB2'::text AS source_name, count(*) AS row_count
  FROM sample.employee
  WHERE empno = ANY (ARRAY(SELECT empno FROM tc024_scope))
  UNION ALL
  SELECT 'LOCAL'::text, count(*)
  FROM tc024_local_employee
) AS source_counts
ORDER BY source_name;

-- deparseExpr() knows how to print COALESCE, but COALESCE is deliberately not
-- accepted by is_foreign_expr().  A DISTINCT path over this CASE expression
-- must therefore use a local sort instead of advertising a remote path that
-- appendOrderByClause() cannot reproduce later.
EXPLAIN (VERBOSE)
SELECT DISTINCT
       workdept,
       CASE WHEN COALESCE(edlevel, 0) = 1 THEN salary ELSE bonus END AS amount
FROM sample.employee;

CREATE TEMP TABLE tc024_distinct_case AS
SELECT DISTINCT
       workdept,
       CASE WHEN COALESCE(edlevel, 0) = 1 THEN salary ELSE bonus END AS amount
FROM sample.employee;

SELECT count(*) AS distinct_case_rows
FROM tc024_distinct_case;

-- A pg_catalog function is not automatically available with identical
-- syntax and semantics in DB2.  btrim() is intentionally evaluated locally,
-- as is the comparison with PostgreSQL's empty string (DB2 treats it as
-- NULL).  The non-NULL predicate may still be pushed down independently.
EXPLAIN (VERBOSE)
SELECT empno
FROM sample.employee
WHERE firstnme IS NOT NULL
  AND btrim(firstnme) <> '';

CREATE TEMP TABLE tc024_nonempty_names AS
SELECT empno
FROM sample.employee
WHERE firstnme IS NOT NULL
  AND btrim(firstnme) <> '';

SELECT count(*) AS nonempty_name_rows
FROM tc024_nonempty_names;

--
-- END of TC024
--
