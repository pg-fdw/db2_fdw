--
-- TC027: preserve ForeignScan output Vars for parameterized LATERAL paths
--
-- A LATERAL VALUES relation can make the selected path's output target list
-- differ from the base relation's RelOptInfo target.  db2_fdw uses a custom
-- fdw_scan_tlist, so it must include every base Var from GetForeignPlan's
-- actual target list.  Otherwise PostgreSQL's setrefs.c aborts planning with
-- "variable not found in subplan target list".
--
CREATE TEMP TABLE tc027_target (
  empno varchar(6),
  enabled boolean
);

CREATE TEMP TABLE tc027_mapping (
  source_expression text,
  target_code text,
  enabled boolean
);

INSERT INTO tc027_target VALUES ('000010', true);
INSERT INTO tc027_mapping VALUES
  ('salary', 'salary', true),
  ('bonus',  'bonus',  true),
  ('comm',   'comm',   true);

ANALYZE tc027_target;
ANALYZE tc027_mapping;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT src.empno,
       value_row.source_expression,
       value_row.amount,
       value_row.department_present,
       value_row.job_present
FROM sample.employee src
JOIN tc027_target target
  ON target.empno = src.empno
 AND target.enabled
CROSS JOIN LATERAL (VALUES
  ('salary', 'salary', src.salary::numeric,
   src.workdept IS NOT NULL, src.job IS NOT NULL),
  ('bonus', 'bonus', src.bonus::numeric,
   src.workdept IS NOT NULL, src.job IS NOT NULL),
  ('comm', 'comm', src.comm::numeric,
   src.workdept IS NOT NULL, src.job IS NOT NULL)
) AS value_row(source_expression, target_code, amount,
               department_present, job_present)
JOIN tc027_mapping mapping
  ON mapping.source_expression = value_row.source_expression
 AND mapping.target_code = value_row.target_code
 AND mapping.enabled
WHERE value_row.amount IS NOT NULL
   OR value_row.department_present
   OR value_row.job_present;

CREATE TEMP TABLE tc027_result AS
SELECT src.empno,
       value_row.source_expression,
       value_row.amount,
       value_row.department_present,
       value_row.job_present
FROM sample.employee src
JOIN tc027_target target
  ON target.empno = src.empno
 AND target.enabled
CROSS JOIN LATERAL (VALUES
  ('salary', 'salary', src.salary::numeric,
   src.workdept IS NOT NULL, src.job IS NOT NULL),
  ('bonus', 'bonus', src.bonus::numeric,
   src.workdept IS NOT NULL, src.job IS NOT NULL),
  ('comm', 'comm', src.comm::numeric,
   src.workdept IS NOT NULL, src.job IS NOT NULL)
) AS value_row(source_expression, target_code, amount,
               department_present, job_present)
JOIN tc027_mapping mapping
  ON mapping.source_expression = value_row.source_expression
 AND mapping.target_code = value_row.target_code
 AND mapping.enabled
WHERE value_row.amount IS NOT NULL
   OR value_row.department_present
   OR value_row.job_present;

SELECT count(*) AS lateral_rows
FROM tc027_result;

--
-- END of TC027
--
