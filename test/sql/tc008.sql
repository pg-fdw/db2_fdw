--
-- TC008: Query-based foreign table
--
DROP FOREIGN TABLE IF EXISTS sample.emp_l;

CREATE FOREIGN TABLE sample.emp_l (
  empno    char(6),
  firstnme varchar(12),
  lastname varchar(15)
)
SERVER sample 
OPTIONS (schema 'DB2INST1',table 'EMPLOYEE', readonly 'true');

\d+ sample.emp_l;
SELECT * FROM sample.emp_l ORDER BY empno;

-- Optional: show it is read-only by intent (uncomment if you want a hard failure case)
INSERT INTO sample.emp_l(empno, firstnme, lastname) VALUES ('999999', 'X', 'Y');

DROP FOREIGN TABLE sample.emp_l;

--
-- END of TC008
--
