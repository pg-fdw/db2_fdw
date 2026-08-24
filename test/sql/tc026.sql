--
-- TC026: map subset/reordered foreign columns by remote name
--
-- EMPLOYEE contains MIDINIT between FIRSTNME and LASTNAME and WORKDEPT before
-- JOB. Mapping this local subset by ordinal gives LASTNAME the CHAR(1) buffer
-- of MIDINIT and JOB the CHAR(3) buffer of WORKDEPT, truncating both values.
--
CREATE FOREIGN TABLE sample.employee_subset (
  empno char(6),
  firstnme varchar(12),
  lastname varchar(15),
  job varchar(8)
)
SERVER sample
OPTIONS (schema 'DB2INST1', table 'EMPLOYEE');

SELECT empno, firstnme, lastname, job
FROM sample.employee_subset
WHERE empno IN ('000010', '000020', '000030')
ORDER BY empno;

DROP FOREIGN TABLE sample.employee_subset;

--
-- END of TC026
--
