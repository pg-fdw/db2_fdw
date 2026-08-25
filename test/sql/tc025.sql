--
-- TC025: result-buffer and tuple-slot safety
--
-- A broad multi-row projection with mixed character, date and numeric columns
-- exercises bound columns, the ABI-neutral SQLLEN indicator storage and the
-- deliberately unbound numeric SQLGetData columns in one result. Deliberately
-- use an order different from the DB2 table definition and force PostgreSQL to
-- retain/sort all returned virtual tuples.
--
CREATE TEMP TABLE tc025_employee_copy AS
SELECT empno, hiredate, salary, firstnme, midinit, lastname, workdept,
       phoneno, job, edlevel, sex, birthdate, bonus, comm
FROM sample.employee
ORDER BY empno;

SELECT count(*) AS copied_rows
FROM tc025_employee_copy;

--
-- END of TC025
--