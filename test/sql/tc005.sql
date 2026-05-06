--
-- TC005: run a pushdown query for aggregate functions
--
\d+ sample.employee;
explain (analyze,verbose) select min(salary),max(salary),avg(salary),sum(salary +comm + bonus),count(*) from sample.employee;
select min(salary),max(salary),avg(salary),sum(salary +comm + bonus),count(*) from sample.employee;
--
explain (analyze,verbose) select empno, firstnme,lastname, salary + bonus + comm from sample.employee where salary > 8000 and lastname like 'L%';
select empno, firstnme,lastname, salary + bonus + comm from sample.employee where salary > 8000 and lastname like 'L%';
--
-- END of TC004
--