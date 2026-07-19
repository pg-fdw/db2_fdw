--
-- TC016: test between
--
\d+ sample.employee;
explain (analyze,verbose) select * from sample.employee where lastname between 'LUTZ' and 'LEE' order by lastname;
select * from sample.employee where lastname between 'LEE' and 'LUTZ' order by lastname;
--
explain select * from sample.employee where salary between 10000.00 and 60000.00 order by salary;
select * from sample.employee where salary between 10000.00 and 60000.00 order by salary;
--
-- END of TC016
--