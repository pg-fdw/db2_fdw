--
-- TC003: Run a simple join
--
\d+ sample.employee;
select * from sample.employee;
--
\d+ sample.sales;
select * from sample.sales;
-- test a simple join
explain (analyze,verbose) select * from sample.employee a, sample.sales b where a.lastname = b.sales_person;
select * from sample.employee a, sample.sales b where a.lastname = b.sales_person;
--
explain (analyze,verbose) select * from sample.employee a join sample.sales b on a.lastname = b.sales_person;
select * from sample.employee a join sample.sales b on a.lastname = b.sales_person;
--
-- End of TC003
--