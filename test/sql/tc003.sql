--
-- TC003: Run a simple join
--
\d+ sample.employee;
\d+ sample.sales;
-- test a simple join
select * from sample.employee a, sample.sales b where a.lastname = b.sales_person;
--
-- End of TC003
--