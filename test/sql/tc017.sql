--
-- TC017: test multi-argument function pushdown (substr, replace, lpad, rpad, strpos, mod)
--
\d+ sample.employee;
explain (analyze,verbose) select empno, lastname from sample.employee where substr(lastname,1,1) = 'L' order by empno;
select empno, lastname from sample.employee where substr(lastname,1,1) = 'L' order by empno;
--
explain (analyze,verbose) select empno, lastname from sample.employee where substr(lastname,2,3) = 'UTZ' order by empno;
select empno, lastname from sample.employee where substr(lastname,2,3) = 'UTZ' order by empno;
--
explain (analyze,verbose) select empno, lastname from sample.employee where replace(lastname,'E','X') = 'LXX' order by empno;
select empno, lastname from sample.employee where replace(lastname,'E','X') = 'LXX' order by empno;
--
explain (analyze,verbose) select empno, lastname from sample.employee where lpad(lastname,10,'*') = '******LUTZ' order by empno;
select empno, lastname from sample.employee where lpad(lastname,10,'*') = '******LUTZ' order by empno;
--
explain (analyze,verbose) select empno, lastname from sample.employee where rpad(lastname,10,'*') = 'LUTZ******' order by empno;
select empno, lastname from sample.employee where rpad(lastname,10,'*') = 'LUTZ******' order by empno;
--
explain (analyze,verbose) select empno, lastname from sample.employee where strpos(lastname,'EE') = 2 order by empno;
select empno, lastname from sample.employee where strpos(lastname,'EE') = 2 order by empno;
--
explain (analyze,verbose) select empno, edlevel from sample.employee where mod(edlevel,2) = 0 order by empno;
select empno, edlevel from sample.employee where mod(edlevel,2) = 0 order by empno;
--
-- END of TC017
--
