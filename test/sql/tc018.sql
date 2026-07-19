--
-- TC018: test translate() pushdown, including argument reordering
--
-- PostgreSQL's translate(string, from, to) and DB2's TRANSLATE(expression, to-string,
-- from-string) take the "from"/"to" arguments in opposite order; deparseFuncExpr() must
-- swap them so the pushed-down predicate keeps PostgreSQL semantics.
\d+ sample.employee;
explain (analyze,verbose) select empno, lastname from sample.employee where translate(lastname,'AEIOU','12345') = 'L5TZ' order by empno;
select empno, lastname from sample.employee where translate(lastname,'AEIOU','12345') = 'L5TZ' order by empno;
--
explain (analyze,verbose) select empno, lastname from sample.employee where translate(lastname,'AEIOU','12345') = translate('LUTZ','AEIOU','12345') order by empno;
select empno, lastname from sample.employee where translate(lastname,'AEIOU','12345') = translate('LUTZ','AEIOU','12345') order by empno;
--
-- END of TC018
--
