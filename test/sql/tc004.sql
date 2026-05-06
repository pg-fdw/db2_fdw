--
-- TC004: clone a foreign table into a local table including content
--
create table sample.orgcopy as select * from sample.org;
\d+ sample.org*
drop table sample.orgcopy;
--
-- END of TC004
--