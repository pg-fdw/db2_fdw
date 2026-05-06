-- 
-- TC001: Dropping and re-creating a foreign table "manually"
--
-- drop an imported table
\d+ sample.org;
DROP FOREIGN TABLE IF EXISTS sample.org;
-- recreate it manually
\d+ sample.org;
CREATE FOREIGN TABLE sample.org (
                  DEPTNUMB SMALLINT OPTIONS (key 'yes') NOT NULL ,
                  DEPTNAME VARCHAR(14) ,
                  MANAGER SMALLINT ,
                  DIVISION VARCHAR(10) ,
                  LOCATION VARCHAR(13)
                   )
      SERVER sample OPTIONS (schema 'DB2INST1',table 'ORG');
\d+ sample.org;
-- 
-- TC001a: on a freshly created foreign table remove the content and manually re-create it again.
--
-- remove its content
delete from sample.org;
SELECT * FROM sample.org;
-- repopulate the content
insert into sample.org (DEPTNUMB,DEPTNAME,MANAGER,DIVISION,LOCATION) values(10,'Head Office',160,'Corporate','New York');
insert into sample.org (DEPTNUMB,DEPTNAME,MANAGER,DIVISION,LOCATION) values(15,'New England',50,'Eastern','Boston');
insert into sample.org (DEPTNUMB,DEPTNAME,MANAGER,DIVISION,LOCATION) values(20,'Mid Atlantic',10,'Eastern','Washington');
insert into sample.org (DEPTNUMB,DEPTNAME,MANAGER,DIVISION,LOCATION) values(38,'South Atlantic',30,'Eastern','Atlanta');
insert into sample.org (DEPTNUMB,DEPTNAME,MANAGER,DIVISION,LOCATION) values(42,'Great Lakes',100,'Midwest','Chicago');
insert into sample.org (DEPTNUMB,DEPTNAME,MANAGER,DIVISION,LOCATION) values(51,'Plains',140,'Midwest','Dallas');
insert into sample.org (DEPTNUMB,DEPTNAME,MANAGER,DIVISION,LOCATION) values(66,'Pacific',270,'Western','San Francisco');
insert into sample.org (DEPTNUMB,DEPTNAME,MANAGER,DIVISION,LOCATION) values(84,'Mountain',290,'Western','Denver');
-- inquire the content
SELECT * FROM sample.org;
--
-- END of TC001
--