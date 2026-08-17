-- 
-- TC001: Dropping and re-creating a foreign table "manually"
--
-- drop an imported table
\d+ sample.org;
DROP FOREIGN TABLE IF EXISTS sample.org;
-- recreate it manually
\d+ sample.org;
--
CREATE FOREIGN TABLE sample.org (
                  DEPTNUMB SMALLINT    OPTIONS (db2type '5' , db2size '5' , db2bytes '2' , db2chars '5', db2scale '0', db2null '0', db2ccsid '0' ,key 'yes') NOT NULL ,
                  DEPTNAME VARCHAR(14) OPTIONS (db2type '12', db2size '14', db2bytes '14', db2chars '0', db2scale '0', db2null '1', db2ccsid '1208'        )          ,
                  MANAGER SMALLINT     OPTIONS (db2type '5' , db2size '5' , db2bytes '2' , db2chars '5', db2scale '0', db2null '1', db2ccsid '0'           )          ,
                  DIVISION VARCHAR(10) OPTIONS (db2type '12', db2size '10', db2bytes '10', db2chars '0', db2scale '0', db2null '1', db2ccsid '1208'        )          ,
                  LOCATION VARCHAR(13) OPTIONS (db2type '12', db2size '13', db2bytes '13', db2chars '0', db2scale '0', db2null '1', db2ccsid '1208'        )
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