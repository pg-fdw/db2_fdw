--
-- TC022: CLOB/DBCLOB Unicode round-trip correctness (CODEUNITS16/32, UTF-16)
--
--
\d+ sample.dataxml;
--
select * from sample.dataxml;
--
--
\d+ sample1.dataxml;
--
select * from sample1.dataxml;
--
--
CREATE FOREIGN TABLE sample1.dataxml1 (
    xmlid          bigint                         OPTIONS (db2type '-5' , db2size '19'       , db2bytes '8'        , db2chars '19', db2scale '0', db2null '0', db2ccsid '0'   ) NOT NULL ,
    dataxml        bytea                          OPTIONS (db2type '-99', db2size '104857600', db2bytes '104857600', db2chars '0' , db2scale '0', db2null '1', db2ccsid '1208')          ,
    lastupdated    timestamp(6) without time zone OPTIONS (db2type '93' , db2size '26'       , db2bytes '16'       , db2chars '26', db2scale '6', db2null '0', db2ccsid '0'   ) NOT NULL 
)
SERVER sample1
OPTIONS (schema 'DB2INST1', "table" 'DATAXML');
--
select xmlid, convert_from(dataxml, 'UTF8'), dataxml, lastupdated from sample1.dataxml1;
--
-- END of TC022
--
