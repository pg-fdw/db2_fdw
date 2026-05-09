\set ECHO all
CREATE DATABASE regtest;
GRANT ALL PRIVILEGES ON DATABASE regtest to postgres;
\c regtest
-- Install extension
CREATE EXTENSION IF NOT EXISTS db2_fdw;
-- Install FDW Server
CREATE SERVER IF NOT EXISTS sample FOREIGN DATA WRAPPER db2_fdw OPTIONS (dbserver 'SAMPLE');
-- Map a user
CREATE USER MAPPING FOR PUBLIC SERVER sample OPTIONS (user 'db2inst1', password 'db2inst1');
-- CREATE USER MAPPING FOR PUBLIC SERVER sample OPTIONS (user '', password '');
-- Prepare a local schema
CREATE SCHEMA IF NOT EXISTS sample;
-- Import the complete sample db into the local schema
IMPORT FOREIGN SCHEMA "DB2INST1" FROM SERVER sample INTO sample;

-- For UPDATE/DELETE, db2_fdw requires at least one primary key column marked
-- with the column option "key".
-- The DB2 SAMPLE.ORG table uses DEPTNUMB as its primary key.
ALTER FOREIGN TABLE sample.org
  ALTER COLUMN deptnumb OPTIONS (ADD key 'true');

-- list imported tables
\detr+ sample.*
--
select db2_diag();
set log_min_messages=debug5;

-- starting testcases
-- running tc001.sql
\i ./test/sql/tc001.sql
-- running tc002.sql
\i ./test/sql/tc002.sql
-- running tc003.sql
\i ./test/sql/tc003.sql
-- running tc004.sql
\i ./test/sql/tc004.sql
-- running tc005.sql
\i ./test/sql/tc005.sql
-- running tc006.sql
\i ./test/sql/tc006.sql
-- running tc007.sql
\i ./test/sql/tc007.sql
-- running tc008.sql
\i ./test/sql/tc008.sql
-- running tc009.sql
\i ./test/sql/tc009.sql
-- running tc010.sql
\i ./test/sql/tc010.sql
-- running tc011.sql
--\i ./test/sql/tc011.sql
-- running tc012.sql
\i ./test/sql/tc012.sql
-- testcases ended
-- starting cleanup
\c postgres
DROP DATABASE regtest;
-- test finished