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
-- list imported tables
\detr+ sample.*
--
select db2_diag();
-- starting testcases
-- running tc001.sql
\i tc001.sql
-- running tc002.sql
\i tc002.sql
-- running tc003.sql
\i tc003.sql
-- running tc004.sql
\i tc004.sql
-- running tc005.sql
\i tc005.sql
-- testcases ended
-- starting cleanup
\c postgres
DROP DATABASE regtest;
-- test finished