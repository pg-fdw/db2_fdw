-- Install extension
CREATE EXTENSION IF NOT EXISTS db2_fdw;
select db2_diag();

-- Install FDW Server
CREATE SERVER IF NOT EXISTS sample FOREIGN DATA WRAPPER db2_fdw OPTIONS (dbserver 'SAMPLE');
-- Map a user
CREATE USER MAPPING FOR PUBLIC SERVER sample OPTIONS (user 'db2inst1', password 'db2inst1');
