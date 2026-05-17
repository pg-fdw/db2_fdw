--
-- TC009: Close cached DB2 connections and verify reconnect works
--
SELECT count(*) FROM sample.employee;
SELECT db2_close_connections();
SELECT count(*) FROM sample.employee;

--
-- END of TC009
--
