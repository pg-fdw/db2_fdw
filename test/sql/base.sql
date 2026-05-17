\set ECHO all
\pset null '[NULL]'

\i ./test/sql/tccdb.sql
\i ./test/sql/tcfdw.sql
\i ./test/sql/tcstart.sql

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
--running tc011.sql
\i ./test/sql/tc011.sql
-- running tc012.sql
\i ./test/sql/tc012.sql
-- running tc013.sql
\i ./test/sql/tc013.sql
-- running tc014.sql
\i ./test/sql/tc014.sql
-- testcases ended
-- starting cleanup
\i ./test/sql/tcend.sql
-- test finished