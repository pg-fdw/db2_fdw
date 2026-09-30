--
-- TC033: validation of the IMPORT FOREIGN SCHEMA options readonly and importtype
--
-- Both options used to accept any value: readonly silently became false, and
-- importtype was pasted into the DB2 catalog query, so a lower case 't' found
-- nothing (SYSCAT.TABLES.TYPE is upper case) and other values changed the query.
--
-- Uses the DB2 schema FDWUNIQ from tc032.sql (tables ORDERS and "Items", no views).
--
CREATE SCHEMA tc033;
-- invalid values are rejected
IMPORT FOREIGN SCHEMA "FDWUNIQ" FROM SERVER sample INTO tc033 OPTIONS (readonly 'maybe');
IMPORT FOREIGN SCHEMA "FDWUNIQ" FROM SERVER sample INTO tc033 OPTIONS (importtype 'X');
IMPORT FOREIGN SCHEMA "FDWUNIQ" FROM SERVER sample INTO tc033 OPTIONS (importtype 'T'') OR (''1''=''1');
IMPORT FOREIGN SCHEMA "FDWUNIQ" FROM SERVER sample INTO tc033 OPTIONS (case 'KEEP');
SELECT count(*) AS imported FROM pg_foreign_table ft JOIN pg_class c ON c.oid = ft.ftrelid
WHERE c.relnamespace = 'tc033'::regnamespace;
-- valid values are case insensitive: 'v' finds no views, 't' finds both tables
IMPORT FOREIGN SCHEMA "FDWUNIQ" FROM SERVER sample INTO tc033 OPTIONS (importtype 'v');
SELECT count(*) AS imported FROM pg_foreign_table ft JOIN pg_class c ON c.oid = ft.ftrelid
WHERE c.relnamespace = 'tc033'::regnamespace;
IMPORT FOREIGN SCHEMA "FDWUNIQ" FROM SERVER sample INTO tc033 OPTIONS (importtype 't', readonly 'YES');
SELECT c.relname, array_to_string(ft.ftoptions, ', ') AS options
FROM pg_foreign_table ft JOIN pg_class c ON c.oid = ft.ftrelid
WHERE c.relnamespace = 'tc033'::regnamespace ORDER BY c.relname COLLATE "C";
-- readonly is enforced
INSERT INTO tc033.orders VALUES (2, 'not allowed');
DROP SCHEMA tc033 CASCADE;
--
-- END of TC033
--
