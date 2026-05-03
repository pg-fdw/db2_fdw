/*
 * Author: The maintainer's name
 * Created at: 2025-10-27 13:34:00 +0100
 *
 */

--
-- This is a example code genereted automaticaly
-- by pgxn-utils.

SET client_min_messages = warning;

BEGIN;

-- You can use this statements as
-- template for your extension.

DROP FUNCTION db2_fdw_handler();
DROP FUNCTION db2_fdw_validator(text[], oid);
DROP FUNCTION db2_close_connections();
DROP FUNCTION db2_diag(name DEFAULT NULL);
DROP EXTENSION db2_fdw CASCADE;
COMMIT;
