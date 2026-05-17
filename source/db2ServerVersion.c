#include <string.h>
#include "db2_fdw.h"

/** global variables  */

/** external variables */

/** external prototypes */
extern SQLRETURN db2CheckErr          (SQLRETURN status, SQLHANDLE handle, SQLSMALLINT handleType, int line, char* file);

/** local prototypes */
void             db2ServerVersion     (DB2Session* session, char* version);

/* db2ServerVersion
 * Returns the five components of the server version.
 */
void db2ServerVersion (DB2Session* session, char* version) {
  SQLSMALLINT len = 0;
  size_t      ver_len = sizeof(version);
  SQLRETURN   rc  = 0;
  db2Entry1();
  memset(version,0x00,ver_len);
  rc = SQLGetInfo(session->connp->hdbc, SQL_DBMS_VER, version, sizeof(version), &len);
  db2Debug2("rc = %d, version = '%s', ind = %d", rc, version, len);
  rc = db2CheckErr(rc,session->connp->hdbc,SQL_HANDLE_DBC,__LINE__,__FILE__);
  db2Exit1(": '%s'", version);
}
