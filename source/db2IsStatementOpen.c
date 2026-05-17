#include "db2_fdw.h"

/** global variables */

/** external variables */

/** external prototypes */

/** local prototypes */
int                 db2IsStatementOpen   (DB2Session* session);

/* db2IsStatementOpen
 * Return 1 if there is a statement handle, else 0.
 */
int db2IsStatementOpen (DB2Session* session) {
  int result = 0;
  db2Entry1();
  result = (session->stmtp != NULL && session->stmtp->hsql != SQL_NULL_HSTMT);
  db2Exit1(": %d",result);
  return result;
}
