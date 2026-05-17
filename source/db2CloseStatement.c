#include "db2_fdw.h"

/** global variables */

/** external variables */

/** external prototypes */
extern void      db2FreeStmtHdl       (HdlEntry* handlep, DB2ConnEntry* connp);

/** local prototypes */
void db2CloseStatement (DB2Session* session);

/* db2CloseStatement
 * Close any open statement associated with the session.
 */
void db2CloseStatement (DB2Session* session) {
  db2Entry1();
  /* release statement handle, if it exists */
  if (session->stmtp != NULL) {
    /* release the statement handle */
    db2FreeStmtHdl(session->stmtp, session->connp);
    session->stmtp = NULL;
  } else {
    db2Debug3("no handle to close");
  }
  db2Exit1();
}
