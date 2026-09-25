#include "db2_fdw.h"

/** global variables */

/** external variables */
extern char      db2Message[ERRBUFSIZE];/* contains DB2 error messages, set by db2CheckErr()             */

/** external prototypes */
extern void      db2FreeStmtHdl       (HdlEntry* handlep, DB2ConnEntry* connp);
extern SQLRETURN db2CheckErr          (SQLRETURN status, SQLHANDLE handle, SQLSMALLINT handleType, int line, char* file);

/** local prototypes */
void db2CloseStatement (DB2Session* session);
void db2CloseCursor    (DB2Session* session);

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

/* db2CloseCursor
 * Close the open cursor of the session's statement, but keep the statement prepared and its result columns bound,
 * so that it can be executed again (used for DML with a RETURNING clause, which runs as SELECT ... FROM NEW/OLD TABLE).
 */
void db2CloseCursor (DB2Session* session) {
  SQLRETURN rc = 0;
  db2Entry1();
  if (session->stmtp != NULL) {
    rc = SQLFreeStmt(session->stmtp->hsql, SQL_CLOSE);
    rc = db2CheckErr(rc, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
    if (rc != SQL_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: SQLFreeStmt failed to close cursor", db2Message);
    }
  } else {
    db2Debug3("no handle to close");
  }
  db2Exit1();
}
