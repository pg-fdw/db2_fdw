#include <stdio.h>
#include "db2_fdw.h"

/** global variables */

/** external variables */
extern char          db2Message[ERRBUFSIZE];/* contains DB2 error messages, set by db2CheckErr()             */

/** external prototypes */
extern void          db2Error_d           (db2error sqlstate, const char* message, const char* detail, ...);
extern SQLRETURN     db2CheckErr          (SQLRETURN status, SQLHANDLE handle, SQLSMALLINT handleType, int line, char* file);
extern HdlEntry*     db2AllocStmtHdl      (SQLSMALLINT type, DB2ConnEntry* connp, db2error error, const char* errmsg);
extern void          db2FreeStmtHdl       (HdlEntry* handlep, DB2ConnEntry* connp);

/** local prototypes */
void                 db2SetSavepoint      (DB2Session* session, int nest_level);

/* db2SetSavepoint
 * Set savepoints up to level "nest_level".
 */
void db2SetSavepoint (DB2Session* session, int nest_level) {
  SQLRETURN rc    = 0;
  HdlEntry* hstmt = NULL;

  db2Entry1("(session, nest_level %d)",nest_level);
  db2Debug2("xact_level: %d",session->connp->xact_level);
  while (session->connp->xact_level < nest_level) {
    SQLCHAR query[80];

    db2Debug2("db2_fdw::db2SetSavepoint: set savepoint s%d", session->connp->xact_level + 1);
    snprintf((char*)query, 79, "SAVEPOINT s%d ON ROLLBACK RETAIN CURSORS", session->connp->xact_level + 1);
    db2Debug2("query: '%s'",query);

    /* create statement handle */
    hstmt = db2AllocStmtHdl(SQL_HANDLE_STMT, session->connp, FDW_UNABLE_TO_CREATE_EXECUTION, "error setting savepoint: failed to allocate statement handle");

    /* prepare the query */
    rc = SQLPrepare(hstmt->hsql, (SQLCHAR*)query, SQL_NTS);
    rc = db2CheckErr(rc,hstmt->hsql, hstmt->type, __LINE__, __FILE__); 
    if (rc != SQL_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error setting savepoint: SQLPrepare failed to prepare savepoint statement", db2Message);
    }

    /* set savepoint */
    rc = SQLExecute(hstmt->hsql);
    rc = db2CheckErr(rc, hstmt->hsql, hstmt->type, __LINE__, __FILE__);
    if (rc  != SQL_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error setting savepoint: SQLExecute failed to set savepoint", db2Message);
    }

    /* release statement handle */
    db2FreeStmtHdl(hstmt, session->connp);
    ++session->connp->xact_level;
  }
  db2Debug2("xact_level: %d",session->connp->xact_level);
  db2Exit1();
}
