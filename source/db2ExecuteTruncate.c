#include <string.h>
#include "db2_fdw.h"
#include "ParamDesc.h"

/** global variables */

/** external variables */
extern char         db2Message[ERRBUFSIZE];/* contains DB2 error messages, set by db2CheckErr()             */
extern int          err_code;              /* error code, set by db2CheckErr()                              */

/** external prototypes */
extern SQLRETURN    db2CheckErr          (SQLRETURN status, SQLHANDLE handle, SQLSMALLINT handleType, int line, char* file);
extern void         db2Error_d           (db2error sqlstate, const char* message, const char* detail, ...);
extern HdlEntry*    db2AllocStmtHdl      (SQLSMALLINT type, DB2ConnEntry* connp, db2error error, const char* errmsg);

/** internal prototypes */
int                 db2ExecuteTruncate   (DB2Session* session, const char* query);

/* db2ExecuteTruncate */
int db2ExecuteTruncate   (DB2Session* session, const char* query) {
  SQLRETURN   rc           = 0;
  SQLINTEGER  rowcount_val = 0;
  int         rowcount     = 0;
  
  db2Entry1("(DB2Session: %x, query: %s)",session,query);

  rc = SQLEndTran(SQL_HANDLE_DBC, session->connp->hdbc, SQL_COMMIT);
  rc = db2CheckErr(rc, session->connp->hdbc, SQL_HANDLE_DBC, __LINE__, __FILE__);
  if (rc  != SQL_SUCCESS) {
    db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error committing transaction: SQLEndTran failed", db2Message);
  }
  session->stmtp = db2AllocStmtHdl(SQL_HANDLE_STMT, session->connp, FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: failed to allocate statement handle");
  rc = SQLExecDirect(session->stmtp->hsql, (SQLCHAR*) query, SQL_NTS);
  rc = db2CheckErr(rc, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
  if (rc != SQL_SUCCESS && rc != SQL_NO_DATA) {
    /* use the correct SQLSTATE for serialization failures */
    db2Error_d(err_code == 8177 ? FDW_SERIALIZATION_FAILURE : FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: SQLExecute failed to execute remote query", db2Message);
  }

  db2Debug2("rowcount_val: %lld", rowcount_val);
  rowcount = (int) rowcount_val;
  db2Exit1(": %d",rowcount);
  return rowcount;
}
