#include "db2_fdw.h"

/** external variables */
extern char         db2Message[ERRBUFSIZE];/* contains DB2 error messages, set by db2CheckErr()             */
extern DB2EnvEntry* rootenvEntry;          /* Linked list of handles for cached DB2 connections.            */

/** external prototypes */
extern void      db2Error             (db2error sqlstate, const char* message);
extern void      db2Error_d           (db2error sqlstate, const char* message, const char* detail, ...);
extern SQLRETURN db2CheckErr          (SQLRETURN status, SQLHANDLE handle, SQLSMALLINT handleType, int line, char* file);
extern void      db2FreeStmtHdl       (HdlEntry* handlep, DB2ConnEntry* connp);

/** local prototypes */
void             db2EndTransaction    (void* arg, int is_commit, int noerror);

/* db2EndTransaction
 * Commit or rollback the transaction.
 * The first argument must be a connEntry.
 * If "noerror" is true, don't throw errors.
 */
void db2EndTransaction (void* arg, int is_commit, int noerror) {
  DB2ConnEntry* connp = NULL;
  DB2EnvEntry*  envp  = NULL;
  int           found = 0;
  SQLRETURN     rc    = 0;

  db2Entry1("(arg:%x, is_commit:%d, noerror:%d)",arg,is_commit,noerror);
  /* do nothing if there is no transaction */
  if (((DB2ConnEntry*) arg)->xact_level == 0) {
    db2Debug2("there is no transaction");
    db2Debug2("((DB2ConnEntry*) arg)->xact_level: %d",((DB2ConnEntry*) arg)->xact_level);
  } else {
    /* find the cached handles for the argument */
    envp = rootenvEntry;
    for (envp = rootenvEntry; envp != NULL; envp = envp->right) {
      for (connp = envp->connlist; connp != NULL; connp = connp->right ){
        if (connp == (DB2ConnEntry *) arg) {
          found = 1;
          break;
        }
      }
    }
    if (!found)
      /* print this trace hint, since the code will abend due to connp = NULL*/
      db2Error (FDW_ERROR, "db2EndTransaction internal error: handle not found in cache");

    /* release all handles of this connection, if any*/
    while (connp->handlelist != NULL)
      db2FreeStmtHdl(connp->handlelist, connp);

    /* commit or rollback */
    if (is_commit) {
      db2Debug2("db2_fdw::db2EndTransaction: commit remote transaction");
      rc = SQLEndTran(SQL_HANDLE_DBC, connp->hdbc, SQL_COMMIT);
      rc = db2CheckErr(rc, connp->hdbc, SQL_HANDLE_DBC, __LINE__, __FILE__);
      if (rc  != SQL_SUCCESS && !noerror) {
        db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error committing transaction: SQLEndTran failed", db2Message);
      }
    } else {
      db2Debug2("db2_fdw::db2EndTransaction: roll back remote transaction");
      rc = SQLEndTran(SQL_HANDLE_DBC, connp->hdbc, SQL_ROLLBACK);
      rc = db2CheckErr(rc, connp->hdbc, SQL_HANDLE_DBC, __LINE__, __FILE__);
      if (rc != SQL_SUCCESS && !noerror) {
        db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error rolling back transaction: SQLEndTran failed", db2Message);
      }
    }
    connp->xact_level = 0;
    db2Debug2("connp->xact_level: %d",connp->xact_level);
  }
  db2Exit1();
}
