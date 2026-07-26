#include <string.h>
#include "db2_fdw.h"

/** global variables  */

/** external variables */
extern int          sql_initialized;       /* set to "1" as soon as SQLAllocHandle(SQL_HANDLE_ENV is called */
extern int          silent;                /* emit no error messages when set, used for shutdown            */
extern char         db2Message[ERRBUFSIZE];/* contains DB2 error messages, set by db2CheckErr()             */
extern DB2EnvEntry* rootenvEntry;          /* Cached handle for the (at most one) DB2 environment per backend. */

/** external prototypes */
extern void      db2Error             (db2error sqlstate, const char* message);
extern void      db2Error_d           (db2error sqlstate, const char* message, const char* detail, ...);
extern SQLRETURN db2CheckErr          (SQLRETURN status, SQLHANDLE handle, SQLSMALLINT handleType, int line, char* file);

/** local prototypes */
       void             db2FreeEnvHdl        (DB2EnvEntry* envp);

/* db2FreeEnvHdl */
void db2FreeEnvHdl(DB2EnvEntry* envp){
  SQLRETURN rc = 0;

  db2Entry1();
  if (envp == NULL || envp != rootenvEntry) {
    db2Debug3("removeEnvironment internal error: environment handle not found in cache");
    if (!silent) {
      db2Error (FDW_ERROR, "removeEnvironment internal error: environment handle not found in cache");
    }
  } else {
    /* release environment handle */
    rc = SQLFreeHandle(SQL_HANDLE_ENV, envp->henv);
    db2Debug3("release env handle - rc: %d, henv: %d", rc, envp->henv);
    rc = db2CheckErr(rc, envp->henv, SQL_HANDLE_ENV,__LINE__, __FILE__);
    if (rc != SQL_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "cannot release environment handle","%s", db2Message);
    }
    db2Debug3("DB2Enventry freed: %x", envp);
    free (envp);
    rootenvEntry = NULL;
    sql_initialized = 0;
    db2Debug3("sql_initialized: %d",sql_initialized);
  }
  db2Exit1();
}
