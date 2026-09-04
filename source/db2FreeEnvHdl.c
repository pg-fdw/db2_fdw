#include <string.h>
#include "db2_fdw.h"

/** global variables  */

/** external variables */
extern int          sql_initialized;       /* set to "1" as soon as SQLAllocHandle(SQL_HANDLE_ENV is called */
extern int          silent;                /* emit no error messages when set, used for shutdown            */
extern char         db2Message[ERRBUFSIZE];/* contains DB2 error messages, set by db2CheckErr()             */
extern DB2EnvEntry* rootenvEntry;          /* Cached handle for the (at most one) DB2 environment per backend. */

/** external prototypes */
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

    /* Reset the cache before reporting any error: db2Error_d() throws via
     * ereport(ERROR), which would otherwise skip this cleanup and leave
     * rootenvEntry/sql_initialized pointing at a handle we already tried
     * (and failed) to free. That stale state made the next db2FreeEnvHdl()
     * call - including the one db2Shutdown() makes at backend exit - retry
     * SQLFreeHandle() on the same handle and fail again; when that retry
     * happens during proc_exit, PostgreSQL forces the resulting ERROR up to
     * FATAL, killing the backend abnormally instead of exiting cleanly. */
    db2Debug3("DB2Enventry freed: %x", envp);
    free (envp);
    rootenvEntry = NULL;
    sql_initialized = 0;
    db2Debug3("sql_initialized: %d",sql_initialized);

    if (rc != SQL_SUCCESS && !silent) {
      db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "cannot release environment handle",db2Message);
    }
  }
  db2Exit1();
}
