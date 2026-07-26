#include <string.h>
#include "db2_fdw.h"

/** global variables */
DB2EnvEntry*        rootenvEntry    = NULL;/* Cached handle for the (at most one) DB2 environment per backend. */
int                 sql_initialized = 0;   /* set to "1" as soon as SQLAllocHandle(SQL_HANDLE_ENV is called */

/** external variables */
extern char         db2Message[ERRBUFSIZE];/* contains DB2 error messages, set by db2CheckErr()             */

/** external prototypes */
extern void      db2SetHandlers       (void);
extern void      db2Error_d           (db2error sqlstate, const char* message, const char* detail, ...);
extern SQLRETURN db2CheckErr          (SQLRETURN status, SQLHANDLE handle, SQLSMALLINT handleType, int line, char* file);

/** local prototypes */
       DB2EnvEntry* db2AllocEnvHdl       (void);

/* db2AllocEnvHdl */
DB2EnvEntry* db2AllocEnvHdl(void){
  DB2EnvEntry*  envp    = NULL;
  SQLHENV       henv    = SQL_NULL_HENV;
  SQLRETURN     rc      = 0;

  db2Entry1();
  /* create environment handle */
  rc = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
  db2Debug3("allocate env handle - rc: %d, henv: %d",rc, henv);
  rc = db2CheckErr(rc, henv, SQL_HANDLE_ENV, __LINE__, __FILE__);
  if (rc != SQL_SUCCESS) {
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2: SQLAllocHandle failed to create environment handle", db2Message);
  }

  /* we can call db2Shutdown now */
  sql_initialized = 1;
  db2Debug3("sql_initialized: %d",sql_initialized);

  rc = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
  db2Debug3("set env attributes odbcv3 - rc: %d, henv: %d",rc, henv);
  rc = db2CheckErr(rc, henv, SQL_HANDLE_ENV, __LINE__, __FILE__);
  if (rc != SQL_SUCCESS) {
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2: SQLSetEnvAttr failed to set ODBC v3.0", db2Message);
  }

  /* DB2 overwrites PostgreSQL's signal handlers, so we have to restore them.
   * DB2's SIGINT handler is ok (it cancels the query), but we must do something reasonable for SIGTERM.
   */
  db2SetHandlers ();

  /* cache the (single) environment handle */
  envp           = malloc(sizeof(DB2EnvEntry));
  if (envp == NULL) {
    db2Error_d (FDW_OUT_OF_MEMORY, "error connecting to DB2:"," failed to allocate %zu bytes of memory", sizeof (DB2EnvEntry));
  }
  envp->henv     = henv;
  envp->connlist = NULL;
  rootenvEntry   = envp;

  db2Debug3("newEntry: %x ->henv: %d, ->connlist: %x",envp,envp->henv,envp->connlist);
  db2Exit1(": %x",envp);
  return envp;
}
