#include "db2_fdw.h"

/** global variables  */

/** external variables */
extern int          silent;                /* emit no error messages when set, used for shutdown            */
extern int          sql_initialized;       /* set to "1" as soon as SQLAllocHandle(SQL_HANDLE_ENV is called */
extern DB2EnvEntry* rootenvEntry;          /* Linked list of handles for cached DB2 connections.            */
extern char         db2Message[ERRBUFSIZE];/* contains DB2 error messages, set by db2CheckErr()             */

/** external prototypes */
extern void      db2Error             (db2error sqlstate, const char* message);
extern void      db2Error_d           (db2error sqlstate, const char* message, const char* detail, ...);
extern SQLRETURN db2CheckErr          (SQLRETURN status, SQLHANDLE handle, SQLSMALLINT handleType, int line, char* file);
extern void      db2UnregisterCallback(void* arg);
extern void      db2FreeEnvHdl        (DB2EnvEntry* envp, const char* nls_lang);

/** local prototypes */
       void      db2CloseConnections  (void);
static void      db2FreeConnHdl       (DB2EnvEntry* envp, DB2ConnEntry* connp);
static int       deleteconnEntry      (DB2ConnEntry* start, DB2ConnEntry* node);

/* db2CloseConnections
 * Close everything in the cache.
 */
void db2CloseConnections (void) {
  db2Entry1();
  while (rootenvEntry != NULL) {
    while (rootenvEntry->connlist != NULL) {
      db2FreeConnHdl(rootenvEntry, rootenvEntry->connlist);
      db2Debug3("rootenvEntry: %x, rootenvEntry->connlist: %x",rootenvEntry, rootenvEntry->connlist);
    }
    db2FreeEnvHdl(rootenvEntry, NULL);
  }
  db2Exit1();
}

/* db2FreeConnHdl */
static void db2FreeConnHdl(DB2EnvEntry* envp, DB2ConnEntry* connp){
  SQLRETURN  rc     = 0;
  int        result = 0;

  db2Entry2();
  db2Debug3("envp : %x, ->henv: %d, ->connlist: %x",envp,envp->henv,envp->connlist);
  db2Debug3("connp: %x, ->hdbc: %d, ->handlelist: %x",connp,connp->hdbc,connp->handlelist);
  if (connp == NULL) {
    if (silent) return;
    else db2Error (FDW_ERROR, "closeSession internal error: connp is null");
  }

  /* terminate the session */
  db2Debug3("connp->hdbc: %x",connp->hdbc);
  rc = SQLDisconnect(connp->hdbc);
  db2Debug4("SQLDisconnect.rc: %d",rc);
  rc = db2CheckErr(rc, connp->hdbc,SQL_HANDLE_DBC,__LINE__,__FILE__);
  if (rc != SQL_SUCCESS && !silent) {
    db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error closing session: SQLDisconnect failed to terminate session", db2Message);
  }

  /* release the session handle */
  db2Debug3("connp->hdbc: %x",connp->hdbc);
  rc = SQLFreeHandle(SQL_HANDLE_DBC, connp->hdbc);
  db2Debug4("SQLFreeHandle.rc: %d",rc);
  if (rc != SQL_SUCCESS && !silent) {
    db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error freeing session handle: SQLFreeHandle failed", db2Message);
  }
  /* unregister callback for rolled back transactions */
  db2UnregisterCallback (connp);

  /* remove the session handle from the cache */
  result = deleteconnEntry(envp->connlist, connp);
  if (result && envp->connlist == connp) {
    envp->connlist = NULL;
    db2Debug4("envp->connlist: %x",envp->connlist);
  }

  db2Exit2();
}

/* deleteconnEntry */
int deleteconnEntry(DB2ConnEntry* start, DB2ConnEntry* node) {
  int           result = 0;
  DB2ConnEntry* step   = NULL;
  db2Entry2("(start:%x,node:%x)",start,node);

  for (step = start; step != NULL; step = step->right) {
    if (step == node) {
      db2Debug4("step == node: start: %x, step: %x, node %x", start, step, node);
      if (step->left == NULL && step->right == NULL){
        db2Debug4("step left and right is null: start: %x, step: %x",start,step);
      } else if (step->left == NULL) {
        db2Debug4("step left null");
        step->right->left = NULL;
      } else if (step->right == NULL) {
        db2Debug4("step right null");
        step->left->right = NULL;
      } else {
        step->left->right = step->right;
        step->right->left = step->left;
      }
      if (step->srvname)   free (step->srvname);
      if (step->uid)       free (step->uid);
      if (step->pwd)       free (step->pwd);
      if (step->jwt_token) free (step->jwt_token);
      if (step) {
        db2Debug4("DB2ConnEntry freed: %x", step);
        free (step);
      }
      result = 1;
      break;
    }
  }
  if (db2IsLogEnabled(DB2DEBUG3)) {
    for (step = start; step != NULL; step = step->right) {
      db2Debug3("start:%x, step:%x, step->left: %x, step->right:%x",start,step,step->left,step->right);
    }
  }
  db2Exit2(": %d", result);
  return result;
}
