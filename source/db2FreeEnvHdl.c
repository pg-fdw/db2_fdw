#include <string.h>
#include "db2_fdw.h"

/** global variables  */

/** external variables */
extern int          sql_initialized;       /* set to "1" as soon as SQLAllocHandle(SQL_HANDLE_ENV is called */
extern int          silent;                /* emit no error messages when set, used for shutdown            */
extern char         db2Message[ERRBUFSIZE];/* contains DB2 error messages, set by db2CheckErr()             */
extern DB2EnvEntry* rootenvEntry;          /* Linked list of handles for cached DB2 connections.            */

/** external prototypes */
extern void      db2Error             (db2error sqlstate, const char* message);
extern void      db2Error_d           (db2error sqlstate, const char* message, const char* detail, ...);
extern SQLRETURN db2CheckErr          (SQLRETURN status, SQLHANDLE handle, SQLSMALLINT handleType, int line, char* file);

/** local prototypes */
       void             db2FreeEnvHdl        (DB2EnvEntry* envp, const char* nls_lang);
static int              deleteenvEntry       (DB2EnvEntry* start, DB2EnvEntry* node);
static int              deleteenvEntryLang   (DB2EnvEntry* start, const char* nlslang);
static DB2EnvEntry*     findenvEntryHandle   (DB2EnvEntry* start, SQLHENV henv);
       DB2EnvEntry*     findenvEntry         (DB2EnvEntry* start, const char* nlslang);

/* db2FreeEnvHdl */
void db2FreeEnvHdl(DB2EnvEntry* envp, const char* nls_lang){
  SQLRETURN rc = 0;

  db2Entry1();
  /* search environment handle in cache */
  envp = findenvEntryHandle (rootenvEntry, envp->henv);

  if (envp == NULL) {
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
    if (nls_lang != NULL){
      deleteenvEntryLang(rootenvEntry, nls_lang);
    }
    deleteenvEntry(rootenvEntry,envp);
    sql_initialized = 0;
    db2Debug3("sql_initialized: %d",sql_initialized);
  }
  db2Exit1();
}

/* deleteenvEntry */
static int deleteenvEntry(DB2EnvEntry* start, DB2EnvEntry* node) {
  int          result = 1;
  DB2EnvEntry* step   = NULL;
  db2Entry1("(start: %x, node: %x)", start, node);
  for (step = start; step != NULL; step = step->right){
    if (step == node) {
      free (step->nls_lang);
      step->nls_lang = NULL;
      if (step->left == NULL && step->right == NULL){
        rootenvEntry = NULL;
        db2Debug3("rootenvEntry     : %x", rootenvEntry);
        db2Debug3("DB2Enventry freed: %x", step);
        free (step);
        step = NULL;
      } else if (step->left == NULL) {
        step->right->left = NULL;
        db2Debug3("rootenvEntry     : %x", rootenvEntry);
        db2Debug3("DB2Enventry freed: %x", step);
        free (step);
        step = NULL;
      } else if (step->right == NULL) {
        step->left->right = NULL;
        db2Debug3("rootenvEntry     : %x", rootenvEntry);
        db2Debug3("DB2Enventry freed: %x", step);
        free (step);
        step = NULL;
      } else {
        step->left->right = step->right;
        step->right->left = step->left;
        db2Debug3("rootenvEntry     : %x", rootenvEntry);
        db2Debug3("DB2Enventry freed: %x", step);
        free (step);
        step = NULL;
      }
      result = 0;
      break;
    }
  }
  db2Exit1(": %d",result);
  return result;
}

/* deleteenvEntryLang */
static int deleteenvEntryLang(DB2EnvEntry* start, const char* nlslang)  {
  int          result = 1;
  DB2EnvEntry *step = NULL;
  db2Entry1("(start: %x, nlslang: %s)", start, nlslang);
  for (step = start; step != NULL; step = step->right){
    if (strcmp (step->nls_lang, nlslang) == 0) {
      free (step->nls_lang);
      if (step->left == NULL && step->right == NULL){
        rootenvEntry = NULL;
        db2Debug3("rootenvEntry     : %x", rootenvEntry);
        db2Debug3("DB2Enventry freed: %x", step);
        free (step);
        step = NULL;
      } else if (step->left == NULL) {
        step->right->left = NULL;
        db2Debug3("rootenvEntry     : %x", rootenvEntry);
        db2Debug3("DB2Enventry freed: %x", step);
        free (step);
        step = NULL;
      } else if (step->right == NULL) {
        step->left->right = NULL;
        db2Debug3("rootenvEntry     : %x", rootenvEntry);
        db2Debug3("DB2Enventry freed: %x", step);
        free (step);
        step = NULL;
      } else {
        step->left->right = step->right;
        step->right->left = step->left;
        db2Debug3("rootenvEntry     : %x", rootenvEntry);
        db2Debug3("DB2Enventry freed: %x", step);
        free (step);
        step = NULL;
      }
      result = 0;
      break;
    }
  }
  db2Exit1(": %d",result);
  return result;
}

/* findenvEntryHandle  */
static DB2EnvEntry* findenvEntryHandle (DB2EnvEntry* start, SQLHENV henv) {
  DB2EnvEntry* step = NULL;
  db2Entry1("(start: %x, SQLHENV: %d)",start, henv);
  for (step = start; step != NULL; step = step->right){
    if (step->henv == henv) {
      break;
    }
  }
  db2Exit1(": %x",step);
  return step;
}

/* findenvEntry */
DB2EnvEntry* findenvEntry(DB2EnvEntry* start, const char* nlslang) {
  DB2EnvEntry* step = NULL;
  db2Entry1("(start: %x, nlslang: '%s')", start, nlslang);
  for (step = start; step != NULL; step = step->right) {
    db2Debug3("step: %x ->nls_lang: '%s'", step, step->nls_lang);
    db2Debug3("nls_lang      : '%s'", nlslang);
    db2Debug3("strcmp(step->nls_lang, nlslang): %d",strcmp (step->nls_lang, nlslang));
    if (strcmp (step->nls_lang, nlslang) == 0) {
      break;
    }
  }
  if (step != NULL) {
    db2Debug3("step: %x, ->henv: %d, ->nls_lang: '%s', ->connlist: %x", step, step->henv, step->nls_lang,step->connlist);
  }
  db2Exit1(": %x", step);
  return step;
}
