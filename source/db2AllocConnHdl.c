#include <string.h>
#include <stdio.h>
#include "db2_fdw.h"

/** external variables */
extern char      db2Message[ERRBUFSIZE];/* contains DB2 error messages, set by db2CheckErr()             */

/** external prototypes */
extern void      db2Error_d           (db2error sqlstate, const char* message, const char* detail, ...);
extern void      db2RegisterCallback  (void* arg);
extern SQLRETURN db2CheckErr          (SQLRETURN status, SQLHANDLE handle, SQLSMALLINT handleType, int line, char* file);
extern void      db2FreeEnvHdl        (DB2EnvEntry* envp, const char* nls_lang);

/** local prototypes */
       DB2ConnEntry*    db2AllocConnHdl      (DB2EnvEntry* envp,const char* srvname, char* user, char* password, char* jwt_token, const char* nls_lang);
       DB2ConnEntry*    findconnEntry        (DB2ConnEntry* start, const char* srvname, const char* user, const char* jwttok);
static DB2ConnEntry*    insertconnEntry      (DB2ConnEntry* start, const char* srvname, const char* uid, const char* pwd, const char* jwt_token, SQLHDBC hdbc);

/* db2AllocConnHdl */
DB2ConnEntry* db2AllocConnHdl(DB2EnvEntry* envp,const char* srvname, char* user, char* password, char* jwt_token, const char* nls_lang) {
  DB2ConnEntry* connp   = NULL;
  SQLRETURN     rc      = 0;
  SQLHDBC       hdbc    = SQL_NULL_HDBC;

  db2Entry1("(envp: %x, srvname: %s, user: %s, password: %s, jwt_token: %s, nls_lang: %s)", envp, srvname,user, password, jwt_token ? "***" : "NULL", nls_lang);
  if (nls_lang != NULL) {
    rc = SQLAllocHandle(SQL_HANDLE_DBC, envp->henv, &hdbc);
    db2Debug3("alloc dbc handle - rc: %d, henv: %d, hdbc: %d",rc, envp->henv, hdbc);
    rc = db2CheckErr(rc, hdbc, SQL_HANDLE_DBC, __LINE__, __FILE__);
    if (rc != SQL_SUCCESS) {
        db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2: SQLAllocHandle failed to allocate hdbc handle", db2Message);
        db2FreeEnvHdl(envp,nls_lang);
        envp = NULL;
    }
    // envp->connlist = connp = insertconnEntry (envp->connlist, srvname, user, password, hdbc);
  } else {
    /* search user session for this server in cache */
    connp = findconnEntry(envp->connlist, srvname, user, jwt_token);
    if (connp == NULL) {
      /* Declare all variables at beginning for C90 compatibility */
      char connStr[4096];
      int connStrLen;
      SQLCHAR outConnStr[1024];
      SQLSMALLINT outConnStrLen;

      /* create connection handle */
      rc = SQLAllocHandle(SQL_HANDLE_DBC, envp->henv, &hdbc);
      db2Debug3("alloc dbc handle - rc: %d, henv: %d, hdbc: %d",rc, envp->henv, hdbc);
      rc = db2CheckErr(rc, envp->henv, SQL_HANDLE_ENV, __LINE__, __FILE__);
      if (rc  != SQL_SUCCESS) {
        db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2: SQLAllochHandle failed to allocate hdbc handle", db2Message);
      }

      /* Check if JWT token authentication is used */
      if (jwt_token != NULL && jwt_token[0] != '\0') {
        /* JWT token authentication */
        db2Debug2("using JWT token authentication");

        /* For DB2 11.5.4+ with JWT, use SQLDriverConnect with AUTHENTICATION=TOKEN */
        /* Requires: DB2 client 11.5.4+, server configured with db2token.cfg */
        /* and SRVCON_AUTH set to SERVER_ENCRYPT_TOKEN or similar */

        /* Build connection string with JWT token using DB2 TOKEN authentication keywords */
        /* JWT tokens contain identity information, so UID is typically not required */
        connStrLen = snprintf(connStr, sizeof(connStr),
                             "DSN=%s;AUTHENTICATION=TOKEN;ACCESSTOKEN=%s;ACCESSTOKENTYPE=JWT;",
                             srvname, jwt_token);

        if (connStrLen >= sizeof(connStr)) {
          db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "connection string too long", " connection to foreign DB2 server");
        }

        db2Debug2("connecting with connection string (token hidden)");

        /* Use SQLDriverConnect instead of SQLConnect */
        rc = SQLDriverConnect(hdbc, NULL, (SQLCHAR*)connStr, SQL_NTS,
                             outConnStr, sizeof(outConnStr), &outConnStrLen,
                             SQL_DRIVER_NOPROMPT);

        db2Debug2("connect to database(%s) with JWT token - rc: %d, hdbc: %d", srvname, rc, hdbc);
        rc = db2CheckErr(rc, hdbc, SQL_HANDLE_DBC, __LINE__, __FILE__);
        if (rc != SQL_SUCCESS) {
          db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "cannot authenticate with JWT token", " connection connectstring: %s ,%s", srvname, db2Message);
          db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "cannot authenticate", " connection to foreign DB2 server,%s", db2Message);
        }
      } else {
        /* Traditional user/password authentication */
        db2Debug2("using user/password authentication");
        rc = SQLConnect(hdbc, (SQLCHAR*)srvname, SQL_NTS, (SQLCHAR*)user, SQL_NTS, (SQLCHAR*)password, SQL_NTS);
        db2Debug2("connect to database(%s) - rc: %d, hdbc: %d",srvname, rc, hdbc);
        rc = db2CheckErr(rc, hdbc, SQL_HANDLE_DBC, __LINE__, __FILE__);
        if (rc != SQL_SUCCESS) {
          db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "cannot authenticate"," connection User: %s ,%s"            , user    , db2Message);
          db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "cannot authenticate"," connection password: %s ,%s"        , password, db2Message);
          db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "cannot authenticate"," connection connectstring: %s ,%s"   , srvname , db2Message);
          db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "cannot authenticate"," connection to foreign DB2 server,%s"          , db2Message);
        }
      }

      if (rc == SQL_SUCCESS) {
        /* add session handle to cache */
        envp->connlist = connp = insertconnEntry (envp->connlist, srvname, user, password, jwt_token, hdbc);

        /* set Autocommit off */
        rc = SQLSetConnectAttr(hdbc, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_OFF, SQL_IS_UINTEGER);
        rc = db2CheckErr(rc, hdbc, SQL_HANDLE_DBC, __LINE__, __FILE__);
        if (rc != SQL_SUCCESS) {
          db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "failed to set autocommit=off"," connection to foreign DB2 server,%s", db2Message);
        }
        /* register callback for PostgreSQL transaction events */
        db2RegisterCallback (connp);
      }
    }
  }
  db2Exit1(": %x",connp);
  return connp;
}

/* findconnEntry */
DB2ConnEntry* findconnEntry(DB2ConnEntry* start, const char* srvname, const char* user, const char* jwttok) {
  DB2ConnEntry* step = NULL;
  db2Entry2();
  for (step = start; step != NULL; step = step->right){
    /* NULL-safe comparison for JWT auth where user may be NULL */
    int jwt_null_or_empty     = (!step->jwt_token || step->jwt_token[0] == '\0');
    int jwttok_null_or_empty  = (!jwttok || jwttok[0] == '\0');
    int jwt_match             = (jwt_null_or_empty && jwttok_null_or_empty)
                             || (!jwt_null_or_empty && !jwttok_null_or_empty && strcmp(step->jwt_token, jwttok) == 0);

    int srv_null_or_empty     = (!step->srvname || step->srvname[0] == '\0');
    int srvname_null_or_empty = (!srvname || srvname[0] == '\0');
    int srv_match             = (srv_null_or_empty && srvname_null_or_empty) 
                             || (!srv_null_or_empty && !srvname_null_or_empty && strcmp(step->srvname, srvname) == 0);

    int uid_null_or_empty     = (!step->uid || step->uid[0] == '\0');
    int user_null_or_empty    = (!user || user[0] == '\0');
    int uid_match             = (uid_null_or_empty && user_null_or_empty)
                             || (!uid_null_or_empty && !user_null_or_empty && strcmp(step->uid, user) == 0);

    if (srv_match && uid_match && jwt_match) {
      break;
    }
  }
  db2Exit2(": %x", step);
  return step;
}

/* insertconnEntry  */
static DB2ConnEntry* insertconnEntry(DB2ConnEntry* start, const char* srvname, const char* uid, const char* pwd, const char* jwt_token, SQLHDBC hdbc) {
  DB2ConnEntry* step = NULL;
  DB2ConnEntry* new  = NULL;

  db2Entry2();
  new = malloc(sizeof(DB2ConnEntry));
  if (start == NULL){ /* first entry in list */
    new->right = new->left = NULL;
  } else {
    for (step = start; step->right != NULL; step = step->right){ }
    step->right = new;
    new->left = step;
    new->right = NULL;
  }
  // generate a deep copy using strdup, so these values survive together with DB2ConnEntry
  new->srvname    = (srvname   && srvname[0]   != '\0') ? strdup(srvname)   : NULL;
  new->uid        = (uid       && uid[0]       != '\0') ? strdup(uid)       : NULL;
  new->pwd        = (pwd       && pwd[0]       != '\0') ? strdup(pwd)       : NULL;
  new->jwt_token  = (jwt_token && jwt_token[0] != '\0') ? strdup(jwt_token) : NULL;
  new->handlelist = NULL;
  new->hdbc       = hdbc;
  new->xact_level = 0;
  db2Exit2(": %x",new);
  return new;
}
