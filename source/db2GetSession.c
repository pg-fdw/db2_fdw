#include "db2_fdw.h"

/** global variables */

/** external variables */
extern DB2EnvEntry*  rootenvEntry;          /* contains DB2 error messages, set by db2CheckErr()             */

/** external prototypes */
extern DB2ConnEntry* db2AllocConnHdl      (DB2EnvEntry* envp,const char* srvname, char* user, char* password, char* jwt_token, const char* nls_lang);
extern DB2EnvEntry*  db2AllocEnvHdl       (const char* nls_lang);
extern DB2EnvEntry*  findenvEntry         (DB2EnvEntry* start, const char* nlslang);
extern DB2ConnEntry* findconnEntry        (DB2ConnEntry* start, const char* srvname, const char* user, const char* jwttok);
extern void          db2SetSavepoint      (DB2Session* session, int nest_level);

/** local prototypes */
DB2Session*          db2GetSession        (const char* srvname, char* user, char* password, char* jwt_token, const char* nls_lang, int curlevel);

/* db2GetSession
 * Look up an DB2 connection in the cache, create a new one if there is none.
 * The result is an allocated data structure containing the connection.
 * "curlevel" is the current PostgreSQL transaction level.
 */
DB2Session* db2GetSession (const char* srvname, char* user, char* password, char* jwt_token, const char* nls_lang, int curlevel) {
  DB2Session*   session = NULL;
  DB2EnvEntry*  envp    = NULL;
  DB2ConnEntry* connp   = NULL;

  db2Entry1();
  /* it's easier to deal with empty strings */
  if (!srvname)   srvname   = "";
  if (!user)      user      = "";
  if (!password)  password  = "";
  if (!jwt_token) jwt_token = "";
  if (!nls_lang)  nls_lang  = "";

  /* search environment and server handle in cache */
  db2Debug2("rootenvEntry: %x", rootenvEntry);
  envp = findenvEntry (rootenvEntry, nls_lang);
  if (envp == NULL) {
    envp = db2AllocEnvHdl(nls_lang);
  }
  connp = findconnEntry(envp->connlist, srvname, user, jwt_token);
  if (connp == NULL){
    connp = db2AllocConnHdl(envp, srvname, user, password, jwt_token, NULL);
  }
  if (connp->xact_level <= 0) {
    db2Debug2("db2_fdw::db2GetSession: begin serializable remote transaction");
    connp->xact_level = 1;
  }

  /* allocate a data structure pointing to the cached entries */
  session        = db2alloc(sizeof (DB2Session),"DB2Session* session");
  session->envp  = envp;
  session->connp = connp;
  session->stmtp = NULL;

  /* set savepoints up to the current level */
  db2SetSavepoint (session, curlevel);

  db2Exit1();
  return session;
}
