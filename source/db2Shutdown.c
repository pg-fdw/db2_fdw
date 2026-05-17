#include "db2_fdw.h"

/** global variables  */
int                 silent = 0;            /* emit no error messages when set, used for shutdown            */

/** external variables */
extern int          sql_initialized;       /* set to "1" as soon as SQLAllocHandle(SQL_HANDLE_ENV is called */
extern DB2EnvEntry* rootenvEntry;          /* Linked list of handles for cached DB2 connections.            */

/** external prototypes */
extern void      db2FreeEnvHdl        (DB2EnvEntry* envp, const char* nls_lang);
extern void      db2CloseConnections  (void);

/** local prototypes */
void db2Shutdown(void);

/* db2Shutdown
 * Close all open connections, release handles, terminate DB2.
 * This will be called at the end of the PostgreSQL session.
 */
void db2Shutdown (void) {
  db2Entry1();
  /* don't report error messages */
  silent = 1;
  db2CloseConnections();
  /* done with DB2 */
  if (sql_initialized)
    db2FreeEnvHdl(rootenvEntry, NULL);
  db2Exit1();
}
