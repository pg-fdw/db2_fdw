#include "db2_fdw.h"

/** global variables */

/** external variables */
extern DB2EnvEntry* rootenvEntry;          /* Cached handle for the (at most one) DB2 environment per backend. */

/** external prototypes */

/** local prototypes */
void             db2Cancel            (void);

/** db2Cancel
 *   Cancel all running DB2 queries.
 */
void db2Cancel (void) {
  DB2EnvEntry*  envp   = NULL;
  DB2ConnEntry* connp  = NULL;
  HdlEntry*     entryp = NULL;

  db2Entry1();
  /* send a cancel request for all servers ignoring errors */
  envp = rootenvEntry;
  if (envp != NULL) {
    for (connp = envp->connlist; connp != NULL; connp = connp->right) {
      for (entryp = connp->handlelist; entryp != NULL; entryp = entryp->next){
        if (entryp->type == SQL_HANDLE_STMT) {
          SQLCancel(entryp->hsql);
        }
      }
    }
  }
  db2Exit1();
}
