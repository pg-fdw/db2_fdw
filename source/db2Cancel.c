#include "db2_fdw.h"

/** global variables */

/** external variables */
extern DB2EnvEntry* rootenvEntry;          /* contains DB2 error messages, set by db2CheckErr()             */

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
  for (envp = rootenvEntry; envp != NULL; envp = envp->right) {
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
