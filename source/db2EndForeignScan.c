#include <postgres.h>
#include <nodes/makefuncs.h>
#include <optimizer/optimizer.h>
#include <access/heapam.h>
#include "db2_fdw.h"
#include "DB2FdwState.h"

/** external prototypes */
extern void         db2CloseStatement         (DB2Session* session);

/** local prototypes */
void db2EndForeignScan(ForeignScanState* node);

/* db2EndForeignScan
 * Close the currently active DB2 statement.
 */
void db2EndForeignScan (ForeignScanState* node) {
  DB2FdwState* fdw_state = (DB2FdwState*) node->fdw_state;

  db2Entry1();
  /* release the DB2 session */
  db2CloseStatement(fdw_state->session);
  // check fdw_state->session for dangling references that need to be freed
  db2free(fdw_state->session,"fdw_state->session");
  fdw_state->session = NULL;
  // check fdw_state for dangling references that need to be freed
  db2free(fdw_state,"fdw_state");
  db2Exit1();
}
