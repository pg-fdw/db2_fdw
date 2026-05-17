#include <postgres.h>
#include <nodes/makefuncs.h>
#include <optimizer/optimizer.h>
#include <access/heapam.h>
#include "db2_fdw.h"
#include "DB2FdwState.h"

/** external prototypes */
extern void         db2CloseStatement         (DB2Session* session);

/** local prototypes */
void db2ReScanForeignScan(ForeignScanState* node);

/* db2ReScanForeignScan
 * Close the DB2 statement if there is any.
 * That causes the next db2IterateForeignScan call to restart the scan.
 */
void db2ReScanForeignScan (ForeignScanState* node) {
  DB2FdwState* fdw_state = (DB2FdwState*) node->fdw_state;
 
  db2Entry1();
  /* close open DB2 statement if there is one */
  db2CloseStatement(fdw_state->session);
  /* reset row count to zero */
  fdw_state->rowcount = 0;
  db2Exit1();
}
