#include <postgres.h>
#include "db2_fdw.h"
#include "DB2FdwDirectModifyState.h"

/** external variables */
extern regproc* output_funcs;

/** external prototypes */
extern void         db2CloseStatement    (DB2Session* session);

/** local prototypes */
void db2EndDirectModify(ForeignScanState* node);

/* postgresEndDirectModify
 * Finish a direct foreign table modification
 */
void db2EndDirectModify(ForeignScanState* node) {
	DB2FdwDirectModifyState* fdw_state = (DB2FdwDirectModifyState*) node->fdw_state;

	db2Entry1();
	/* MemoryContext will be deleted automatically. */
  if (fdw_state == NULL) {
    db2Debug2("no fdw_state, nothing to do");
    return;
  }

//  /* If you’re batching for COPY, flush any remaining rows here */
//  if (fdw_state->session && fdw_state->db2Table) {
//    /* e.g. db2FlushBatch(fdw_state->session, fdw_state->db2Table); */
//  }

  /* Finish statement / cursor, if you keep a handle there */
  if (fdw_state->session) {
    db2CloseStatement (fdw_state->session);
    db2free(fdw_state->session,"fdw_state->session");
    fdw_state->session = NULL;
  }

  if (fdw_state->temp_cxt) {
    MemoryContextDelete (fdw_state->temp_cxt);
    fdw_state->temp_cxt = NULL;
  }

  if (output_funcs){
    db2free(output_funcs,"output_funcs");
    output_funcs = NULL;
  }
 
  db2free(fdw_state,"fdw_state");
  node->fdw_state = NULL;
	db2Exit1();
}