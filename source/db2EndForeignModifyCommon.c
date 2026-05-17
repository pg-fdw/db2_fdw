#include <postgres.h>
#include <nodes/makefuncs.h>
#include <utils/memutils.h>
#include <optimizer/optimizer.h>
#include <access/heapam.h>
#include "db2_fdw.h"
#include "DB2FdwState.h"

/** external variables */
extern regproc* output_funcs;

/** external prototypes */
extern void         db2CloseStatement    (DB2Session* session);

/** local prototypes */
void                db2EndForeignModifyCommon(EState *estate, ResultRelInfo *rinfo);

void db2EndForeignModifyCommon(EState *estate, ResultRelInfo *rinfo) {
  DB2FdwState *fdw_state = NULL;

  db2Entry1();
  db2Debug2("relid: %d", RelationGetRelid (rinfo->ri_RelationDesc));

  fdw_state = (DB2FdwState*) rinfo->ri_FdwState;
  if (fdw_state == NULL) {
    db2Debug2("no fdw_state, nothing to do");
    return;
  }

  /* If you’re batching for COPY, flush any remaining rows here */
  if (fdw_state->session && fdw_state->db2Table) {
        /* e.g. db2FlushBatch(fdw_state->session, fdw_state->db2Table); */
  }

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
 
  rinfo->ri_FdwState = NULL;
  db2free(fdw_state,"fdw_state");
  db2Exit1();
}
