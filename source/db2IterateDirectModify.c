#include <postgres.h>
#include <catalog/pg_type.h>
#include <utils/syscache.h>
#include "db2_fdw.h"
#include "DB2FdwDirectModifyState.h"

/** external prototypes */
extern int          db2IsStatementOpen        (DB2Session* session);
extern void         db2PrepareQuery           (DB2Session* session, const char *query, DB2ResultColumn* resultList, unsigned long prefetch, int fetchsize);
extern int          db2ExecuteQuery           (DB2Session* session, ParamDesc* paramList);
extern int          db2FetchNext              (DB2Session* session, DB2ResultColumn* resultList);
extern void         db2CloseStatement         (DB2Session* session);

/** local prototypes */
       TupleTableSlot* db2IterateDirectModify(ForeignScanState *node);

       /* postgresIterateDirectModify
 * Execute a direct foreign table modification
 */
TupleTableSlot* db2IterateDirectModify(ForeignScanState *node) {
  DB2FdwDirectModifyState* 	dmstate     = (DB2FdwDirectModifyState*) node->fdw_state;
  EState*                   estate      = node->ss.ps.state;
  TupleTableSlot*           slot        = node->ss.ss_ScanTupleSlot;
//  ResultRelInfo*            rtinfo      = node->resultRelInfo;
  int                       have_result = 0;
  MemoryContext             oldcontext;

  // nachfolgende Werte in ParamDesc Liste zusammenfassen
//  int             numParams     = dmstate->numParams;
//  const char**    values        = dmstate->param_values;
//  FmgrInfo*       param_flinfo  = dmstate->param_flinfo;
//  List*           param_exps    = dmstate->param_exprs;
  
  db2Entry1();
  // We should have a valid session.
  // It was created during db2GetFdwDirectModifyState during db2BeginDirectModify
  MemoryContextReset (dmstate->temp_cxt);
  oldcontext = MemoryContextSwitchTo (dmstate->temp_cxt);
  db2PrepareQuery(dmstate->session, dmstate->query, NULL, dmstate->prefetch, dmstate->fetch_size);
  dmstate->num_tuples = db2ExecuteQuery (dmstate->session, dmstate->paramList);
  db2Debug2(" %d rows affected by this query", have_result);
  MemoryContextSwitchTo (oldcontext);

  slot = ExecClearTuple (slot);
  if (dmstate->num_tuples) {
    Instrumentation*  instr = node->ss.ps.instrument;

    /* Increment the command es_processed count if necessary. */
    if (dmstate->set_processed)
      estate->es_processed += dmstate->num_tuples;

    /* Increment the tuple count for EXPLAIN ANALYZE if necessary. */
    if (instr)
      instr->tuplecount += dmstate->num_tuples;

    /* initialize virtual tuple */
  } else {
    /* close the statement */
    db2CloseStatement (dmstate->session);
  }

  db2Exit1(": %x", slot);
  return slot;
}
