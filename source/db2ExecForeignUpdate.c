#include <postgres.h>
#include <nodes/makefuncs.h>
#include <utils/memutils.h>
#include <optimizer/optimizer.h>
#include <access/heapam.h>
#include "db2_fdw.h"
#include "DB2FdwState.h"

/** external variables */
extern bool dml_in_transaction;

/** external prototypes */
extern int          db2ExecuteQuery           (DB2Session* session, ParamDesc* paramList);
extern int          db2FetchNext              (DB2Session* session, DB2ResultColumn* resultList);
extern void         db2CloseCursor            (DB2Session* session);
extern void         setModifyParameters       (ParamDesc* paramList, TupleTableSlot* newslot, TupleTableSlot* oldslot, DB2Table* db2Table, DB2Session* session);
extern void         convertTuple              (DB2Session* session, DB2ResultColumn* reslist, DB2TupleIndexMode index_mode, int natts, Datum* values, bool* nulls);

/** local prototypes */
TupleTableSlot* db2ExecForeignUpdate      (EState* estate, ResultRelInfo* rinfo, TupleTableSlot* slot, TupleTableSlot* planSlot);

/* db2ExecForeignUpdate
 * Set the parameter values from the slots and execute the UPDATE statement.
 * Returns a slot with the results from the RETRUNING clause.
 */
TupleTableSlot* db2ExecForeignUpdate (EState* estate, ResultRelInfo* rinfo, TupleTableSlot* slot, TupleTableSlot* planSlot) {
  DB2FdwState*  fdw_state = (DB2FdwState*) rinfo->ri_FdwState;
  int           rows      = 0;
  MemoryContext oldcontext;

  db2Entry1();
  db2Debug2("relid: %d", RelationGetRelid (rinfo->ri_RelationDesc));

  ++fdw_state->rowcount;
  dml_in_transaction = true;

  MemoryContextReset (fdw_state->temp_cxt);
  oldcontext = MemoryContextSwitchTo (fdw_state->temp_cxt);

  /* extract the values from the slot and store them in the parameters */
  setModifyParameters (fdw_state->paramList, slot, planSlot, fdw_state->db2Table, fdw_state->session);

  /* execute the UPDATE statement */
  rows = db2ExecuteQuery (fdw_state->session, fdw_state->paramList);

  /* with a RETURNING clause the statement is a SELECT ... FROM NEW/OLD TABLE (...): fetch its row, then close the cursor for the next execution */
  if (fdw_state->resultList != NULL) {
    rows = db2FetchNext (fdw_state->session, fdw_state->resultList);
    db2CloseCursor (fdw_state->session);
  }

  if (rows != 1)
    ereport ( ERROR
            , ( errcode (ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION)
              , errmsg ("UPDATE on DB2 table changed %d rows instead of one in iteration %lu", rows, fdw_state->rowcount)
              , errhint ("This probably means that you did not set the \"key\" option on all primary key columns.")
              )
            );

  MemoryContextSwitchTo (oldcontext);

  /* empty the result slot */
  ExecClearTuple (slot);

  /* convert result for RETURNING to arrays of values and null indicators */
  convertTuple (fdw_state->session, fdw_state->resultList, DB2_TUPLE_INDEX_ATTRIBUTE,
                slot->tts_tupleDescriptor->natts, slot->tts_values, slot->tts_isnull);

  /* store the virtual tuple */
  ExecStoreVirtualTuple (slot);

  db2Exit1();
  return slot;
}
