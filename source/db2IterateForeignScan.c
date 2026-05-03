#include <postgres.h>
#include <commands/explain.h>
#include <commands/vacuum.h>
#include <utils/syscache.h>
#include <optimizer/optimizer.h>
#include <access/heapam.h>
#include <access/xact.h>
#include "db2_fdw.h"
#include "DB2FdwState.h"

/** external prototypes */
extern int          db2IsStatementOpen        (DB2Session* session);
extern void         db2PrepareQuery           (DB2Session* session, const char *query, DB2ResultColumn* resultList, unsigned long prefetch, int fetchsize);
extern int          db2ExecuteQuery           (DB2Session* session, ParamDesc* paramList);
extern int          db2FetchNext              (DB2Session* session);
extern void         db2CloseStatement         (DB2Session* session);
extern void         convertTuple              (DB2Session* session, DB2Table* db2Table, DB2ResultColumn* reslist, int natts, Datum* values, bool* nulls);
extern char*        deparseDate               (Datum datum);
extern char*        deparseTimestamp          (Datum datum, bool hasTimezone);

/** local prototypes */
       TupleTableSlot* db2IterateForeignScan(ForeignScanState* node);
static char*           setSelectParameters  (ParamDesc *paramList, ExprContext * econtext);

/* db2IterateForeignScan
 * On first invocation (if there is no DB2 statement yet), get the actual parameter values and run the remote query against
 * the DB2 database, retrieving the first result row.
 * Subsequent invocations will fetch more result rows until there are no more.
 * The result is stored as a virtual tuple in the ScanState's TupleSlot and returned.
 */
TupleTableSlot* db2IterateForeignScan (ForeignScanState* node) {
  TupleTableSlot* slot      = node->ss.ss_ScanTupleSlot;
  ExprContext*    econtext  = node->ss.ps.ps_ExprContext;
  int             have_result;
  DB2FdwState*    fdw_state = (DB2FdwState*) node->fdw_state;

  db2Entry1();
  if (db2IsStatementOpen (fdw_state->session)) {
    db2Debug2("get next row in foreign table scan");
    /* fetch the next result row */
    have_result = db2FetchNext (fdw_state->session);
  } else {
    /* fill the parameter list with the actual values */
    char* paramInfo = setSelectParameters (fdw_state->paramList, econtext);
    /* execute the DB2 statement and fetch the first row */
    db2Debug2("execute query in foreign table scan '%s'", paramInfo);
    db2PrepareQuery (fdw_state->session, fdw_state->query, fdw_state->resultList, fdw_state->prefetch, fdw_state->fetch_size);
    have_result = db2ExecuteQuery (fdw_state->session, fdw_state->paramList);
    have_result = db2FetchNext (fdw_state->session);
  }
  /* initialize virtual tuple */
  ExecClearTuple (slot);
  if (have_result) {
    /* increase row count */
    ++fdw_state->rowcount;
    /* convert result to arrays of values and null indicators */
    db2Debug2("slot->tts_tupleDescriptor->natts: %d",slot->tts_tupleDescriptor->natts);
    convertTuple (fdw_state->session,fdw_state->db2Table,fdw_state->resultList, slot->tts_tupleDescriptor->natts, slot->tts_values, slot->tts_isnull);
    /* store the virtual tuple */
    ExecStoreVirtualTuple (slot);
  } else {
    /* close the statement */
    db2CloseStatement (fdw_state->session);
  }
  db2Exit1();
  return slot;
}

/** setSelectParameters
 *   Set the current values of the parameters into paramList.
 *   Return a string containing the parameters set for a DEBUG message.
 */
static char* setSelectParameters (ParamDesc* paramList, ExprContext* econtext) {
  ParamDesc*     param;
  Datum          datum;
  HeapTuple      tuple;
  TimestampTz    tstamp;
  bool           is_null;
  bool           first_param = true;
  MemoryContext  oldcontext;
  StringInfoData info;     /* list of parameters for DEBUG message */

  db2Entry4();
  db2Debug5("paramList: %x",paramList);
  db2Debug5("econtext : %x",econtext);
  
  initStringInfo (&info);

  /* switch to short lived memory context */
  oldcontext = MemoryContextSwitchTo (econtext->ecxt_per_tuple_memory);

  /* iterate parameter list and fill values */
  for (param = paramList; param; param = param->next) {
    if (param->txts) {
      /* get transaction start timestamp */
      tstamp = GetCurrentTransactionStartTimestamp ();

      datum = TimestampGetDatum (tstamp);
      is_null = false;
    } else {
      if (param->node) {
        /* Evaluate the expression. This code path cannot be reached in 9.1 */
        datum = ExecEvalExpr ((ExprState *) (param->node), econtext, &is_null);
      } else {
        is_null = true;
      }
    }

    if (is_null) {
      param->value = NULL;
    } else {
      if (param->type == DATEOID)
        param->value = deparseDate (datum);
      else if (param->type == TIMESTAMPOID || param->type == TIMESTAMPTZOID)
        param->value = deparseTimestamp (datum, false/*(param->type == TIMESTAMPTZOID)*/);
      else if (param->type == TIMEOID || param->type == TIMETZOID)
        param->value = deparseTimestamp (datum, false/*(param->type == TIMETZOID)*/);
      else {
        regproc typoutput;

        /* get the type's output function */
        tuple = SearchSysCache1 (TYPEOID, ObjectIdGetDatum (param->type));
        if (!HeapTupleIsValid (tuple)) {
          elog (ERROR, "cache lookup failed for type %u", param->type);
        }
        typoutput = ((Form_pg_type) GETSTRUCT (tuple))->typoutput;
        ReleaseSysCache (tuple);

        /* convert the parameter value into a string */
        param->value = DatumGetCString (OidFunctionCall1 (typoutput, datum));
      }
    }

    /* build a parameter list for the DEBUG message */
    if (first_param) {
      first_param = false;
      appendStringInfo (&info, ", parameters ?=\"%s\"", (param->value ? param->value : "(null)"));
    } else {
      appendStringInfo (&info, ", ?=\"%s\"", (param->value ? param->value : "(null)"));
    }
  }

  /* reset memory context */
  MemoryContextSwitchTo (oldcontext);

  db2Exit4(": %s", info.data);
  return info.data;
}

