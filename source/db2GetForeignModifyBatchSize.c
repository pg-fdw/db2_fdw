#include <postgres.h>
#if PG_VERSION_NUM >= 140000

#include <nodes/makefuncs.h>
#include <utils/guc.h>
#include <access/heapam.h>
#include "db2_fdw.h"
#include "DB2FdwState.h"

/** external variables */

/** external prototypes */

/** local prototypes */
       int  db2GetForeignModifyBatchSize(ResultRelInfo *rinfo);
static int  db2_get_batch_size_option   (Relation rel);

/* db2GetForeignModifyBatchSize
 * Determine the maximum number of tuples that can be inserted in bulk
 *
 * Returns the batch size specified for server or table. 
 * When batching is not allowed (e.g. for tables with BEFORE/AFTER ROW triggers or with RETURNING clause), returns 1.
 */
int db2GetForeignModifyBatchSize(ResultRelInfo *rinfo) {
  int          batch_size  = 1;
  DB2FdwState* fmstate     = (DB2FdwState*) rinfo->ri_FdwState;

  db2Entry1();
  /* should be called only once */
  Assert(rinfo->ri_BatchSize == 0);

  /* Should never get called when the insert is being performed on a table that is also among the target relations of an UPDATE operation, because
   * postgresBeginForeignInsert() currently rejects such insert attempts.
   */
  Assert(fmstate == NULL || fmstate->aux_fmstate == NULL);

  /* In EXPLAIN without ANALYZE, ri_FdwState is NULL, so we have to lookup the option directly in server/table options. 
   * Otherwise just use the value we determined earlier.
   */
  batch_size = (fmstate) ? fmstate->db2Table->batchsz : db2_get_batch_size_option(rinfo->ri_RelationDesc);

  /* Disable batching when we have to use RETURNING, there are any BEFORE/AFTER ROW INSERT triggers on the foreign table, or there are any
   * WITH CHECK OPTION constraints from parent views.
   *
   * When there are any BEFORE ROW INSERT triggers on the table, we can't support it, because such triggers might query the table we're inserting
   * into and act differently if the tuples that have already been processed and prepared for insertion are not there.
   */
  if (rinfo->ri_projectReturning != NULL 
  ||  rinfo->ri_WithCheckOptions != NIL 
  || (rinfo->ri_TrigDesc && (rinfo->ri_TrigDesc->trig_insert_before_row  ||  rinfo->ri_TrigDesc->trig_insert_after_row))) {
    batch_size = 1;
  } else {
    int resultLength = (fmstate) ? (sizeof(fmstate->resultList)/sizeof(DB2ResultColumn*)) : 0;
    /* If the foreign table has no columns, disable batching as the INSERT syntax doesn't allow batching multiple empty rows into a zero-column
     * table in a single statement.  
     * This is needed for COPY FROM, in which case fmstate must be non-NULL.
     */
    if (resultLength == 0) {
      batch_size = 1;
    } else {
      int paramLength = (fmstate) ? (sizeof(fmstate->paramList)/sizeof(ParamDesc*)) : 0;
      /* Otherwise use the batch size specified for server/table. 
       * The number of parameters in a batch is limited to 65535 (uint16), 
       * so make sure we don't exceed this limit by using the maximum batch_size possible.
       */
      if (paramLength > 0) {
        batch_size = Min(batch_size, (PQ_QUERY_PARAM_MAX_LIMIT / paramLength));
      }
    }
  }
  db2Exit1(": %d", batch_size);
  return batch_size;
}

static int db2_get_batch_size_option(Relation rel) {
  Oid            relid      = RelationGetRelid(rel);
  ForeignTable*  table      = NULL;
  ForeignServer* server     = NULL;
  ListCell*      lc         = NULL;
  int            batch_size = 0;

  db2Entry1();
  table  = GetForeignTable(relid);
  server = GetForeignServer(table->serverid);

  /* Table-level option has precedence. */
  foreach(lc, table->options) {
    DefElem *def = (DefElem *) lfirst(lc);
    if (strcmp(def->defname, OPT_BATCH_SIZE) == 0) {
      char  *valstr = strVal(def->arg);
      int32  val;

      if (!parse_int(valstr, &val, 0, NULL) || val <= 0) {
        ereport(ERROR,
                (errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
                errmsg("invalid value for option \"%s\"", OPT_BATCH_SIZE),
                errdetail("Value must be a positive integer.")));
      }
      batch_size = val;
      break;
    }
  }

  if (batch_size < 1) {
    /* Fall back to server-level option, if any. */
    foreach(lc, server->options) {
      DefElem *def = (DefElem *) lfirst(lc);
      if (strcmp(def->defname, OPT_BATCH_SIZE) == 0) {
        char  *valstr = strVal(def->arg);
        int32  val;

        if (!parse_int(valstr, &val, 0, NULL) || val <= 0) {
          ereport(ERROR,
                  (errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
                   errmsg("invalid value for option \"%s\"", OPT_BATCH_SIZE),
                   errdetail("Value must be a positive integer.")));
        }
        batch_size = val;
        break;
      }
    }
  }
  /* Default: no batching */
  batch_size = (batch_size < 1) ? DEFAULT_BATCHSZ : batch_size; 
  db2Exit1(": %d", batch_size);
  return batch_size;
}
#endif