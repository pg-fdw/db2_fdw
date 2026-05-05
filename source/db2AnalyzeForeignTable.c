#include <postgres.h>
#include <commands/vacuum.h>
#include <foreign/fdwapi.h>
#include <utils/memutils.h>
#include <nodes/pathnodes.h>
#include <optimizer/optimizer.h>
#include <access/heapam.h>
#include "db2_fdw.h"
#include "DB2FdwState.h"

/** external prototypes */
extern DB2FdwState* db2GetFdwState            (Oid foreigntableid, double* sample_percent, bool describe);
extern int          db2IsStatementOpen        (DB2Session* session);
extern void         db2PrepareQuery           (DB2Session* session, const char* query, DB2Table* db2Table, unsigned long prefetch);
extern int          db2ExecuteQuery           (DB2Session* session, const DB2Table* db2Table, ParamDesc* paramList);
extern int          db2FetchNext              (DB2Session* session);
extern void         checkDataType             (short db2type, int scale, Oid pgtype, const char* tablename, const char* colname);
extern short        c2dbType                  (short fcType);
extern void         convertTuple              (DB2FdwState* fdw_state, Datum* values, bool* nulls) ;
extern void         db2Debug1                 (const char* message, ...);
extern void         db2Debug2                 (const char* message, ...);
extern void         db2Debug3                 (const char* message, ...);
extern void*        db2alloc                  (const char* type, size_t size);

/** local prototypes */
bool db2AnalyzeForeignTable(Relation relation, AcquireSampleRowsFunc* func, BlockNumber* totalpages);
int  acquireSampleRowsFunc (Relation relation, int elevel, HeapTuple* rows, int targrows, double* totalrows, double* totaldeadrows);

/** db2AnalyzeForeignTable
 * 
 */
bool db2AnalyzeForeignTable (Relation relation, AcquireSampleRowsFunc* func, BlockNumber* totalpages) {
  db2Debug1("> db2AnalyzeForeignTable");
  *func = acquireSampleRowsFunc;
  /* use positive page count as a sign that the table has been ANALYZEd */
  *totalpages = 42;
  db2Debug1("< db2AnalyzeForeignTable");
  return true;
}

/** acquireSampleRowsFunc
 *   Perform a sequential scan on the DB2 table and return a sampe of rows.
 *   exceeding this is not used by compute_scalar_stats().
 */
int acquireSampleRowsFunc (Relation relation, int elevel, HeapTuple * rows, int targrows, double *totalrows, double *totaldeadrows) {
  int collected_rows = 0, i;
  DB2FdwState* fdw_state;
  bool first_column = true;
  StringInfoData query;
  TupleDesc tupDesc = RelationGetDescr (relation);
  Datum* values = (Datum*) db2alloc("values", tupDesc->natts* sizeof (Datum));
  bool*  nulls  = (bool*)  db2alloc("null"  , tupDesc->natts* sizeof (bool));
  double rstate, rowstoskip = -1, sample_percent;
  MemoryContext old_cxt, tmp_cxt;

  db2Debug1("> acquireSampleRowsFunc");
  elog (DEBUG1, "db2_fdw: analyze foreign table %d", RelationGetRelid (relation));

  *totalrows = 0;

  /* create a memory context for short-lived data in convertTuples() */
  tmp_cxt = AllocSetContextCreate (CurrentMemoryContext, "db2_fdw temporary data", ALLOCSET_SMALL_MINSIZE, ALLOCSET_SMALL_INITSIZE, ALLOCSET_SMALL_MAXSIZE);

  /* Prepare for sampling rows */
  rstate = anl_init_selection_state (targrows);

  /* get connection options, connect and get the remote table description */
  fdw_state = db2GetFdwState (RelationGetRelid (relation), &sample_percent, true);
  fdw_state->paramList = NULL;
  fdw_state->rowcount = 0;

  /* construct query */
  initStringInfo (&query);
  appendStringInfo (&query, "SELECT ");

  /* loop columns */
  for (i = 0; i < fdw_state->db2Table->ncols; ++i) {
    /* don't get LONG, LONG RAW and untranslatable values */
    short dbType = c2dbType(fdw_state->db2Table->cols[i]->colType);
    if (dbType == DB2_BIGINT || dbType == DB2_UNKNOWN_TYPE) {
      fdw_state->db2Table->cols[i]->used = 0;
    } else {
      db2Debug2("  fdw_state->db2Table->cols[%d]->name: %s",i,fdw_state->db2Table->cols[i]->colName);
      /* all columns are used */
      fdw_state->db2Table->cols[i]->used = 1;
      db2Debug2("  fdw_state->db2Table->cols[%d]->used: %d",i,fdw_state->db2Table->cols[i]->used);

      /* allocate memory for return value */
      db2Debug2("  fdw_state->db2Table->cols[%d]->val_size: %x",i,fdw_state->db2Table->cols[i]->val_size);
      fdw_state->db2Table->cols[i]->val = (char *) db2alloc ("fdw_state->db2Table->cols[i]->val", fdw_state->db2Table->cols[i]->val_size + 1);
      db2Debug2("  fdw_state->db2Table->cols[%d]->val: %x",i,fdw_state->db2Table->cols[i]->val);
      fdw_state->db2Table->cols[i]->val_len  = 0;
      db2Debug2("  fdw_state->db2Table->cols[%d]->val_len: %x",i,fdw_state->db2Table->cols[i]->val);
      fdw_state->db2Table->cols[i]->val_null = 1;
      db2Debug2("  fdw_state->db2Table->cols[%d]->val_null: %x",i,fdw_state->db2Table->cols[i]->val_null);

      if (first_column)
        first_column = false;
      else
        appendStringInfo (&query, ", ");

      /* append column name */
      appendStringInfo (&query, "%s", fdw_state->db2Table->cols[i]->colName);
    }
  }

  /* if there are no columns, use NULL */
  if (first_column)
    appendStringInfo (&query, "NULL");

  /* append DB2 table name */
  appendStringInfo (&query, " FROM %s", fdw_state->db2Table->name);

  /* append SAMPLE clause if appropriate */
  if (sample_percent < 100.0)
    appendStringInfo (&query, " SAMPLE BLOCK (%f)", sample_percent);

  fdw_state->query = query.data;
  elog (DEBUG2, "  fdw_state->query: '%s'", fdw_state->query);

  /* get PostgreSQL column data types, check that they match DB2's */
  for (i = 0; i < fdw_state->db2Table->ncols; ++i)
    if (fdw_state->db2Table->cols[i]->used)
      checkDataType (fdw_state->db2Table->cols[i]->colType, fdw_state->db2Table->cols[i]->colScale, fdw_state->db2Table->cols[i]->pgtype, fdw_state->db2Table->pgname, fdw_state->db2Table->cols[i]->pgname);

  db2Debug3("  loop through query results");
  /* loop through query results */
  while (db2IsStatementOpen (fdw_state->session) ? db2FetchNext (fdw_state->session) : (db2PrepareQuery (fdw_state->session, fdw_state->query, fdw_state->db2Table, fdw_state->prefetch), db2ExecuteQuery (fdw_state->session, fdw_state->db2Table, fdw_state->paramList))) {
    /* allow user to interrupt ANALYZE */
    #if PG_VERSION_NUM >= 180000
    vacuum_delay_point (true);
    #else
    vacuum_delay_point ();
    #endif

    ++fdw_state->rowcount;

    if (collected_rows < targrows) {
      /* the first "targrows" rows are added as samples */
      /* use a temporary memory context during convertTuple */
      old_cxt = MemoryContextSwitchTo (tmp_cxt);
      convertTuple (fdw_state, values, nulls);
      MemoryContextSwitchTo (old_cxt);
      rows[collected_rows++] = heap_form_tuple (tupDesc, values, nulls);
      MemoryContextReset (tmp_cxt);
    } else {
      /*
       * Skip a number of rows before replacing a random sample row.
       * A more detailed description of the algorithm can be found in analyze.c
       */
      if (rowstoskip < 0) {
        rowstoskip = anl_get_next_S (*totalrows, targrows, &rstate);
      }
      if (rowstoskip <= 0) {
        int k = (int) (targrows * anl_random_fract ());
        heap_freetuple (rows[k]);
        /* use a temporary memory context during convertTuple */
        old_cxt = MemoryContextSwitchTo (tmp_cxt);
        convertTuple (fdw_state, values, nulls);
        MemoryContextSwitchTo (old_cxt);
        rows[k] = heap_form_tuple (tupDesc, values, nulls);
        MemoryContextReset (tmp_cxt);
      }
    }
  }

  MemoryContextDelete (tmp_cxt);

  *totalrows = (double) fdw_state->rowcount / sample_percent * 100.0;
  *totaldeadrows = 0;

  /* report report */
  ereport (elevel, (errmsg ("\"%s\": table contains %lu rows; %d rows in sample", RelationGetRelationName (relation), fdw_state->rowcount, collected_rows)));

  db2Debug1("< acquireSampleRowsFunc");
  return collected_rows;
}

