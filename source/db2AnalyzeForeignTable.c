#include <postgres.h>
#include <access/heapam.h>
#if PG_VERSION_NUM < 140000
#include <access/xact.h>
#endif
#include <commands/vacuum.h>
#include <optimizer/optimizer.h>
#include <utils/memutils.h>
#include "db2_fdw.h"
#include "DB2FdwState.h"

/** external prototypes */
extern DB2Session*  db2GetSession             (const char* connectstring, char* user, char* password, char* jwt_token, const char* nls_lang, int curlevel);
extern DB2FdwState* db2GetFdwState            (Oid foreigntableid, double* sample_percent, bool describe);
extern int          db2IsStatementOpen        (DB2Session* session);
extern void         db2PrepareQuery           (DB2Session* session, const char *query, DB2ResultColumn* resultList, unsigned long prefetch, int fetchsize);
extern int          db2ExecuteQuery           (DB2Session* session, ParamDesc* paramList);
extern int          db2FetchNext              (DB2Session* session);
extern void         checkDataType             (short db2type, int scale, Oid pgtype, const char* tablename, const char* colname);
extern short        c2dbType                  (short fcType);
extern void         convertTuple              (DB2Session* session, DB2Table* db2Table, DB2ResultColumn* reslist, int natts, Datum* values, bool* nulls);

/** local prototypes */
       bool db2AnalyzeForeignTable(Relation relation, AcquireSampleRowsFunc* func, BlockNumber* totalpages);
static int  acquireSampleRowsFunc (Relation relation, int elevel, HeapTuple* rows, int targrows, double* totalrows, double* totaldeadrows);

/* db2AnalyzeForeignTable */
bool db2AnalyzeForeignTable (Relation relation, AcquireSampleRowsFunc* func, BlockNumber* totalpages) {
  db2Entry1();
  *func = acquireSampleRowsFunc;
  /* use positive page count as a sign that the table has been ANALYZEd */
  *totalpages = 42;
  db2Exit1(": true");
  return true;
}

/* acquireSampleRowsFunc
 * Perform a sequential scan on the DB2 table and return a sample of rows.
 * All LOB values are truncated to WIDTH_THRESHOLD+1 because anything exceeding this is not used by compute_scalar_stats().
 */
static int acquireSampleRowsFunc (Relation relation, int elevel, HeapTuple* rows, int targrows, double* totalrows, double* totaldeadrows) {
  int               collected_rows  = 0;
  DB2FdwState*      fdw_state       = NULL;
  bool              first_column    = true;
  StringInfoData    query;
  TupleDesc         tupDesc         = RelationGetDescr (relation);
  Datum*            values          = (Datum*) db2alloc(tupDesc->natts* sizeof (Datum), "values");
  bool*             nulls           = (bool*)  db2alloc(tupDesc->natts* sizeof (bool) , "null");
  double            rstate          = 0;
  double            rowstoskip      = -1;
  double            sample_percent  = 0;
  MemoryContext     old_cxt;
  MemoryContext     tmp_cxt;

  db2Entry1();
  db2Debug2("db2_fdw: analyze foreign table %d", RelationGetRelid (relation));

  *totalrows = 0;

  /* create a memory context for short-lived data in convertTuples() */
  tmp_cxt = AllocSetContextCreate (CurrentMemoryContext, "db2_fdw temporary data", ALLOCSET_SMALL_MINSIZE, ALLOCSET_SMALL_INITSIZE, ALLOCSET_SMALL_MAXSIZE);

  /* Prepare for sampling rows */
  rstate = anl_init_selection_state (targrows);

  /* get connection options, connect and get the remote table description */
  fdw_state             = db2GetFdwState (RelationGetRelid (relation), &sample_percent, true);
  if (!fdw_state->session) {
    fdw_state->session  = db2GetSession (fdw_state->dbserver, fdw_state->user, fdw_state->password, fdw_state->jwt_token, fdw_state->nls_lang, GetCurrentTransactionNestLevel () );
  }
  fdw_state->paramList  = NULL;
  fdw_state->rowcount   = 0;

  /* construct query */
  initStringInfo (&query);
  appendStringInfo (&query, "SELECT ");

  /* loop columns */
  for (int i = 0; i < fdw_state->db2Table->ncols; ++i) {
    if (DB2_UNKNOWN_TYPE != c2dbType(fdw_state->db2Table->cols[i]->colType)) {
      checkDataType(fdw_state->db2Table->cols[i]->colType,fdw_state->db2Table->cols[i]->colScale,fdw_state->db2Table->cols[i]->pgtype,fdw_state->db2Table->pgname,fdw_state->db2Table->cols[i]->pgname);
      appendStringInfo (&query, "%s%s", ((first_column) ? "" : ", "), fdw_state->db2Table->cols[i]->colName);
      first_column = false;
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
  db2Debug2("fdw_state->query: '%s'", fdw_state->query);

  db2Debug3("loop through query results");
  /* loop through query results */
  fdw_state->rowcount = -1;
  while (db2IsStatementOpen (fdw_state->session) ? db2FetchNext (fdw_state->session) : (db2PrepareQuery (fdw_state->session, fdw_state->query, fdw_state->resultList, fdw_state->prefetch, fdw_state->fetch_size), db2ExecuteQuery (fdw_state->session, fdw_state->paramList))) {
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
      convertTuple (fdw_state->session,fdw_state->db2Table,fdw_state->resultList, tupDesc->natts, values, nulls);
      MemoryContextSwitchTo (old_cxt);
      rows[collected_rows++] = heap_form_tuple (tupDesc, values, nulls);
      MemoryContextReset (tmp_cxt);
    } else {
      /* Skip a number of rows before replacing a random sample row.
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
        convertTuple (fdw_state->session,fdw_state->db2Table,fdw_state->resultList, tupDesc->natts, values, nulls);
        MemoryContextSwitchTo (old_cxt);
        rows[k] = heap_form_tuple (tupDesc, values, nulls);
        MemoryContextReset (tmp_cxt);
      }
    }
  }

  MemoryContextDelete (tmp_cxt);

  *totalrows      = (double) fdw_state->rowcount / sample_percent * 100.0;
  *totaldeadrows  = 0;

  /* report report */
  ereport (elevel, (errmsg ("\"%s\": table contains %lu rows; %d rows in sample", RelationGetRelationName (relation), fdw_state->rowcount, collected_rows-1)));

  db2Exit1();
  return collected_rows;
}

