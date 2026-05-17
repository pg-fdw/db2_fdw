#include <postgres.h>
#include <funcapi.h>
#include <miscadmin.h>
#include <nodes/nodeFuncs.h>
#include <utils/rel.h>
#include <utils/lsyscache.h>

#include "db2_fdw.h"
#include "DB2FdwDirectModifyState.h"

extern DB2Session*              db2GetSession             (const char* connectstring, char* user, char* password, char* jwt_token, const char* nls_lang, int curlevel);
extern DB2FdwDirectModifyState* db2GetFdwDirectModifyState(Oid foreigntableid, double* sample_percent, bool describe);

       void       db2BeginDirectModify            (ForeignScanState* node, int eflags);
static TupleDesc  get_tupdesc_for_join_scan_tuples(ForeignScanState* node);
static void       init_returning_filter           (DB2FdwDirectModifyState* dmstate, List* fdw_scan_tlist, Index rtindex);
static void       prepare_query_params            (PlanState* node, List* fdw_exprs, int numParams, FmgrInfo** param_flinfo, List** param_exprs, const char ***param_values);

/* postgresBeginDirectModify
 * Prepare a direct foreign table modification
 */
void db2BeginDirectModify(ForeignScanState* node, int eflags) {
  ForeignScan*              fsplan 	= (ForeignScan*) node->ss.ps.plan;
  EState*                   estate 	= node->ss.ps.state;
  DB2FdwDirectModifyState*  dmstate	= NULL;
  Index                     rtindex;
  Relation                  foreigntable;

  db2Entry1();
  /* Do nothing in EXPLAIN (no ANALYZE) case.  node->fdw_state stays NULL. */
  if (!(eflags & EXEC_FLAG_EXPLAIN_ONLY)) {
    /* Get info about foreign table. */
    #if PG_VERSION_NUM < 140000
    rtindex           = estate->es_result_relation_info->ri_RangeTableIndex;
    #else
    rtindex           = node->resultRelInfo->ri_RangeTableIndex;
    #endif
    foreigntable      = (fsplan->scan.scanrelid == 0) ? ExecOpenScanRelation(estate, rtindex, eflags) : node->ss.ss_currentRelation;
      /* We'll save private state in node->fdw_state. */
    dmstate           = db2GetFdwDirectModifyState(foreigntable->rd_id, NULL, false);
    dmstate->rel      = foreigntable;
    node->fdw_state   = dmstate;

    /* Update the foreign-join-related fields. */
    if (fsplan->scan.scanrelid == 0) {
      /* Save info about foreign table. */
      dmstate->resultRel = dmstate->rel;

     /* Set dmstate->rel to NULL to teach get_returning_data() and make_tuple_from_result_row() 
      * that columns fetched from the remote server are described by fdw_scan_tlist of the 
      * foreign-scan plan node, not the tuple descriptor for the target relation.
      */
      dmstate->rel = NULL;
    }

    /* Initialize state variable */
    dmstate->num_tuples = -1;	/* -1 means not set yet */

    /* Get private info created by planner functions. */
    dmstate->query           = strVal(list_nth (fsplan->fdw_private, FdwDirectModifyPrivateUpdateSql));
    db2Debug2("dmstate->query: %s",dmstate->query);
    #if PG_VERSION_NUM < 150000
    dmstate->has_returning   = intVal(list_nth(fsplan->fdw_private, FdwDirectModifyPrivateHasReturning));
    #else
    dmstate->has_returning   = boolVal(list_nth(fsplan->fdw_private, FdwDirectModifyPrivateHasReturning));
    #endif
    db2Debug2("dmstate->has_returning: %s",dmstate->has_returning? "true" : "false");
    dmstate->retrieved_attrs = (List*) list_nth(fsplan->fdw_private, FdwDirectModifyPrivateRetrievedAttrs);
    db2Debug2("dmstate->retrieved_attrs: %x - %d", dmstate->retrieved_attrs, list_length(dmstate->retrieved_attrs));
    #if PG_VERSION_NUM < 150000
    dmstate->set_processed   = intVal(list_nth(fsplan->fdw_private, FdwDirectModifyPrivateSetProcessed));
    #else
    dmstate->set_processed   = boolVal(list_nth(fsplan->fdw_private, FdwDirectModifyPrivateSetProcessed));
    #endif
    db2Debug2("dmstate->set_processed: %s", dmstate->set_processed ? "true" : "false");

    /* Create context for per-tuple temp workspace. */
    dmstate->temp_cxt = AllocSetContextCreate(estate->es_query_cxt,  "db2_fdw temporary data", ALLOCSET_SMALL_SIZES);

    /* Prepare for input conversion of RETURNING results. */
    if (dmstate->has_returning) {
      TupleDesc	tupdesc;

      if (fsplan->scan.scanrelid == 0)
        tupdesc = get_tupdesc_for_join_scan_tuples(node);
      else
        tupdesc = RelationGetDescr(dmstate->rel);

      dmstate->attinmeta = TupleDescGetAttInMetadata(tupdesc);

      /* When performing an UPDATE/DELETE .. RETURNING on a join directly, initialize a filter to 
      * extract an updated/deleted tuple from a scan tuple.
      */
      if (fsplan->scan.scanrelid == 0)
        init_returning_filter(dmstate, fsplan->fdw_scan_tlist, rtindex);
    }

    /* Prepare for processing of parameters used in remote query, if any. */
    dmstate->numParams  = list_length(fsplan->fdw_exprs);
    if (dmstate->numParams > 0) {
      prepare_query_params( (PlanState*) node
                          , fsplan->fdw_exprs
                          , dmstate->numParams
                          , &dmstate->param_flinfo
                          , &dmstate->param_exprs
                          , &dmstate->param_values
                          );
    }
  }
  db2Exit1();
}

/*
 * Construct a tuple descriptor for the scan tuples handled by a foreign join.
 */
static TupleDesc get_tupdesc_for_join_scan_tuples(ForeignScanState *node) {
  ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
  EState	   *estate = node->ss.ps.state;
  TupleDesc	tupdesc;

  db2Entry4();
  /* The core code has already set up a scan tuple slot based on fsplan->fdw_scan_tlist, and this slot's tupdesc is mostly good enough, but there's one case where it isn't.
   * If we have any whole-row row identifier Vars, they may have vartype RECORD, and we need to replace that with the associated table's actual composite type.
   * This ensures that when we read those ROW() expression values from the remote server, we can convert them to a composite type the local server knows.
   */
  tupdesc = CreateTupleDescCopy(node->ss.ss_ScanTupleSlot->tts_tupleDescriptor);
  for (int i = 0; i < tupdesc->natts; i++) {
    Form_pg_attribute att = TupleDescAttr(tupdesc, i);
    Var*            var;
    RangeTblEntry*  rte;
    Oid             reltype;

    /* Nothing to do if it's not a generic RECORD attribute */
    if (att->atttypid != RECORDOID || att->atttypmod >= 0)
      continue;

    /* If we can't identify the referenced table, do nothing.  This'll likely lead to failure later, but perhaps we can muddle through. */
    var = (Var *) list_nth_node(TargetEntry, fsplan->fdw_scan_tlist, i)->expr;
    if (!IsA(var, Var) || var->varattno != 0)
      continue;
    rte = list_nth(estate->es_range_table, var->varno - 1);
    if (rte->rtekind != RTE_RELATION)
      continue;
    reltype = get_rel_type_id(rte->relid);
    if (!OidIsValid(reltype))
      continue;
    att->atttypid = reltype;
    /* shouldn't need to change anything else */
  }
  db2Exit4();
  return tupdesc;
}

/* Initialize a filter to extract an updated/deleted tuple from a scan tuple. */
static void init_returning_filter(DB2FdwDirectModifyState* dmstate, List* fdw_scan_tlist, Index rtindex) {
  TupleDesc resultTupType = RelationGetDescr(dmstate->resultRel);
  ListCell* lc            = NULL;
  int       i             = 0;

  db2Entry4();
  /* Calculate the mapping between the fdw_scan_tlist's entries and the result tuple's attributes.
   *
   * The "map" is an array of indexes of the result tuple's attributes in fdw_scan_tlist, i.e., one entry for every attribute 
   * of the result tuple.
   * We store zero for any attributes that don't have the corresponding entries in that list, marking that a NULL is needed in
   * the result tuple.
   *
   * Also get the indexes of the entries for ctid and oid if any.
   */
  dmstate->attnoMap   = (AttrNumber*) db2alloc(resultTupType->natts * sizeof(AttrNumber), "attnoMap");
  dmstate->ctidAttno  = dmstate->oidAttno = 0;

  i = 1;
  dmstate->hasSystemCols = false;
  foreach(lc, fdw_scan_tlist) {
    TargetEntry*  tle = (TargetEntry*) lfirst(lc);
    Var*          var = (Var*) tle->expr;

    Assert(IsA(var, Var));

    /* If the Var is a column of the target relation to be retrieved from the foreign server, get the index of the entry. */
    if (var->varno == rtindex && list_member_int(dmstate->retrieved_attrs, i)) {
      int attrno = var->varattno;
      if (attrno < 0) {
        /* We don't retrieve system columns other than ctid and oid. */
        if (attrno == SelfItemPointerAttributeNumber)
          dmstate->ctidAttno = i;
        else
          Assert(false);
        dmstate->hasSystemCols = true;
      } else {
        /* We don't retrieve whole-row references to the target relation either. */
        Assert(attrno > 0);
        dmstate->attnoMap[attrno - 1] = i;
      }
    }
    i++;
  }
  db2Exit4();
}

/* Prepare for processing of parameters used in remote query. */
static void prepare_query_params(PlanState* node, List* fdw_exprs, int numParams, FmgrInfo** param_flinfo, List** param_exprs, const char ***param_values) {
  int       i   = 0;
  ListCell* lc  = NULL;

  db2Entry4();
  Assert(numParams > 0);

  /* Prepare for output conversion of parameters used in remote query. */
  *param_flinfo = db2alloc(sizeof(FmgrInfo) * numParams,"param_flinfo");

  i = 0;
  foreach(lc, fdw_exprs) {
    Node* param_expr = (Node *) lfirst(lc);
    Oid   typefnoid;
    bool  isvarlena;

    getTypeOutputInfo(exprType(param_expr), &typefnoid, &isvarlena);
    fmgr_info(typefnoid, &(*param_flinfo)[i]);
    i++;
  }

  /* Prepare remote-parameter expressions for evaluation.
   * (Note: in practice, we expect that all these expressions will be just Params, so we could possibly do something 
   * more efficient than using the full expression-eval machinery for this.
   * But probably there would be little benefit, and it'd require postgres_fdw to know more than is desirable
   * about Param evaluation.)
   */
  *param_exprs = ExecInitExprList(fdw_exprs, node);

  /* Allocate buffer for text form of query parameters. */
  *param_values = (const char **) db2alloc(numParams * sizeof(char *),"param_values");
  db2Exit4();
}

