#include <postgres.h>
#include <miscadmin.h>
#include <access/heapam.h>
#include <commands/defrem.h>
#include <commands/extension.h>
#include <optimizer/cost.h>
#include <optimizer/optimizer.h>
#include <utils/guc.h>
#include <utils/varlena.h>
#include "db2_fdw.h"
#include "DB2FdwState.h"
#include "DB2FdwPathExtraData.h"

/** external prototypes */
extern DB2FdwState* db2GetFdwState            (Oid foreigntableid, double* sample_percent, bool describe);
extern char*        deparseWhereConditions    (PlannerInfo* root, RelOptInfo* baserel);
extern void         classifyConditions        (PlannerInfo* root, RelOptInfo* baserel, List* input_conds, List** remote_conds, List** local_conds);
extern void         estimate_path_cost_size   (PlannerInfo* root, RelOptInfo* foreignrel, List* param_join_conds, List* pathkeys, DB2FdwPathExtraData* fpextra, double* p_rows, int* p_width, int* p_disabled_nodes, Cost* p_startup_cost, Cost* p_total_cost);

/** local prototypes */
       void   db2GetForeignRelSize  (PlannerInfo* root, RelOptInfo* baserel, Oid foreigntableid);
static void   db2PopulateFdwStateOld(PlannerInfo* root, RelOptInfo* baserel, Oid foreigntableid);
static void   db2PopulateFdwStateNew(PlannerInfo* root, RelOptInfo* baserel, Oid foreigntableid);
static void   apply_server_options  (DB2FdwState* fpinfo);
static void   apply_table_options   (DB2FdwState* fpinfo);
static List*  ExtractExtensionList  (const char* extensionsString, bool warnOnMissing);


/** db2GetForeignRelSize
 *   Get an DB2FdwState for this foreign scan.
 *   Construct the remote SQL query.
 *   Provide estimates for the number of tuples, the average width and the cost.
 */
void db2GetForeignRelSize (PlannerInfo* root, RelOptInfo* baserel, Oid foreigntableid) {
  DB2FdwState* fdwState = NULL;

  db2Entry1();
  /* get connection options, connect and get the remote table description */
  fdwState = db2GetFdwState(foreigntableid, NULL, true);
  /* store the state so that the other planning functions can use it */
  baserel->fdw_private = (void*) fdwState;
  db2PopulateFdwStateOld(root, baserel,foreigntableid);
  db2PopulateFdwStateNew(root, baserel,foreigntableid);
  db2Exit1();
}

static void db2PopulateFdwStateOld(PlannerInfo* root, RelOptInfo* baserel, Oid foreigntableid){
  DB2FdwState* fdwState = (DB2FdwState*)baserel->fdw_private;
  int          i        = 0;
  double       ntuples  = -1;

  db2Entry1();
  /* Store the table OID in each table column.
   * This is redundant for base relations, but join relations will have columns from different tables, and we have to keep track of them.
   */
  for (i = 0; i < fdwState->db2Table->ncols; ++i) {
    fdwState->db2Table->cols[i]->pgrelid = baserel->relid;
  }
  /* Classify conditions into remote_conds or local_conds.
   * These parameters are used in foreign_join_ok and db2GetForeignPlan.
   * Those conditions that can be pushed down will be collected into an DB2 WHERE clause.
   */
  fdwState->where_clause = deparseWhereConditions ( root, baserel );

  /* release DB2 session (will be cached) */
  db2free (fdwState->session,"fdwState->session");
  fdwState->session = NULL;
  /* use a random "high" value for cost */
  fdwState->startup_cost = 10000.0;
  /* if baserel->pages > 0, there was an ANALYZE; use the row count estimate */
  if (baserel->pages > 0)
    ntuples = baserel->tuples;
  /* estimale selectivity locally for all conditions */
  /* apply statistics only if we have a reasonable row count estimate */
  if (ntuples != -1) {
    /* estimate how conditions will influence the row count */
    ntuples = ntuples * clauselist_selectivity (root, baserel->baserestrictinfo, 0, JOIN_INNER, NULL);
    /* make sure that the estimate is not less that 1 */
    ntuples = clamp_row_est (ntuples);
    baserel->rows = ntuples;
  }
  /* estimate total cost as startup cost + 10 * (returned rows) */
  fdwState->total_cost = fdwState->startup_cost + baserel->rows * 10.0;
  db2Exit1();
}

static void db2PopulateFdwStateNew(PlannerInfo* root, RelOptInfo* baserel, Oid foreigntableid){
  DB2FdwState* fpinfo = (DB2FdwState*)baserel->fdw_private;
  ListCell*     lc    = NULL;

  db2Entry1();
  /* Base foreign tables need to be pushed down always. */
  fpinfo->pushdown_safe = true;

  /* Look up foreign-table catalog info. */
  fpinfo->ftable  = GetForeignTable(foreigntableid);
  fpinfo->fserver = GetForeignServer(fpinfo->ftable->serverid);

  /* Extract user-settable option values.  Note that per-table settings of use_remote_estimate, fetch_size and async_capable override per-server
   * settings of them, respectively.
   */
  fpinfo->use_remote_estimate   = false;
  fpinfo->fdw_startup_cost      = DEFAULT_FDW_STARTUP_COST;
  fpinfo->fdw_tuple_cost        = DEFAULT_FDW_TUPLE_COST;
  fpinfo->shippable_extensions  = NIL;
  fpinfo->async_capable         = false;

  apply_server_options(fpinfo);
  apply_table_options(fpinfo);

  /* If the table or the server is configured to use remote estimates, identify which user to do remote access as during planning.
   * This should match what ExecCheckPermissions() does. If we fail due to lack of permissions, the query would have failed at runtime anyway.
   */
  if (fpinfo->use_remote_estimate) {
    Oid userid;

    userid        = OidIsValid(baserel->userid) ? baserel->userid : GetUserId();
    fpinfo->fuser = GetUserMapping(userid, fpinfo->fserver->serverid);
  } else {
    fpinfo->fuser = NULL;
  }

   /* Identify which baserestrictinfo clauses can be sent to the remote server and which can't. */
  classifyConditions(root, baserel, baserel->baserestrictinfo, &fpinfo->remote_conds, &fpinfo->local_conds);

  /* Identify which attributes will need to be retrieved from the remote server.
   * These include all attrs needed for joins or final output, plus all attrs used in the local_conds.
   * (Note: if we end up using a parameterized scan, it's possible that some of the join clauses will be sent to the remote 
   * and thus we wouldn't really need to retrieve the columns used in them.  Doesn't seem worth detecting that case though.)
   */
  fpinfo->attrs_used = NULL;
  pull_varattnos((Node *) baserel->reltarget->exprs, baserel->relid, &fpinfo->attrs_used);
  foreach(lc, fpinfo->local_conds) {
    RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

    pull_varattnos((Node *) rinfo->clause, baserel->relid, &fpinfo->attrs_used);
  }

  /* Compute the selectivity and cost of the local_conds, so we don't have to do it over again for each path.
   * The best we can do for these conditions is to estimate selectivity on the basis of local statistics.
   */
  fpinfo->local_conds_sel = clauselist_selectivity(root, fpinfo->local_conds, baserel->relid, JOIN_INNER, NULL);
  cost_qual_eval(&fpinfo->local_conds_cost, fpinfo->local_conds, root);

  /* Set # of retrieved rows and cached relation costs to some negative value, so that we can detect when they are set to some sensible values,
   * during one (usually the first) of the calls to estimate_path_cost_size.
   */
  fpinfo->retrieved_rows    = -1;
  fpinfo->rel_startup_cost  = -1;
  fpinfo->rel_total_cost    = -1;

  /* If the table or the server is configured to use remote estimates, connect to the foreign server and execute EXPLAIN to estimate the
   * number of rows selected by the restriction clauses, as well as the average row width.  Otherwise, estimate using whatever statistics we
   * have locally, in a way similar to ordinary tables.
   */
  if (fpinfo->use_remote_estimate) {
    /* Get cost/size estimates with help of remote server.
     * Save the values in fpinfo so we don't need to do it again to generate the basic foreign path.
     */
    estimate_path_cost_size(root, baserel, NIL, NIL, NULL, &fpinfo->rows, &fpinfo->width, &fpinfo->disabled_nodes, &fpinfo->startup_cost, &fpinfo->total_cost);

    /* Report estimated baserel size to planner. */
    baserel->rows = fpinfo->rows;
    baserel->reltarget->width = fpinfo->width;
  } else {
    /* If the foreign table has never been ANALYZEd, it will have reltuples < 0, meaning "unknown".
     * We can't do much if we're not allowed to consult the remote server, but we can use a hack similar 
     * to plancat.c's treatment of empty relations: use a minimum size estimate of 10 pages, and divide by 
     * the column-datatype-based width estimate to get the corresponding number of tuples.
     */
    if (baserel->tuples < 0) {
      baserel->pages  = 10;
      baserel->tuples = (10 * BLCKSZ) / (baserel->reltarget->width + MAXALIGN(SizeofHeapTupleHeader));
    }

    /* Estimate baserel size as best we can with local statistics. */
    set_baserel_size_estimates(root, baserel);

    /* Fill in basically-bogus cost estimates for use later. */
    estimate_path_cost_size(root, baserel, NIL, NIL, NULL, &fpinfo->rows, &fpinfo->width, &fpinfo->disabled_nodes, &fpinfo->startup_cost, &fpinfo->total_cost);
  }

  /* fpinfo->relation_name gets the numeric rangetable index of the foreign table RTE.
   * (If this query gets EXPLAIN'd, we'll convert that to a human-readable string at that time.)
   */
  fpinfo->relation_name = psprintf("%u", baserel->relid);

  /* No outer and inner relations. */
  fpinfo->make_outerrel_subquery  = false;
  fpinfo->make_innerrel_subquery  = false;
  fpinfo->lower_subquery_rels     = NULL;
  fpinfo->hidden_subquery_rels    = NULL;
  /* Set the relation index. */
  fpinfo->relation_index          = baserel->relid;
  db2Exit1();
}

/* Parse options from foreign server and apply them to fpinfo.
 * New options might also require tweaking merge_fdw_options().
 */
static void apply_server_options(DB2FdwState* fpinfo) {
  ListCell* lc;

  db2Entry4();
  foreach(lc, fpinfo->fserver->options) {
    DefElem*  def = (DefElem*) lfirst(lc);

    if (strcmp(def->defname, "use_remote_estimate") == 0)
      fpinfo->use_remote_estimate = defGetBoolean(def);
    else if (strcmp(def->defname, "fdw_startup_cost") == 0)
      (void) parse_real(defGetString(def), &fpinfo->fdw_startup_cost, 0, NULL);
    else if (strcmp(def->defname, "fdw_tuple_cost") == 0)
      (void) parse_real(defGetString(def), &fpinfo->fdw_tuple_cost, 0, NULL);
    else if (strcmp(def->defname, "extensions") == 0)
      fpinfo->shippable_extensions = ExtractExtensionList(defGetString(def), false);
    else if (strcmp(def->defname, "fetch_size") == 0)
      (void) parse_int(defGetString(def), &fpinfo->fetch_size, 0, NULL);
    else if (strcmp(def->defname, "async_capable") == 0)
      fpinfo->async_capable = defGetBoolean(def);
  }
  db2Exit4();
}

/* Parse options from foreign table and apply them to fpinfo.
 * New options might also require tweaking merge_fdw_options().
 */
static void apply_table_options(DB2FdwState* fpinfo) {
  ListCell* lc;

  db2Entry4();
  foreach(lc, fpinfo->ftable->options) {
    DefElem*  def = (DefElem*) lfirst(lc);

    if (strcmp(def->defname, "use_remote_estimate") == 0)
      fpinfo->use_remote_estimate = defGetBoolean(def);
    else if (strcmp(def->defname, "fetch_size") == 0)
      (void) parse_int(defGetString(def), &fpinfo->fetch_size, 0, NULL);
    else if (strcmp(def->defname, "async_capable") == 0)
      fpinfo->async_capable = defGetBoolean(def);
  }
  db2Exit4();
}

/* Parse a comma-separated string and return a List of the OIDs of the extensions named in the string.  If any names in the list cannot be
 * found, report a warning if warnOnMissing is true, else just silently ignore them.
 */
static List * ExtractExtensionList(const char *extensionsString, bool warnOnMissing) {
  List*     extensionOids = NIL;
  List*     extlist       = NIL;
  ListCell* lc            = NULL;

  db2Entry4("extensionString: %s, warnOnMissing: %d",extensionsString, warnOnMissing);
  /* SplitIdentifierString scribbles on its input, so pstrdup first */
  if (!SplitIdentifierString(pstrdup(extensionsString), ',', &extlist)) {
    /* syntax error in name list */
    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("parameter \"%s\" must be a list of extension names", "extensions")));
  }

  foreach(lc, extlist) {
    const char *extension_name = (const char *) lfirst(lc);
    Oid extension_oid = get_extension_oid(extension_name, true);

    if (OidIsValid(extension_oid)) {
      extensionOids = lappend_oid(extensionOids, extension_oid);
    } else if (warnOnMissing) {
      ereport(WARNING, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("extension \"%s\" is not installed", extension_name)));
    }
  }
  list_free(extlist);
  db2Exit4(": %x", extensionOids);
  return extensionOids;
}
