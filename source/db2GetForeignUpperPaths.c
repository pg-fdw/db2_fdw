#include <postgres.h>
#include <catalog/pg_operator.h>
#include <catalog/pg_opfamily.h>
#include <miscadmin.h>
#include <nodes/makefuncs.h>
#include <optimizer/cost.h>
#include <optimizer/optimizer.h>
#include <optimizer/paths.h>
#include <optimizer/pathnode.h>
#include <optimizer/clauses.h>
#include <optimizer/prep.h>
#include <optimizer/tlist.h>
#include <optimizer/restrictinfo.h>
#include <utils/selfuncs.h>

#include "db2_fdw.h"
#include "DB2FdwState.h"
#include "DB2FdwPathExtraData.h"

#if PG_VERSION_NUM < 140000
/* source-code-compatibility hacks for pull_varnos() API change */
#define make_restrictinfo(a,b,c,d,e,f,g,h,i) make_restrictinfo_new(a,b,c,d,e,f,g,h,i)
#endif

/** external prototypes */
extern List*              build_tlist_to_deparse    (RelOptInfo* foreignrel);
extern char*              deparseExpr               (PlannerInfo* root, RelOptInfo* foreignrel, Expr* expr, List** params);
extern bool               is_shippable              (Oid objectId, Oid classId, DB2FdwState* fpinfo);
extern bool               is_foreign_param          (PlannerInfo *root, RelOptInfo *baserel, Expr *expr);
extern bool               is_foreign_expr           (PlannerInfo *root, RelOptInfo *baserel, Expr *expr);
extern bool               is_foreign_pathkey        (PlannerInfo *root, RelOptInfo *baserel, PathKey *pathkey);
extern void               deparseSelectStmtForRel   (StringInfo buf, PlannerInfo* root, RelOptInfo* rel,List* tlist, List* remote_conds, List* pathkeys, bool has_final_sort, bool has_limit, bool is_subquery, List** retrieved_attrs, List** params_list);
extern void               classifyConditions        (PlannerInfo* root, RelOptInfo* baserel, List* input_conds, List** remote_conds, List** local_conds);
extern EquivalenceMember* find_em_for_rel_target    (PlannerInfo* root, EquivalenceClass* ec, RelOptInfo* rel);

/** local prototypes */
void                db2GetForeignUpperPaths   (PlannerInfo *root, UpperRelationKind stage, RelOptInfo *input_rel, RelOptInfo *output_rel, void *extra);
static void         db2CloneFdwStateUpper     (PlannerInfo* root, RelOptInfo* input_rel, RelOptInfo* output_rel);
static DB2Table*    db2CloneDb2TableForPlan   (const DB2Table* src);
static DB2Column*   db2CloneDb2ColumnForPlan  (const DB2Column* src);
//static bool         db2_is_shippable          (PlannerInfo* root, UpperRelationKind stage, RelOptInfo* input_rel, RelOptInfo* output_rel);
//static bool         db2_is_shippable_expr     (PlannerInfo* root, RelOptInfo* foreignrel, Expr* expr, const char* label);
static void         add_foreign_grouping_paths(PlannerInfo* root, RelOptInfo* input_rel, RelOptInfo* grouped_rel, GroupPathExtraData *extra);
static void         add_foreign_ordered_paths (PlannerInfo* root, RelOptInfo* input_rel, RelOptInfo* ordered_rel);
static void         add_foreign_final_paths   (PlannerInfo* root, RelOptInfo* input_rel, RelOptInfo* final_rel, FinalPathExtraData *extra);
static void         adjust_foreign_grouping_path_cost(PlannerInfo* root, List* pathkeys, double retrieved_rows, double width, double limit_tuples, int* p_disabled_nodes, Cost* p_startup_cost, Cost* p_run_cost);
static bool         foreign_grouping_ok       (PlannerInfo* root, RelOptInfo* grouped_rel, Node* havingQual);
       void         estimate_path_cost_size   (PlannerInfo* root, RelOptInfo* foreignrel, List* param_join_conds, List* pathkeys, DB2FdwPathExtraData* fpextra, double* p_rows, int* p_width, int* p_disabled_nodes, Cost* p_startup_cost, Cost* p_total_cost);
static void         merge_fdw_options         (DB2FdwState* fpinfo, const DB2FdwState* fpinfo_o, const DB2FdwState* fpinfo_i);

void db2GetForeignUpperPaths(PlannerInfo *root, UpperRelationKind stage, RelOptInfo *input_rel, RelOptInfo *output_rel, void *extra) {
  db2Entry1();
  if (root != NULL && root->parse != NULL && input_rel->fdw_private != NULL && output_rel->fdw_private == NULL) {
    Query*       query  = root->parse;
    DB2FdwState* fpinfo = NULL;

    db2Debug3("query->hasAggs        : %s", query->hasAggs         ? "true" : "false");
    db2Debug3("query->hasWindowFuncs : %s", query->hasWindowFuncs  ? "true" : "false");
    db2Debug3("query->hasDistinctOn  : %s", query->hasDistinctOn   ? "true" : "false");
    db2Debug3("query->hasTargetSRFs  : %s", query->hasTargetSRFs   ? "true" : "false");
    db2Debug3("query->hasForUpdate   : %s", query->hasForUpdate    ? "true" : "false");
    #if PG_VERSION_NUM >= 180000
    db2Debug3("query->hasGroupRTE    : %s", query->hasGroupRTE     ? "true" : "false");
    #endif
    db2Debug3("query->hasModifyingCTE: %s", query->hasModifyingCTE ? "true" : "false");
    db2Debug3("query->hasRecursive   : %s", query->hasRecursive    ? "true" : "false");
    db2Debug3("query->hasSubLinks    : %s", query->hasSubLinks     ? "true" : "false");
    db2Debug3("query->hasRowSecurity : %s", query->hasRowSecurity  ? "true" : "false");

    db2CloneFdwStateUpper(root, input_rel, output_rel);

    /*
     * Ensure upperrel FDW state is fully initialized.
     * - stage is relied upon by later upper-path stages (e.g. FINAL handling of ORDERED input).
     * - outerrel must point at the immediate underlying relation for upperrels.
     */
    fpinfo = (DB2FdwState*) output_rel->fdw_private;
    if (fpinfo != NULL) {
      fpinfo->stage    = stage;
      fpinfo->outerrel = input_rel;
    }    
    switch (stage) {
      case UPPERREL_SETOP:              // UNION/INTERSECT/EXCEPT
        db2Debug2("stage: %d - UPPERREL_SETOP", stage);
      break;
      case UPPERREL_PARTIAL_GROUP_AGG:  // partial grouping/aggregation
        db2Debug2("stage: %d - UPPERREL_PARTIAL_GROUP_AGG", stage);
        db2Debug2("query->hasAggs: %d", query->hasAggs);
        db2Debug2("query->groupClause: %x", query->groupClause);
        if (query->hasAggs || query->groupClause != NIL) {
          add_foreign_grouping_paths(root, input_rel, output_rel, (GroupPathExtraData*) extra);
        }
      break;
      case UPPERREL_GROUP_AGG: {        // grouping/aggregation
        db2Debug2("stage: %d - UPPERREL_GROUP_AGG", stage);
        db2Debug2("query->hasAggs: %d", query->hasAggs);
        db2Debug2("query->groupClause: %x", query->groupClause);
        if (query->hasAggs || query->groupClause != NIL) {
          add_foreign_grouping_paths(root, input_rel, output_rel, (GroupPathExtraData*) extra);
        }
      }
      break;
      case UPPERREL_WINDOW: {           // window functions
        db2Debug2("stage: %d - UPPERREL_WINDOW", stage);
        db2Debug2("query->hasWindowFuncs: %d", query->hasWindowFuncs);
        if (query->hasWindowFuncs) {
          db2Debug2("window function push down not yet implemented");
        }
      }
      break;
      #if PG_VERSION_NUM >= 150000
      case UPPERREL_PARTIAL_DISTINCT: { // partial "SELECT DISTINCT"
        db2Debug2("stage: %d - UPPERREL_PARTIAL_DISTINCT", stage);
        db2Debug2("query->hasDistinctOn: %d", query->hasDistinctOn);
        if (query->hasDistinctOn) {
          db2Debug2("distinct function push down not yet implemented");
        }
      }
      break;
      #endif
      case UPPERREL_DISTINCT: {         // "SELECT DISTINCT"
        db2Debug2("stage: %d - UPPERREL_DISTINCT", stage);
        db2Debug2("query->hasDistinctOn: %d", query->hasDistinctOn);
        if (query->hasDistinctOn) {
          db2Debug2("distinct function push down not yet implemented");
        }
      }
      break;
      case UPPERREL_ORDERED:            // ORDER BY
        db2Debug2("stage: %d - UPPERREL_ORDERED", stage);
        db2Debug2("query->setOperations: %x", query->setOperations);
        /*
         * ORDER BY handling: attempt pushdown when this query has a sort clause.
         * (The ordered upperrel is part of the normal path even when there are no set operations.)
         */
        if (query->sortClause != NIL) {
          add_foreign_ordered_paths(root, input_rel, output_rel);
        }
      break;
      case UPPERREL_FINAL:              // any remaining top-level actions
        db2Debug2("stage: %d - UPPERREL_FINAL", stage);
        add_foreign_final_paths(root, input_rel, output_rel, (FinalPathExtraData*) extra);
      break;
      default:                          // unknown stage type
        db2Debug2("stage: %d - unknown", stage);
      break;
    }
  } else {
    db2Debug2("skipping this call");
    db2Debug2("root: %x", root);
    db2Debug2("root->parse: %x", root->parse);
    db2Debug2("input_rel->fdw_private: %x", input_rel->fdw_private);
    db2Debug2("output_rel->fdw_private: %x", output_rel->fdw_private);
  }
  db2Exit1();
}

/* db2CloneFdwStateUpper
 * Create a deep copy suitable for upper-relation planning.
 *
 * Rationale: planning can change mutable fields like DB2Column.used and also rewrite the params list (createQuery will NULL out entries).
 * We must avoid those mutations affecting the original baserel/joinrel planning state.
 */
static void db2CloneFdwStateUpper(PlannerInfo* root, RelOptInfo* input_rel, RelOptInfo* output_rel) {
  DB2FdwState* fdw_in = (DB2FdwState*)input_rel->fdw_private;
  DB2FdwState* copy   = NULL;

  db2Entry4();
  if (fdw_in != NULL) {
    copy = (DB2FdwState*) db2alloc(sizeof(DB2FdwState), "copy");

    /* Start from a full struct copy so we don't leave fields uninitialized. */
    *copy = *fdw_in;

    /* Deep-copy mutable/owned pointer fields */
    copy->dbserver      = fdw_in->dbserver      ? db2strdup(fdw_in->dbserver,      "copy->dbserver")      : NULL;
    copy->user          = fdw_in->user          ? db2strdup(fdw_in->user,          "copy->user")          : NULL;
    copy->password      = fdw_in->password      ? db2strdup(fdw_in->password,      "copy->password")      : NULL;
    copy->jwt_token     = fdw_in->jwt_token     ? db2strdup(fdw_in->jwt_token,     "copy->jwt_token")     : NULL;
    copy->nls_lang      = fdw_in->nls_lang      ? db2strdup(fdw_in->nls_lang,      "copy->nls_lang")      : NULL;
    copy->query         = fdw_in->query         ? db2strdup(fdw_in->query,         "copy->query")         : NULL;
    copy->order_clause  = fdw_in->order_clause  ? db2strdup(fdw_in->order_clause,  "copy->order_clause")  : NULL;
    copy->where_clause  = fdw_in->where_clause  ? db2strdup(fdw_in->where_clause,  "copy->where_clause")  : NULL;
    copy->relation_name = fdw_in->relation_name ? db2strdup(fdw_in->relation_name, "copy->relation_name") : NULL;

    /* Shallow-copy expression lists (Expr nodes are immutable at this stage), but ensure list cells are independent because createQuery mutates the list. */
    copy->params              = fdw_in->params              ? list_copy(fdw_in->params)              : NIL;
    copy->retrieved_attr      = fdw_in->retrieved_attr      ? list_copy(fdw_in->retrieved_attr)      : NIL;
    copy->remote_conds        = fdw_in->remote_conds        ? list_copy(fdw_in->remote_conds)        : NIL;
    copy->local_conds         = fdw_in->local_conds         ? list_copy(fdw_in->local_conds)         : NIL;
    copy->final_remote_exprs  = fdw_in->final_remote_exprs  ? list_copy(fdw_in->final_remote_exprs)  : NIL;
    copy->shippable_extensions= fdw_in->shippable_extensions? list_copy(fdw_in->shippable_extensions): NIL;
    copy->joinclauses         = fdw_in->joinclauses         ? list_copy(fdw_in->joinclauses)         : NIL;
    copy->grouped_tlist       = fdw_in->grouped_tlist       ? list_copy(fdw_in->grouped_tlist)       : NIL;

    copy->attrs_used          = fdw_in->attrs_used          ? bms_copy(fdw_in->attrs_used)           : NULL;
    copy->lower_subquery_rels = fdw_in->lower_subquery_rels ? bms_copy(fdw_in->lower_subquery_rels)  : NULL;
    copy->hidden_subquery_rels= fdw_in->hidden_subquery_rels? bms_copy(fdw_in->hidden_subquery_rels) : NULL;

    /* Deep-copy DB2 table/columns because DB2Column.used is re-derived for each planned query shape. */
    copy->db2Table            = db2CloneDb2TableForPlan(fdw_in->db2Table);

    /* Runtime-only / per-plan rebuilt fields */
    copy->rowcount            = 0;
    copy->temp_cxt            = NULL;
    copy->paramList           = NULL;
  }
  output_rel->fdw_private = copy;
  db2Exit4();
}

static DB2Table* db2CloneDb2TableForPlan(const DB2Table* src) {
  DB2Table* dst = NULL;
  int i;

  db2Entry4();
  if (src != NULL) {
    dst = (DB2Table*) db2alloc(sizeof(DB2Table),"DB2Table* dst");

    dst->name    = src->name   ? db2strdup(src->name,"dst->name")   : NULL;
    dst->pgname  = src->pgname ? db2strdup(src->pgname,"dst->pgname") : NULL;
    dst->batchsz = src->batchsz;
    dst->ncols   = src->ncols;
    dst->npgcols = src->npgcols;

    if (src->ncols > 0) {
      dst->cols = (DB2Column**) db2alloc(sizeof(DB2Column*) * src->ncols,"dst->cols(%d)",src->ncols);
      for (i = 0; i < src->ncols; ++i) {
        dst->cols[i] = db2CloneDb2ColumnForPlan(src->cols[i]);
      }
    } else {
      dst->cols = NULL;
    }
  }
  db2Exit4(": %x", dst);
  return dst;
}

static DB2Column* db2CloneDb2ColumnForPlan(const DB2Column* src) {
  DB2Column* dst = NULL;

  db2Entry4();
  if (src != NULL) {
    dst = (DB2Column*) db2alloc( sizeof(DB2Column),"DB2Column* dst");
    /* start with a struct copy, then fix up pointer members */
    *dst = *src;

    dst->colName = src->colName ? db2strdup(src->colName,"dst->colName") : NULL;
    dst->pgname  = src->pgname  ? db2strdup(src->pgname,"dst->pgname")  : NULL;
  }
  db2Exit4(": %x", dst);
  return dst;
}

/* add_foreign_grouping_paths
 * Add foreign path for grouping and/or aggregation.
 * Given input_rel represents the underlying scan.  The paths are added to the given grouped_rel.
 */
static void add_foreign_grouping_paths(PlannerInfo *root, RelOptInfo *input_rel, RelOptInfo *grouped_rel, GroupPathExtraData *extra) {
  Query*       parse   = root->parse;
  DB2FdwState* ifpinfo = (DB2FdwState*)input_rel->fdw_private;
  DB2FdwState* fpinfo  = (DB2FdwState*)grouped_rel->fdw_private;
  ForeignPath* grouppath;
  double       rows;
  int          width;
  int          disabled_nodes;
  Cost         startup_cost;
  Cost         total_cost;

  db2Entry4();
  /* Nothing to be done, if there is no grouping or aggregation required. */
  if (!parse->groupClause && !parse->groupingSets && !parse->hasAggs &&	!root->hasHavingQual) {
    db2Debug5("Nothing to be done, if there is no grouping or aggregation required");
  } else {
    Assert(extra->patype == PARTITIONWISE_AGGREGATE_NONE || extra->patype == PARTITIONWISE_AGGREGATE_FULL);
    /* save the input_rel as outerrel in fpinfo */
    fpinfo->outerrel = input_rel;

    // All of this is redundant since fpinfo is 1:1 clone of ifpinfo
    // Copy foreign table, foreign server, user mapping, FDW options etc. details from the input relation's fpinfo.
    fpinfo->ftable    = ifpinfo->ftable;
    fpinfo->fserver   = ifpinfo->fserver;
    fpinfo->fuser     = ifpinfo->fuser;
    //fpinfo->db2Table = db2CloneDb2TableForPlan(ifpinfo->db2Table);
    //fpinfo->dbserver = db2strdup(ifpinfo->dbserver);
    //fpinfo->user     = db2strdup(ifpinfo->user);
    merge_fdw_options(fpinfo, ifpinfo, NULL);

    /* Assess if it is safe to push down aggregation and grouping.
     * Use HAVING qual from extra. In case of child partition, it will have translated Vars.
     */
    if (!foreign_grouping_ok(root, grouped_rel, extra->havingQual)) {
      db2Debug5("foreigm grouping is not ok");
    } else {
      /* Compute the selectivity and cost of the local_conds, so we don't have to do it over again for each path.
      * (Currently we create just a single path here, but in future it would be possible that we build more paths
      * such as pre-sorted paths as in postgresGetForeignPaths and postgresGetForeignJoinPaths.)
      * The best we can do for these conditions is to estimate selectivity on the basis of local statistics.
      */
      fpinfo->local_conds_sel = clauselist_selectivity(root, fpinfo->local_conds, 0, JOIN_INNER, NULL);
      cost_qual_eval(&fpinfo->local_conds_cost, fpinfo->local_conds, root);

      /* Estimate the cost of push down */
      estimate_path_cost_size(root, grouped_rel, NIL, NIL, NULL, &rows, &width, &disabled_nodes, &startup_cost, &total_cost);
      /* Now update this information in the fpinfo */
      fpinfo->rows           = rows;
      fpinfo->width          = width;
      fpinfo->disabled_nodes = disabled_nodes;
      fpinfo->startup_cost   = startup_cost;
      fpinfo->total_cost     = total_cost;
        /* Create and add foreign path to the grouping relation. */
      grouppath = create_foreign_upper_path( root
                                          , grouped_rel
                                          , grouped_rel->reltarget
                                          , rows
      #if PG_VERSION_NUM >= 180000
                                          , disabled_nodes
      #endif
                                          , startup_cost
                                          , total_cost
                                          , NIL                    /* no pathkeys */
                                          , NULL
      #if PG_VERSION_NUM >= 170000
                                          , NIL                    /* no fdw_restrictinfo list */
      #endif
                                          , NIL);                  /* no fdw_private */
      /* Add generated path into grouped_rel by add_path(). */
      add_path(grouped_rel, (Path*) grouppath);
    }
  }
  db2Exit4();
}

/* add_foreign_ordered_paths
 * Add foreign paths for performing the final sort remotely.
 * Given input_rel contains the source-data Paths.
 * The paths are added to the given ordered_rel.
 */
static void add_foreign_ordered_paths(PlannerInfo *root, RelOptInfo *input_rel, RelOptInfo *ordered_rel) {
  Query*               parse   = root->parse;
  DB2FdwState*         ifpinfo = (DB2FdwState*)input_rel->fdw_private;
  DB2FdwState*         fpinfo  = (DB2FdwState*)ordered_rel->fdw_private;
  DB2FdwPathExtraData* fpextra;
  double               rows;
  int                  width;
  int                  disabled_nodes;
  Cost                 startup_cost;
  Cost                 total_cost;
  List*                fdw_private;
  ForeignPath*         ordered_path;
  ListCell*            lc;

  db2Entry4();

  // Shouldn't get here unless the query has ORDER BY
  Assert(parse->sortClause);

  // We don't support cases where there are any SRFs in the targetlist
  if (parse->hasTargetSRFs) {
    db2Debug5("no support where there are any SRFs in the targetlist");
  } else {
    bool isSafe = true;

    // Save the input_rel as outerrel in fpinfo
    fpinfo->outerrel = input_rel;

    // Copy foreign table, foreign server, user mapping, FDW options etc. details from the input relation's fpinfo.
    fpinfo->ftable  = ifpinfo->ftable;
    fpinfo->fserver = ifpinfo->fserver;
    fpinfo->fuser   = ifpinfo->fuser;
    merge_fdw_options(fpinfo, ifpinfo, NULL);

    /* If the input_rel is a base or join relation, we would already have considered pushing down the final sort to the remote server when
     * creating pre-sorted foreign paths for that relation, because the query_pathkeys is set to the root->sort_pathkeys in that case (see
     * standard_qp_callback()).
     */
    if (input_rel->reloptkind == RELOPT_BASEREL || input_rel->reloptkind == RELOPT_JOINREL) {
      Assert(root->query_pathkeys == root->sort_pathkeys);
      /* Safe to push down if the query_pathkeys is safe to push down */
      fpinfo->pushdown_safe = ifpinfo->qp_is_pushdown_safe;
      db2Debug5("query_pathkeys is safe to push down");
    } else {
      // The input_rel should be a grouping relation
      Assert(input_rel->reloptkind == RELOPT_UPPER_REL && ifpinfo->stage == UPPERREL_GROUP_AGG);
      /* We try to create a path below by extending a simple foreign path for the underlying grouping relation to perform the final sort remotely,
      * which is stored into the fdw_private list of the resulting path.
      */
      foreach(lc, root->sort_pathkeys) {
        PathKey*          pathkey     = (PathKey*) lfirst(lc);
        EquivalenceClass* pathkey_ec  = pathkey->pk_eclass;
        /* is_foreign_expr would detect volatile expressions as well, but checking ec_has_volatile here saves some cycles. */
        if (pathkey_ec->ec_has_volatile) {
          db2Debug5("ec_has_volatile is true");
          isSafe = false;
          break;
        }
        /* Can't push down the sort if pathkey's opfamily is not shippable. */
        if (!is_shippable(pathkey->pk_opfamily, OperatorFamilyRelationId, fpinfo)) {
          db2Debug5("pathkey's opfamily is not shippable");
          isSafe = false;
          break;
        }
        /* The EC must contain a shippable EM that is computed in input_rel's reltarget, else we can't push down the sort. */
        if (find_em_for_rel_target(root, pathkey_ec, input_rel) == NULL) {
          db2Debug5("non shippable EM that is computed in input_rel's reltarget");
          isSafe = false;
          break;
        }
      }
      if (isSafe) {
        /* Safe to push down */
        fpinfo->pushdown_safe = true;
        /* Construct PgFdwPathExtraData */
        fpextra                 = db2alloc(sizeof(DB2FdwPathExtraData),"fpextra");
        fpextra->target         = root->upper_targets[UPPERREL_ORDERED];
        fpextra->has_final_sort = true;
        /* Estimate the costs of performing the final sort remotely */
        estimate_path_cost_size(root, input_rel, NIL, root->sort_pathkeys, fpextra, &rows, &width, &disabled_nodes,	&startup_cost, &total_cost);
        /*
        * Build the fdw_private list that will be used by postgresGetForeignPlan.
        * Items in the list must match order in enum FdwPathPrivateIndex.
        */
        #if PG_VERSION_NUM < 150000
        fdw_private = list_make2(makeInteger(true), makeInteger(false));
        #else
        fdw_private = list_make2(makeBoolean(true), makeBoolean(false));
        #endif
        /* Create foreign ordering path */
        ordered_path = create_foreign_upper_path( root
                                                , input_rel
                                                , root->upper_targets[UPPERREL_ORDERED]
                                                , rows
        #if PG_VERSION_NUM >= 180000
                                                , disabled_nodes
        #endif
                                                , startup_cost
                                                , total_cost
                                                , root->sort_pathkeys
                                                , NULL                   /* no extra plan */
        #if PG_VERSION_NUM >= 170000
                                                , NIL                    /* no fdw_restrictinfo list */
        #endif
                                                , fdw_private);
        /* and add it to the ordered_rel */
        add_path(ordered_rel, (Path *) ordered_path);
      }
    }
  }
  db2Exit4();
}

/* add_foreign_final_paths
 * Add foreign paths for performing the final processing remotely.
 * Given input_rel contains the source-data Paths.
 * The paths are added to the given final_rel.
 */
static void add_foreign_final_paths(PlannerInfo *root, RelOptInfo *input_rel, RelOptInfo *final_rel, FinalPathExtraData *extra) {
  Query*               parse                    = root->parse;
  DB2FdwState*         ifpinfo                  = (DB2FdwState *) input_rel->fdw_private;
  DB2FdwState*         fpinfo                   = (DB2FdwState *) final_rel->fdw_private;
  bool                 has_final_sort           = false;
  List*                pathkeys                 = NIL;
  DB2FdwPathExtraData* fpextra                  = NULL;
  bool                 save_use_remote_estimate = false;
  double               rows                     = 0;
  int                  width                    = 0;
  int                  disabled_nodes           = 0;
  Cost                 startup_cost             ;
  Cost                 total_cost               ;
  List*                fdw_private              = NIL;
  ForeignPath*         final_path               = NULL;

  db2Entry4();

  /** Currently, we only support this for SELECT commands */
  if (parse->commandType != CMD_SELECT) {
    db2Debug5("only support SELECT command");
    db2Exit4();
    return;
  }

  // No work if there is no FOR UPDATE/SHARE clause and if there is no need to add a LIMIT node
  if (!parse->rowMarks && !extra->limit_needed) {
    db2Debug5("no FOR UPDATE/SHARE clause and no need to add a LIMIT node");
    db2Exit4();
    return;
  }

  // We don't support cases where there are any SRFs in the targetlist
  if (parse->hasTargetSRFs) {
    db2Debug5("no support for any SRFs in the targetlist");
    db2Exit4();
    return;
  }

  /* Save the input_rel as outerrel in fpinfo */
  fpinfo->outerrel = input_rel;

  // Copy foreign table, foreign server, user mapping, FDW options etc. details from the input relation's fpinfo.
  fpinfo->ftable  = ifpinfo->ftable;
  fpinfo->fserver = ifpinfo->fserver;
  fpinfo->fuser   = ifpinfo->fuser;
  merge_fdw_options(fpinfo, ifpinfo, NULL);

  /* If there is no need to add a LIMIT node, there might be a ForeignPath in the input_rel's pathlist that implements all behavior of the query.
   * Note: we would already have accounted for the query's FOR UPDATE/SHARE (if any) before we get here.
   */
  if (!extra->limit_needed) {
    ListCell   *lc;

    Assert(parse->rowMarks);

    /* Grouping and aggregation are not supported with FOR UPDATE/SHARE, so the input_rel should be a base, join, or ordered relation; and
     * if it's an ordered relation, its input relation should be a base or join relation.
     */
    Assert(input_rel->reloptkind == RELOPT_BASEREL || input_rel->reloptkind == RELOPT_JOINREL  || (input_rel->reloptkind == RELOPT_UPPER_REL && ifpinfo->stage == UPPERREL_ORDERED && (ifpinfo->outerrel->reloptkind == RELOPT_BASEREL || ifpinfo->outerrel->reloptkind == RELOPT_JOINREL)));

    foreach(lc, input_rel->pathlist) {
      Path* path = (Path*) lfirst(lc);

      /* apply_scanjoin_target_to_paths() uses create_projection_path() to adjust each of its input paths if needed, whereas
       * create_ordered_paths() uses apply_projection_to_path() to do that.
       * So the former might have put a ProjectionPath on top of the ForeignPath; look through ProjectionPath and see if the
       * path underneath it is ForeignPath.
       */
      if (IsA(path, ForeignPath) || (IsA(path, ProjectionPath) && IsA(((ProjectionPath *) path)->subpath, ForeignPath))) {
        //Create foreign final path; this gets rid of a no-longer-needed outer plan (if any), which makes the EXPLAIN output look cleaner
        final_path = create_foreign_upper_path( root
                                              , path->parent
                                              , path->pathtarget
                                              , path->rows
        #if PG_VERSION_NUM >= 180000
                                              , path->disabled_nodes
        #endif
                                              , path->startup_cost
                                              , path->total_cost
                                              , path->pathkeys
                                              , NULL                     /* no extra plan */
        #if PG_VERSION_NUM >= 170000
                                              , NIL                      /* no fdw_restrictinfo list */
        #endif
                                              , NIL);                    /* no fdw_private */

        /* and add it to the final_rel */
        add_path(final_rel, (Path *) final_path);

        /* Safe to push down */
        fpinfo->pushdown_safe = true;
        db2Debug5("created foreign final path; this gets rid of a no-longer-needed outer plan (if any), which makes the EXPLAIN output look cleaner");
        db2Exit4();
        return;
      }
    }

    /* If we get here it means no ForeignPaths; since we would already have considered pushing down all operations for the query to the
     * remote server, give up on it.
     */
    db2Debug5("no ForeignPaths; since we would already have considered pushing down all operations for the query to the remote server, give up on it");
    db2Exit4();
    return;
  }

  Assert(extra->limit_needed);

  // If the input_rel is an ordered relation, replace the input_rel with its input relation
  if (input_rel->reloptkind == RELOPT_UPPER_REL && ifpinfo->stage == UPPERREL_ORDERED) {
    input_rel = ifpinfo->outerrel;
    ifpinfo = (DB2FdwState*) input_rel->fdw_private;
    has_final_sort = true;
    pathkeys = root->sort_pathkeys;
  }

  /* The input_rel should be a base, join, or grouping relation */
  Assert(input_rel->reloptkind == RELOPT_BASEREL || input_rel->reloptkind == RELOPT_JOINREL || (input_rel->reloptkind == RELOPT_UPPER_REL && ifpinfo->stage == UPPERREL_GROUP_AGG));

  /* We try to create a path below by extending a simple foreign path for the underlying base, join, or grouping relation to perform the final
   * sort (if has_final_sort) and the LIMIT restriction remotely, which is stored into the fdw_private list of the resulting path.
   * (We re-estimate the costs of sorting the underlying relation, if has_final_sort.)
   */

  /* Assess if it is safe to push down the LIMIT and OFFSET to the remote server */

  /* If the underlying relation has any local conditions, the LIMIT/OFFSET cannot be pushed down. */
  if (ifpinfo->local_conds) {
    db2Debug5("the underlying relation has any local conditions, the LIMIT/OFFSET cannot be pushed down");
    db2Exit4();
    return;
  }

  /* If the query has FETCH FIRST .. WITH TIES, 
   * 1) it must have ORDER BY as well, which is used to determine which additional rows tie for the last place in the result set, and 
   * 2) ORDER BY must already have been determined to be safe to push down before we get here.
   * So in that case the FETCH clause is safe to push down with ORDER BY if the remote server is v13 or later, but if not, the remote query will fail
   * entirely for lack of support for it.
   * Since we do not currently have a way to do a remote-version check (without accessing the remote server), disable pushing the FETCH clause for now.
   */
  if (parse->limitOption == LIMIT_OPTION_WITH_TIES) {
    db2Debug5("the query has FETCH FIRST .. WITH TIES without ORDER BY");
    db2Exit4();
    return;
  }
  /* Also, the LIMIT/OFFSET cannot be pushed down, if their expressions are not safe to remote. */
  if (!is_foreign_expr(root, input_rel, (Expr *) parse->limitOffset) || !is_foreign_expr(root, input_rel, (Expr *) parse->limitCount)) {
    db2Debug5("the LIMIT/OFFSET cannot be pushed down, if their expressions are not safe to remote");
    db2Exit4();
    return;
  }

  /* Safe to push down */
  fpinfo->pushdown_safe = true;

  /* Construct DB2FdwPathExtraData */
  fpextra                 = db2alloc(sizeof(DB2FdwPathExtraData),"fpextra");
  fpextra->target         = root->upper_targets[UPPERREL_FINAL];
  fpextra->has_final_sort = has_final_sort;
  fpextra->has_limit      = extra->limit_needed;
  fpextra->limit_tuples   = extra->limit_tuples;
  fpextra->count_est      = extra->count_est;
  fpextra->offset_est     = extra->offset_est;

  /* Estimate the costs of performing the final sort and the LIMIT restriction remotely.
   * If has_final_sort is false, we wouldn't need to execute EXPLAIN anymore if use_remote_estimate, since the costs can be
   * roughly estimated using the costs we already have for the underlying relation, in the same way as when use_remote_estimate is false.
   * Since it's pretty expensive to execute EXPLAIN, force use_remote_estimate to false in that case.
   */
  if (!fpextra->has_final_sort) {
    save_use_remote_estimate = ifpinfo->use_remote_estimate;
    ifpinfo->use_remote_estimate = false;
  }
  estimate_path_cost_size(root, input_rel, NIL, pathkeys, fpextra, &rows, &width, &disabled_nodes, &startup_cost, &total_cost);
  if (!fpextra->has_final_sort)
    ifpinfo->use_remote_estimate = save_use_remote_estimate;

  /* Build the fdw_private list that will be used by postgresGetForeignPlan. Items in the list must match order in enum FdwPathPrivateIndex. */
  #if PG_VERSION_NUM < 150000
  fdw_private = list_make2(makeInteger(has_final_sort), makeInteger(extra->limit_needed));
  #else
  fdw_private = list_make2(makeBoolean(has_final_sort), makeBoolean(extra->limit_needed));
  #endif

  /* Create foreign final path; this gets rid of a no-longer-needed outer plan (if any), which makes the EXPLAIN output look cleaner */
  final_path = create_foreign_upper_path( root
                                        , input_rel
                                        , root->upper_targets[UPPERREL_FINAL]
                                        , rows
  #if PG_VERSION_NUM >= 180000
                                        , disabled_nodes
  #endif
                                        , startup_cost
                                        , total_cost
                                        , pathkeys
                                        , NULL                                  /* no extra plan */
  #if PG_VERSION_NUM >= 170000
                                        , NIL                                   /* no fdw_restrictinfo list */
  #endif
                                        , fdw_private);

  /* and add it to the final_rel */
  add_path(final_rel, (Path*) final_path);
  db2Exit4();
}

/* Adjust the cost estimates of a foreign grouping path to include the cost of generating properly-sorted output. */
static void adjust_foreign_grouping_path_cost(PlannerInfo* root, List* pathkeys, double retrieved_rows, double width, double limit_tuples, int* p_disabled_nodes, Cost* p_startup_cost, Cost* p_run_cost) {
  db2Entry4();
  /* If the GROUP BY clause isn't sort-able, the plan chosen by the remote side is unlikely to generate properly-sorted output, so it would need
   * an explicit sort; adjust the given costs with cost_sort().
   * Likewise, if the GROUP BY clause is sort-able but isn't a superset of the given pathkeys, adjust the costs with that function.
   * Otherwise, adjust the costs by applying the same heuristic as for the scan or join case.
   */
  #if PG_VERSION_NUM < 160000
  if (!grouping_is_sortable(root->parse->groupClause) || !pathkeys_contained_in(pathkeys, root->group_pathkeys)) {
  #else
  if (!grouping_is_sortable(root->processed_groupClause) || !pathkeys_contained_in(pathkeys, root->group_pathkeys)) {
  #endif
    Path  sort_path;  /* dummy for result of cost_sort */

    cost_sort( &sort_path
             , root
             , pathkeys
    #if PG_VERSION_NUM >= 180000
             , 0
    #endif
             , *p_startup_cost + *p_run_cost
             , retrieved_rows
             , width
             , 0.0
             , work_mem
             , limit_tuples
             );

    *p_startup_cost = sort_path.startup_cost;
    *p_run_cost = sort_path.total_cost - sort_path.startup_cost;
  } else {
    /* The default extra cost seems too large for foreign-grouping cases; add 1/4th of that default. */
    double  sort_multiplier = 1.0 + (DEFAULT_FDW_SORT_MULTIPLIER - 1.0) * 0.25;

    *p_startup_cost *= sort_multiplier;
    *p_run_cost     *= sort_multiplier;
  }
  db2Exit4();
}

/* Assess whether the aggregation, grouping and having operations can be pushed down to the foreign server.
 * As a side effect, save information we obtain in this function to PgFdwRelationInfo of the input relation.
 */
static bool foreign_grouping_ok(PlannerInfo* root, RelOptInfo* grouped_rel, Node* havingQual) {
  Query*       query           = root->parse;
  DB2FdwState* fpinfo          = (DB2FdwState*) grouped_rel->fdw_private;
  PathTarget*  grouping_target = grouped_rel->reltarget;
  DB2FdwState* ofpinfo         = NULL;
  ListCell*    lc              = NULL;
  int          i               = 0;
  List*        tlist           = NIL;

  db2Entry4();
  /* We currently don't support pushing Grouping Sets. */
  if (query->groupingSets) {
    db2Debug5("no support pushing Grouping Sets");
    db2Exit4();
    return false;
  }

  /* Get the fpinfo of the underlying scan relation. */
  ofpinfo = (DB2FdwState*) fpinfo->outerrel->fdw_private;

  /* If underlying scan relation has any local conditions, those conditions are required to be applied before performing aggregation.
   * Hence the aggregate cannot be pushed down.
   */
  if (ofpinfo->local_conds) {
    db2Debug5("foreign_grouping_ok: local_conds found");
    db2Exit4(": false");
    return false;
  }

  /* Examine grouping expressions, as well as other expressions we'd need to compute, and check whether they are safe to push down to the foreign server.
   * All GROUP BY expressions will be part of the grouping target and thus there is no need to search for them separately.
   * Add grouping expressions into target list which will be passed to foreign server.
   *
   * A tricky fine point is that we must not put any expression into the target list that is just a foreign param
   * (that is, something that deparse.c would conclude has to be sent to the foreign server).
   * If we do, the expression will also appear in the fdw_exprs list of the plan node, and setrefs.c will get confused and decide that the fdw_exprs
   * entry is actually a reference to the fdw_scan_tlist entry, resulting in a broken plan.
   * Somewhat oddly, it's OK if the expression contains such a node, as long as it's not at top level; then no match is possible.
   */
  i = 0;
  foreach(lc, grouping_target->exprs) {
    Expr*     expr = (Expr *) lfirst(lc);
    Index     sgref = get_pathtarget_sortgroupref(grouping_target, i);
    ListCell* l;

    /* Check whether this expression is part of GROUP BY clause.
     * Note we check the whole GROUP BY clause not just processed_groupClause, because we will ship all of it, cf. appendGroupByClause.
     */
    if (sgref && get_sortgroupref_clause_noerr(sgref, query->groupClause)) {
      TargetEntry *tle;

      /* If any GROUP BY expression is not shippable, then we cannot push down aggregation to the foreign server. */
      if (!is_foreign_expr(root, grouped_rel, expr)) {
        db2Debug5("foreign_grouping_ok: non-foreign expr found");
        db2Exit4(": false");
        return false;
      }

      /* If it would be a foreign param, we can't put it into the tlist, so we have to fail. */
      if (is_foreign_param(root, grouped_rel, expr)) {
        db2Debug5("foreign_grouping_ok: foreign param found");
        db2Exit4(": false");
        return false;
      }

      /* Pushable, so add to tlist.
       * We need to create a TLE for this expression and apply the sortgroupref to it.
       * We cannot use add_to_flat_tlist() here because that avoids making duplicate entries in the tlist.
       * If there are duplicate entries with distinct sortgrouprefs, we have to duplicate that situation in the output tlist.
       */
      tle = makeTargetEntry(expr, list_length(tlist) + 1, NULL, false);
      tle->ressortgroupref = sgref;
      tlist = lappend(tlist, tle);
    } else {
      /* Non-grouping expression we need to compute.
       * Can we ship it as-is to the foreign server?
       */
      if (is_foreign_expr(root, grouped_rel, expr) && !is_foreign_param(root, grouped_rel, expr)) {
        /* Yes, so add to tlist as-is; OK to suppress duplicates */
        tlist = add_to_flat_tlist(tlist, list_make1(expr));
      } else 	{
        /* Not pushable as a whole; extract its Vars and aggregates */
        List* aggvars = pull_var_clause((Node*) expr, PVC_INCLUDE_AGGREGATES);

        /* If any aggregate expression is not shippable, then we cannot push down aggregation to the foreign server.
         * (We don't have to check is_foreign_param, since that certainly won't return true for any such expression.)
         */
        if (!is_foreign_expr(root, grouped_rel, (Expr *) aggvars)) {
          db2Debug5("foreign_grouping_ok: non-foreign aggvar found");
          db2Exit4(": false");
          return false;
        }

        /* Add aggregates, if any, into the targetlist.
         * Plain Vars outside an aggregate can be ignored, because they should be either same as some GROUP BY column or part of some GROUP BY expression.
         * In either case, they are already part of the targetlist and thus no need to add them again.
         * In fact including plain Vars in the tlist when they do not match a GROUP BY column would cause the foreign server to complain that the shipped query is invalid.
         */
        foreach(l, aggvars) {
          Expr* aggref = (Expr *) lfirst(l);

          if (IsA(aggref, Aggref))
            tlist = add_to_flat_tlist(tlist, list_make1(aggref));
        }
      }
    }
    i++;
  }

  /* Classify the pushable and non-pushable HAVING clauses and save them in remote_conds and local_conds of the grouped rel's fpinfo. */
  if (havingQual) {
    foreach(lc, (List *) havingQual) {
      Expr	   *expr = (Expr *) lfirst(lc);
      RestrictInfo *rinfo;

      /* Currently, the core code doesn't wrap havingQuals in RestrictInfos, so we must make our own. */
      Assert(!IsA(expr, RestrictInfo));
      #if PG_VERSION_NUM < 160000
      rinfo = make_restrictinfo(root, expr, true, false, false,        root->qual_security_level, grouped_rel->relids, NULL, NULL);
      #else
      rinfo = make_restrictinfo(root, expr, true, false, false, false, root->qual_security_level, grouped_rel->relids, NULL, NULL);
      #endif
      if (is_foreign_expr(root, grouped_rel, expr))
        fpinfo->remote_conds = lappend(fpinfo->remote_conds, rinfo);
      else
        fpinfo->local_conds = lappend(fpinfo->local_conds, rinfo);
    }
  }

  /* If there are any local conditions, pull Vars and aggregates from it and check whether they are safe to pushdown or not. */
  if (fpinfo->local_conds) {
    List* aggvars = NIL;

    foreach(lc, fpinfo->local_conds) {
      RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

      aggvars = list_concat(aggvars, pull_var_clause((Node*) rinfo->clause, PVC_INCLUDE_AGGREGATES));
    }

    foreach(lc, aggvars) {
      Expr	   *expr = (Expr *) lfirst(lc);

      /* If aggregates within local conditions are not safe to push down, then we cannot push down the query.
       * Vars are already part of GROUP BY clause which are checked above, so no need to access them again here.
       * Again, we need not check is_foreign_param for a foreign aggregate.
       */
      if (IsA(expr, Aggref)) {
        if (!is_foreign_expr(root, grouped_rel, expr)) {
          db2Exit4(": false");
          return false;
        }
        tlist = add_to_flat_tlist(tlist, list_make1(expr));
      }
    }
  }

  /* Store generated targetlist */
  fpinfo->grouped_tlist = tlist;

  /* Safe to pushdown */
  fpinfo->pushdown_safe = true;

  /* Set # of retrieved rows and cached relation costs to some negative value, so that we can detect when they are set to some sensible values,
   * during one (usually the first) of the calls to estimate_path_cost_size.
   */
  fpinfo->retrieved_rows = -1;
  fpinfo->rel_startup_cost = -1;
  fpinfo->rel_total_cost = -1;

  /* Set the string describing this grouped relation to be used in EXPLAIN output of corresponding ForeignScan.
   * Note that the decoration we add to the base relation name mustn't include any digits, or it'll confuse postgresExplainForeignScan.
   */
  fpinfo->relation_name = psprintf("Aggregate on (%s)", ofpinfo->relation_name);
  db2Exit4(": true");
  return true;
}

/* estimate_path_cost_size
 * Get cost and size estimates for a foreign scan on given foreign relation either a base relation or a join between foreign relations or an upper
 * relation containing foreign relations.
 *
 * param_join_conds  are the parameterization clauses with outer relations.
 * pathkeys          specify the expected sort order if any for given path being costed.
 * fpextra           specifies additional post-scan/join-processing steps such as the final sort and the LIMIT restriction.
 *
 * The function returns the cost and size estimates in p_rows, p_width, p_disabled_nodes, p_startup_cost and p_total_cost variables.
 */
void estimate_path_cost_size(PlannerInfo* root, RelOptInfo* foreignrel, List* param_join_conds, List* pathkeys, DB2FdwPathExtraData* fpextra, double* p_rows, int* p_width, int* p_disabled_nodes, Cost* p_startup_cost, Cost* p_total_cost) {
  DB2FdwState*  fpinfo          = (DB2FdwState*) foreignrel->fdw_private;
  double        rows            = 0;
  double        retrieved_rows  = 0;
  int           width           = 0;
  int           disabled_nodes  = 0;
  Cost          startup_cost;
  Cost          total_cost;

  db2Entry4();
  /* Make sure the core code has set up the relation's reltarget */
  Assert(foreignrel->reltarget);

  /* If the table or the server is configured to use remote estimates, connect to the foreign server and execute EXPLAIN to estimate the
   * number of rows selected by the restriction+join clauses.
   * Otherwise, estimate rows using whatever statistics we have locally, in a way similar to ordinary tables.
   */
  if (fpinfo->use_remote_estimate) {
    List*           remote_param_join_conds;
    List*           local_param_join_conds;
    StringInfoData  sql;
//    PGconn*         conn;
    Selectivity     local_sel;
    QualCost        local_cost;
    List*           fdw_scan_tlist = NIL;
    List*           remote_conds;
    List*           retrieved_attrs;              /* Required only to be passed to deparseSelectStmtForRel */
    /* param_join_conds might contain both clauses that are safe to send across, and clauses that aren't. */
    classifyConditions(root, foreignrel, param_join_conds, &remote_param_join_conds, &local_param_join_conds);

    /* Build the list of columns to be fetched from the foreign server. */
    fdw_scan_tlist = (IS_JOIN_REL(foreignrel) || IS_UPPER_REL(foreignrel)) ? build_tlist_to_deparse(foreignrel) : NIL;

    /* The complete list of remote conditions includes everything from baserestrictinfo plus any extra join_conds relevant to this
     * particular path.
     */
    remote_conds = list_concat(remote_param_join_conds, fpinfo->remote_conds);

    /* Construct EXPLAIN query including the desired SELECT, FROM, and WHERE clauses. Params and other-relation Vars are replaced by dummy
     * values, so don't request params_list.
     */
    initStringInfo(&sql);
    appendStringInfoString(&sql, "EXPLAIN ");
    deparseSelectStmtForRel( &sql, root, foreignrel, fdw_scan_tlist, remote_conds, pathkeys
                           , fpextra ? fpextra->has_final_sort : false
                           , fpextra ? fpextra->has_limit      : false
                           , false, &retrieved_attrs, NULL);

    /* Get the remote estimate */
//    conn = GetConnection(fpinfo->user, false, NULL);
//    get_remote_estimate(sql.data, conn, &rows, &width, &startup_cost, &total_cost);
//    ReleaseConnection(conn);
    retrieved_rows = rows;

    /* Factor in the selectivity of the locally-checked quals */
    local_sel  = clauselist_selectivity(root, local_param_join_conds, foreignrel->relid, JOIN_INNER, NULL);
    local_sel *= fpinfo->local_conds_sel;
    rows       = clamp_row_est(rows* local_sel);

    /* Add in the eval cost of the locally-checked quals */
    startup_cost += fpinfo->local_conds_cost.startup;
    total_cost   += fpinfo->local_conds_cost.per_tuple * retrieved_rows;
    cost_qual_eval(&local_cost, local_param_join_conds, root);
    startup_cost += local_cost.startup;
    total_cost   += local_cost.per_tuple * retrieved_rows;

    /* Add in tlist eval cost for each output row.  In case of an aggregate, some of the tlist expressions such as grouping
     * expressions will be evaluated remotely, so adjust the costs.
     */
    startup_cost += foreignrel->reltarget->cost.startup;
    total_cost   += foreignrel->reltarget->cost.startup;
    total_cost   += foreignrel->reltarget->cost.per_tuple * rows;
    if (IS_UPPER_REL(foreignrel)) {
      QualCost	tlist_cost;

      cost_qual_eval(&tlist_cost, fdw_scan_tlist, root);

      startup_cost -= tlist_cost.startup;
      total_cost   -= tlist_cost.startup;
      total_cost   -= tlist_cost.per_tuple * rows;
    }
  } else {
    Cost    run_cost = 0;

    /* We don't support join conditions in this mode (hence, no parameterized paths can be made). */
    Assert(param_join_conds == NIL);
    /* We will come here again and again with different set of pathkeys or additional post-scan/join-processing steps that caller wants to
     * cost.  We don't need to calculate the cost/size estimates for the underlying scan, join, or grouping each time.
     * Instead, use those estimates if we have cached them already.
     */
    if (fpinfo->rel_startup_cost >= 0 && fpinfo->rel_total_cost >= 0) {
      Assert(fpinfo->retrieved_rows >= 0);

      rows           = fpinfo->rows;
      retrieved_rows = fpinfo->retrieved_rows;
      width          = fpinfo->width;
      startup_cost   = fpinfo->rel_startup_cost;
      run_cost       = fpinfo->rel_total_cost - fpinfo->rel_startup_cost;

      /* If we estimate the costs of a foreign scan or a foreign join with additional post-scan/join-processing steps, the scan or
       * join costs obtained from the cache wouldn't yet contain the eval costs for the final scan/join target, which would've been
       * updated by apply_scanjoin_target_to_paths(); add the eval costs now.
       */
      if (fpextra && !IS_UPPER_REL(foreignrel)) {
        /* Shouldn't get here unless we have LIMIT */
        Assert(fpextra->has_limit);
        Assert(foreignrel->reloptkind == RELOPT_BASEREL || foreignrel->reloptkind == RELOPT_JOINREL);
        startup_cost += foreignrel->reltarget->cost.startup;
        run_cost += foreignrel->reltarget->cost.per_tuple * rows;
      }
    } else if (IS_JOIN_REL(foreignrel)) {
      DB2FdwState*  fpinfo_i;
      DB2FdwState*  fpinfo_o;
      QualCost      join_cost;
      QualCost      remote_conds_cost;
      double        nrows;

      /* Use rows/width estimates made by the core code. */
      rows  = foreignrel->rows;
      width = foreignrel->reltarget->width;

      /* For join we expect inner and outer relations set */
      Assert(fpinfo->innerrel && fpinfo->outerrel);

      fpinfo_i = (DB2FdwState*) fpinfo->innerrel->fdw_private;
      fpinfo_o = (DB2FdwState*) fpinfo->outerrel->fdw_private;

      /* Estimate of number of rows in cross product */
      nrows = fpinfo_i->rows * fpinfo_o->rows;

      /* Back into an estimate of the number of retrieved rows.  Just in case this is nuts, clamp to at most nrows. */
      retrieved_rows = clamp_row_est(rows / fpinfo->local_conds_sel);
      retrieved_rows = Min(retrieved_rows, nrows);

      /* The cost of foreign join is estimated as cost of generating rows for the joining relations + cost for applying quals on the rows. */
      /* Calculate the cost of clauses pushed down to the foreign server */
      cost_qual_eval(&remote_conds_cost, fpinfo->remote_conds, root);
      /* Calculate the cost of applying join clauses */
      cost_qual_eval(&join_cost, fpinfo->joinclauses, root);

      /* Startup cost includes startup cost of joining relations and the startup cost for join and other clauses. We do not include the
       * startup cost specific to join strategy (e.g. setting up hash tables) since we do not know what strategy the foreign server
       * is going to use.
       */
      startup_cost = fpinfo_i->rel_startup_cost + fpinfo_o->rel_startup_cost;
      startup_cost += join_cost.startup;
      startup_cost += remote_conds_cost.startup;
      startup_cost += fpinfo->local_conds_cost.startup;

      /* Run time cost includes:
       * 1. Run time cost (total_cost - startup_cost) of relations being joined
       * 2. Run time cost of applying join clauses on the cross product of the joining relations.
       * 3. Run time cost of applying pushed down other clauses on the result of join
       * 4. Run time cost of applying nonpushable other clauses locally on the result fetched from the foreign server.
       */
      run_cost  = fpinfo_i->rel_total_cost - fpinfo_i->rel_startup_cost;
      run_cost += fpinfo_o->rel_total_cost - fpinfo_o->rel_startup_cost;
      run_cost += nrows * join_cost.per_tuple;
      nrows     = clamp_row_est(nrows * fpinfo->joinclause_sel);
      run_cost += nrows * remote_conds_cost.per_tuple;
      run_cost += fpinfo->local_conds_cost.per_tuple * retrieved_rows;

      /* Add in tlist eval cost for each output row */
      startup_cost += foreignrel->reltarget->cost.startup;
      run_cost += foreignrel->reltarget->cost.per_tuple * rows;
    } else if (IS_UPPER_REL(foreignrel)) {
      RelOptInfo*     outerrel = fpinfo->outerrel;
      DB2FdwState*    ofpinfo;
      AggClauseCosts  aggcosts = {0};
      double          input_rows;
      int             numGroupCols;
      double          numGroups = 1;

      /* The upper relation should have its outer relation set */
      Assert(outerrel);
      /* and that outer relation should have its reltarget set */
      Assert(outerrel->reltarget);
      /* This cost model is mixture of costing done for sorted and hashed aggregates in cost_agg().  We are not sure which
       * strategy will be considered at remote side, thus for simplicity, we put all startup related costs in startup_cost
       * and all finalization and run cost are added in total_cost.
       */
      ofpinfo = (DB2FdwState*) outerrel->fdw_private;
      /* Get rows from input rel */
      input_rows = ofpinfo->rows;
      /* Collect statistics about aggregates for estimating costs. */
      #if PG_VERSION_NUM < 140000
      MemSet(&aggcosts, 0, sizeof(AggClauseCosts));
      if (root->parse->hasAggs) {
        get_agg_clause_costs(root, (Node *) fpinfo->grouped_tlist, AGGSPLIT_SIMPLE, &aggcosts);
        /* The cost of aggregates in the HAVING qual will be the same for each child as it is for the parent, so there's no need
         * to use a translated version of havingQual.
         */
        get_agg_clause_costs(root, (Node *) root->parse->havingQual, AGGSPLIT_SIMPLE, &aggcosts);
      }
      #else
      if (root->parse->hasAggs) {
        get_agg_clause_costs(root, AGGSPLIT_SIMPLE, &aggcosts);
      }
      #endif
      /* Get number of grouping columns and possible number of groups */
      #if PG_VERSION_NUM < 160000
      numGroupCols = list_length(root->parse->groupClause);
      #if PG_VERSION_NUM < 140000
      numGroups    = estimate_num_groups( root
                                        , get_sortgrouplist_exprs( root->parse->groupClause
                                                                 , fpinfo->grouped_tlist
                                                                 )
                                        , input_rows, NULL
                                        );
      #else
      numGroups    = estimate_num_groups( root
                                        , get_sortgrouplist_exprs( root->parse->groupClause
                                                                 , fpinfo->grouped_tlist
                                                                 )
                                        , input_rows, NULL, NULL
                                        );
      #endif
      #else
      numGroupCols = list_length(root->processed_groupClause);
      numGroups    = estimate_num_groups( root
                                        , get_sortgrouplist_exprs( root->processed_groupClause
                                                                 , fpinfo->grouped_tlist
                                                                 )
                                        , input_rows, NULL, NULL
                                        );
      #endif

      /* Get the retrieved_rows and rows estimates.  If there are HAVING quals, account for their selectivity. */
      if (root->hasHavingQual) {
        /* Factor in the selectivity of the remotely-checked quals */
        retrieved_rows = clamp_row_est( numGroups *  clauselist_selectivity( root
                                                                           , fpinfo->remote_conds
                                                                           , 0
                                                                           , JOIN_INNER
                                                                           , NULL
                                                                           )
                                      );
        /* Factor in the selectivity of the locally-checked quals */
        rows = clamp_row_est(retrieved_rows * fpinfo->local_conds_sel);
      } else {
        rows = retrieved_rows = numGroups;
      }

      /* Use width estimate made by the core code. */
      width = foreignrel->reltarget->width;

      /* Startup cost includes:
       *  1. Startup cost for underneath input relation, adjusted for tlist replacement by apply_scanjoin_target_to_paths()
       *  2. Cost of performing aggregation, per cost_agg()
       */
      startup_cost = ofpinfo->rel_startup_cost;
      startup_cost += outerrel->reltarget->cost.startup;
      startup_cost += aggcosts.transCost.startup;
      startup_cost += aggcosts.transCost.per_tuple * input_rows;
      startup_cost += aggcosts.finalCost.startup;
      startup_cost += (cpu_operator_cost * numGroupCols) * input_rows;

      /* Run time cost includes:
       *  1. Run time cost of underneath input relation, adjusted for tlist replacement by apply_scanjoin_target_to_paths()
       *  2. Run time cost of performing aggregation, per cost_agg()
       */
      run_cost = ofpinfo->rel_total_cost - ofpinfo->rel_startup_cost;
      run_cost += outerrel->reltarget->cost.per_tuple * input_rows;
      run_cost += aggcosts.finalCost.per_tuple * numGroups;
      run_cost += cpu_tuple_cost * numGroups;

      /* Account for the eval cost of HAVING quals, if any */
      if (root->hasHavingQual)
      {
        QualCost	remote_cost;

        /* Add in the eval cost of the remotely-checked quals */
        cost_qual_eval(&remote_cost, fpinfo->remote_conds, root);
        startup_cost += remote_cost.startup;
        run_cost += remote_cost.per_tuple * numGroups;
        /* Add in the eval cost of the locally-checked quals */
        startup_cost += fpinfo->local_conds_cost.startup;
        run_cost += fpinfo->local_conds_cost.per_tuple * retrieved_rows;
      }

      /* Add in tlist eval cost for each output row */
      startup_cost += foreignrel->reltarget->cost.startup;
      run_cost += foreignrel->reltarget->cost.per_tuple * rows;
    } else {
      Cost    cpu_per_tuple;

      /* Use rows/width estimates made by set_baserel_size_estimates. */
      rows  = foreignrel->rows;
      width = foreignrel->reltarget->width;

      /* Back into an estimate of the number of retrieved rows.  Just in case this is nuts, clamp to at most foreignrel->tuples. */
      retrieved_rows = clamp_row_est(rows / fpinfo->local_conds_sel);
      retrieved_rows = Min(retrieved_rows, foreignrel->tuples);

      /* Cost as though this were a seqscan, which is pessimistic.  We effectively imagine the local_conds are being evaluated remotely, too. */
      startup_cost = 0;
      run_cost     = 0;
      run_cost    += seq_page_cost * foreignrel->pages;

      startup_cost  += foreignrel->baserestrictcost.startup;
      cpu_per_tuple  = cpu_tuple_cost + foreignrel->baserestrictcost.per_tuple;
      run_cost      += cpu_per_tuple * foreignrel->tuples;

      /* Add in tlist eval cost for each output row */
      startup_cost += foreignrel->reltarget->cost.startup;
      run_cost += foreignrel->reltarget->cost.per_tuple * rows;
    }

    /* Without remote estimates, we have no real way to estimate the cost of generating sorted output.
     * It could be free if the query plan the remote side would have chosen generates properly-sorted output anyway, but in most cases it 
     * will cost something.
     * Estimate a value high enough that we won't pick the sorted path when the ordering isn't locally useful, but low enough that we'll 
     * err on the side of pushing down the ORDER BY clause when it's useful to do so.
     */
    if (pathkeys != NIL) {
      if (IS_UPPER_REL(foreignrel)) {
        Assert(foreignrel->reloptkind == RELOPT_UPPER_REL && fpinfo->stage == UPPERREL_GROUP_AGG);
        /* We can only get here when this function is called from add_foreign_ordered_paths() or add_foreign_final_paths();
         * in which cases, the passed-in fpextra should not be NULL.
         */
        Assert(fpextra);
        adjust_foreign_grouping_path_cost( root
                                         , pathkeys
                                         , retrieved_rows
                                         , width
                                         , fpextra->limit_tuples
                                         , &disabled_nodes
                                         , &startup_cost, &run_cost
                                         );
      } else {
        startup_cost *= DEFAULT_FDW_SORT_MULTIPLIER;
        run_cost     *= DEFAULT_FDW_SORT_MULTIPLIER;
      }
    }
    total_cost = startup_cost + run_cost;
    /* Adjust the cost estimates if we have LIMIT */
    if (fpextra && fpextra->has_limit) {
      adjust_limit_rows_costs(&rows, &startup_cost, &total_cost, fpextra->offset_est, fpextra->count_est);
      retrieved_rows = rows;
    }
  }

  /* If this includes the final sort step, the given target, which will be applied to the resulting path, might have different expressions from
   * the foreignrel's reltarget (see make_sort_input_target()); adjust tlist eval costs.
   */
  if (fpextra && fpextra->has_final_sort && fpextra->target != foreignrel->reltarget) {
    QualCost  oldcost = foreignrel->reltarget->cost;
    QualCost  newcost = fpextra->target->cost;

    startup_cost += newcost.startup    - oldcost.startup;
    total_cost   += newcost.startup    - oldcost.startup;
    total_cost   += (newcost.per_tuple - oldcost.per_tuple) * rows;
  }

  /* Cache the retrieved rows and cost estimates for scans, joins, or groupings without any parameterization, pathkeys, or additional
   * post-scan/join-processing steps, before adding the costs for transferring data from the foreign server.
   * These estimates are useful for costing remote joins involving this relation or costing other remote operations on this relation such as remote
   * sorts and remote LIMIT restrictions, when the costs can not be obtained from the foreign server.
   * This function will be called at least once for every foreign relation without any parameterization, pathkeys, or additional 
   * post-scan/join-processing steps.
   */
  if (pathkeys == NIL && param_join_conds == NIL && fpextra == NULL) {
    fpinfo->retrieved_rows    = retrieved_rows;
    fpinfo->rel_startup_cost  = startup_cost;
    fpinfo->rel_total_cost    = total_cost;
  }

  /* Add some additional cost factors to account for connection overhead (fdw_startup_cost), transferring data across the network
   * (fdw_tuple_cost per retrieved row), and local manipulation of the data (cpu_tuple_cost per retrieved row).
   */
  startup_cost += fpinfo->fdw_startup_cost;
  total_cost   += fpinfo->fdw_startup_cost;
  total_cost   += fpinfo->fdw_tuple_cost * retrieved_rows;
  total_cost   += cpu_tuple_cost * retrieved_rows;

  /* If we have LIMIT, we should prefer performing the restriction remotely rather than locally, as the former avoids extra row fetches from the
   * remote that the latter might cause.  But since the core code doesn't account for such fetches when estimating the costs of the local
   * restriction (see create_limit_path()), there would be no difference between the costs of the local restriction and the costs of the remote
   * restriction estimated above if we don't use remote estimates (except for the case where the foreignrel is a grouping relation, the given
   * pathkeys is not NIL, and the effects of a bounded sort for that rel is accounted for in costing the remote restriction).  Tweak the costs of
   * the remote restriction to ensure we'll prefer it if LIMIT is a useful one.
   */
  if (!fpinfo->use_remote_estimate && fpextra && fpextra->has_limit && fpextra->limit_tuples > 0 && fpextra->limit_tuples < fpinfo->rows) {
    Assert(fpinfo->rows > 0);
    total_cost -= (total_cost - startup_cost) * 0.05 * (fpinfo->rows - fpextra->limit_tuples) / fpinfo->rows;
  }

  /* Return results. */
  *p_rows           = rows;
  *p_width          = width;
  *p_disabled_nodes = disabled_nodes;
  *p_startup_cost   = startup_cost;
  *p_total_cost     = total_cost;
  db2Exit4();
}

/* Merge FDW options from input relations into a new set of options for a join or an upper rel.
 *
 * For a join relation, FDW-specific information about the inner and outer relations is provided using fpinfo_i and fpinfo_o.
 * For an upper relation, fpinfo_o provides the information for the input relation; fpinfo_i is expected to NULL.
 */
static void merge_fdw_options(DB2FdwState* fpinfo, const DB2FdwState* fpinfo_o, const DB2FdwState* fpinfo_i) {
  db2Entry4();
  /* We must always have fpinfo_o. */
  Assert(fpinfo_o);

  /* fpinfo_i may be NULL, but if present the servers must both match. */
  Assert(!fpinfo_i || fpinfo_i->server->serverid == fpinfo_o->server->serverid);

  /* Copy the server specific FDW options.
   * (For a join, both relations come from the same server, so the server options should have the same value for both relations.)
   */
  fpinfo->fdw_startup_cost      = fpinfo_o->fdw_startup_cost;
  fpinfo->fdw_tuple_cost        = fpinfo_o->fdw_tuple_cost;
  fpinfo->shippable_extensions  = fpinfo_o->shippable_extensions;
  fpinfo->use_remote_estimate   = fpinfo_o->use_remote_estimate;
  fpinfo->fetch_size            = fpinfo_o->fetch_size;
  fpinfo->async_capable         = fpinfo_o->async_capable;

  /* Merge the table level options from either side of the join. */
  if (fpinfo_i) {
    /* We'll prefer to use remote estimates for this join if any table from either side of the join is using remote estimates.
     * This is most likely going to be preferred since they're already willing to pay the price of a round trip to get the remote EXPLAIN.
     * In any case it's not entirely clear how we might otherwise handle this best.
     */
    fpinfo->use_remote_estimate = fpinfo_o->use_remote_estimate || fpinfo_i->use_remote_estimate;

    /* Set fetch size to maximum of the joining sides, since we are expecting the rows returned by the join to be proportional to the relation sizes. */
    fpinfo->fetch_size = Max(fpinfo_o->fetch_size, fpinfo_i->fetch_size);

    /* We'll prefer to consider this join async-capable if any table from either side of the join is considered async-capable.
     * This would be reasonable because in that case the foreign server would have its own resources to scan that table asynchronously, 
     * and the join could also be computed asynchronously using the resources.
     */
    fpinfo->async_capable = fpinfo_o->async_capable || fpinfo_i->async_capable;
  }
  db2Exit4();
}
