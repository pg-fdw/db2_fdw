#include <postgres.h>
#include <optimizer/pathnode.h>
#include <optimizer/restrictinfo.h>
#include <optimizer/optimizer.h>
#include <access/heapam.h>
#include "db2_fdw.h"
#include "DB2FdwState.h"

/** external prototypes */
extern char*        deparseExpr               (PlannerInfo* root, RelOptInfo* foreignrel, Expr* expr, List** params);

/** local prototypes */
void db2GetForeignJoinPaths(PlannerInfo* root, RelOptInfo* joinrel, RelOptInfo* outerrel, RelOptInfo* innerrel, JoinType jointype, JoinPathExtraData* extra);
static bool foreign_join_ok       (PlannerInfo* root, RelOptInfo* joinrel, JoinType jointype, RelOptInfo* outerrel, RelOptInfo* innerrel, JoinPathExtraData* extra);
static DB2Column* db2FindColumnByRelidAttnum(DB2Table* db2Table, int pgrelid, int pgattnum);

/* db2FindColumnByRelidAttnum
 * Find the DB2Column descriptor for a base foreign relation column.
 */
static DB2Column* db2FindColumnByRelidAttnum(DB2Table* db2Table, int pgrelid, int pgattnum) {
  int i;
  if (!db2Table || !db2Table->cols)
    return NULL;
  for (i = 0; i < db2Table->ncols; ++i) {
    DB2Column* tmp = db2Table->cols[i];
    if (tmp && tmp->pgrelid == pgrelid && tmp->pgattnum == pgattnum)
      return tmp;
  }
  return NULL;
}

/* db2GetForeignJoinPaths
 * Add possible ForeignPath to joinrel if the join is safe to push down.
 * For now, we can only push down 2-way inner join for SELECT.
 */
void db2GetForeignJoinPaths (PlannerInfo * root, RelOptInfo * joinrel, RelOptInfo * outerrel, RelOptInfo * innerrel, JoinType jointype, JoinPathExtraData * extra) {
  DB2FdwState* fdwState                = NULL;
  DB2FdwState* fdwState_o              = NULL;
  DB2FdwState* fdwState_i              = NULL;
  ForeignPath* joinpath                = NULL;
  double       joinclauses_selectivity = 0;
  double       rows                    = 0;      /* estimated number of returned rows */
  Cost         startup_cost;
  Cost         total_cost;

  db2Entry1();
  /* Currently we don't push-down joins in query for UPDATE/DELETE.
   * This would require a path for EvalPlanQual.
   * This restriction might be relaxed in a later release.
   */
  if (root->parse->commandType != CMD_SELECT) {
    db2Debug2("db2_fdw: don't push down join because it is no SELECT");
  } else {
    /* N-way join is not supported, due to the column definition infrastracture.
    * If we can track relid mapping of join relations, we can support N-way join.
    */
    if (!IS_SIMPLE_REL (outerrel) || !IS_SIMPLE_REL (innerrel)) {
      db2Debug2("either outerrel or ínnerel is not a simple relation");
    } else {
      /* skip if this join combination has been considered already */
      if (joinrel->fdw_private) {
        db2Debug2("this join combination has been considered already");
      } else {
        /* Create unfinished DB2FdwState which is used to indicate
        * that the join relation has already been considered, so that we won't waste
        * time considering it again and don't add the same path a second time.
        * Once we know that this join can be pushed down, we fill the data structure.
        */
        fdwState = (DB2FdwState *) db2alloc(sizeof (DB2FdwState),"DB2FdwState* fdwState");

        joinrel->fdw_private = fdwState;

        /* this performs further checks and completes joinrel->fdw_private */
        if (foreign_join_ok (root, joinrel, jointype, outerrel, innerrel, extra)) {
          fdwState_o = (DB2FdwState*) outerrel->fdw_private;
          fdwState_i = (DB2FdwState*) innerrel->fdw_private;

          /* estimate the number of result rows for the join */
          if (outerrel->tuples >= 0 && innerrel->tuples >= 0)
          {
            /* both relations have been ANALYZEd, so there should be useful statistics */
            joinclauses_selectivity = clauselist_selectivity(root, fdwState->joinclauses, 0, JOIN_INNER, extra->sjinfo);
            rows = clamp_row_est (innerrel->tuples * outerrel->tuples * joinclauses_selectivity);
          } else {
            /* at least one table lacks statistics, so use a fixed estimate */
            rows = 1000.0;
          }

          /*
           * Minimal cost tweak:
           * The previous hard-coded startup_cost=10000 made the foreign-join path
           * effectively impossible to win against a local join, so join pushdown
           * never happened even when it was otherwise safe.
           *
           * Use the already-estimated costs of the two input foreign relations as
           * the baseline cost for the pushed-down join.
           */
          startup_cost = (fdwState_o ? fdwState_o->startup_cost : 0) + (fdwState_i ? fdwState_i->startup_cost : 0);
          total_cost   = (fdwState_o ? fdwState_o->total_cost   : 0) + (fdwState_i ? fdwState_i->total_cost   : 0);

          /* store cost estimation results */
          joinrel->rows          = rows;
          fdwState->startup_cost = startup_cost;
          fdwState->total_cost   = total_cost;

          /* create a new join path */
          joinpath = create_foreign_join_path( root
                                            , joinrel
                                            , NULL  /* default pathtarget */
                                            , rows
          #if PG_VERSION_NUM >= 180000
                                            , 0     /* no disabled plan nodes */
          #endif  /* PG_VERSION_NUM */
                                            , startup_cost
                                            , total_cost
                                            , NIL   /* no pathkeys */
                                            , joinrel->lateral_relids
                                            , NULL  /* no epq_path */
          #if PG_VERSION_NUM >= 170000
                                            , NIL   /* no fdw_restrictinfo */
          #endif  /* PG_VERSION_NUM */
                                            , NIL   /* no fdw_private */
                                            );
          /* add generated path to joinrel */
          add_path(joinrel, (Path *) joinpath);
        } else {
          /*
           * foreign_join_ok can reject pushdown for reasons that might depend on
           * planner state (or because we could not map join target columns). In
           * that case, don't leave a half-initialized marker state behind that
           * prevents reconsideration.
           */
          joinrel->fdw_private = NULL;
        }
      }
    }
  }
  db2Exit1();
}

/* foreign_join_ok
 * Assess whether the join between inner and outer relations can be pushed down to the foreign server. As a side effect, save information we obtain in this
 * function to DB2FdwState passed in.
 */
static bool foreign_join_ok (PlannerInfo * root, RelOptInfo * joinrel, JoinType jointype, RelOptInfo * outerrel, RelOptInfo * innerrel, JoinPathExtraData * extra) {
  DB2FdwState* fdwState     = NULL;
  DB2FdwState* fdwState_o   = NULL;
  DB2FdwState* fdwState_i   = NULL;
  DB2Table*    db2Table_o   = NULL;
  DB2Table*    db2Table_i   = NULL;
  ListCell*    lc           = NULL;
  List*        otherclauses = NULL;

  db2Entry1();
  /* we only support pushing down INNER joins */
  if (jointype != JOIN_INNER)
    return false;

  fdwState   = (DB2FdwState*) joinrel->fdw_private;
  fdwState_o = (DB2FdwState*) outerrel->fdw_private;
  fdwState_i = (DB2FdwState*) innerrel->fdw_private;
  Assert (fdwState && fdwState_o && fdwState_i);

  fdwState->outerrel = outerrel;
  fdwState->innerrel = innerrel;
  fdwState->jointype = jointype;

  /* If joining relations have local conditions, those conditions are
   * required to be applied before joining the relations. Hence the join can
   * not be pushed down.
   */
  if (fdwState_o->local_conds || fdwState_i->local_conds)
    return false;

  /* Separate restrict list into join quals and quals on join relation */

  /* Unlike an outer join, for inner join, the join result contains only
   * the rows which satisfy join clauses, similar to the other clause.
   * Hence all clauses can be treated the same.
   */
  otherclauses = extract_actual_clauses (extra->restrictlist, false);

  /* For inner joins, "otherclauses" contains now the join conditions.
   * Check which ones can be pushed down.
   */
  foreach (lc, otherclauses) {
    char *tmp  = NULL;
    Expr *expr = (Expr *) lfirst (lc);

    tmp = deparseExpr (root, joinrel, expr, &(fdwState->params));

    if (tmp == NULL)
      fdwState->local_conds = lappend (fdwState->local_conds, expr);
    else
      fdwState->remote_conds = lappend (fdwState->remote_conds, expr);
  }

  /* Only push down joins for which all join conditions can be pushed down.
   *
   * For an inner join it would be ok to only push own some of the join
   * conditions and evaluate the others locally, but we cannot be certain
   * that such a plan is a good or even a feasible one:
   * With one of the join conditions missing in the pushed down query,
   * it could be that the "intermediate" join result fetched from the DB2
   * side has many more rows than the complete join result.
   *
   * We could rely on estimates to see how many rows are returned from such
   * a join where not all join conditions can be pushed down, but we choose
   * the safe road of not pushing down such joins at all.
   */
  if (fdwState->local_conds != NIL)
    return false;

  /* CROSS JOIN (T1 JOIN T2 ON true) is not pushed down */
  if (fdwState->remote_conds == NIL)
    return false;

  /* Pull the other remote conditions from the joining relations into join
   * clauses or other remote clauses (remote_conds) of this relation
   * wherever possible. This avoids building subqueries at every join step,
   * which is not currently supported by the deparser logic.
   *
   * For an inner join, clauses from both the relations are added to the
   * other remote clauses.
   *
   * The joining sides can not have local conditions, thus no need to test
   * shippability of the clauses being pulled up.
   */
  fdwState->remote_conds = list_concat (fdwState->remote_conds, list_copy (fdwState_i->remote_conds));
  fdwState->remote_conds = list_concat (fdwState->remote_conds, list_copy (fdwState_o->remote_conds));

  /* For an inner join, all restrictions can be treated alike. Treating the
   * pushed down conditions as join conditions allows a top level full outer
   * join to be deparsed without requiring subqueries.
   */
  fdwState->joinclauses = fdwState->remote_conds;
  fdwState->remote_conds = NIL;

  /* set fetch size to minimum of the joining sides */
  if (fdwState_o->prefetch < fdwState_i->prefetch)
    fdwState->prefetch = fdwState_o->prefetch;
  else
    fdwState->prefetch = fdwState_i->prefetch;

  /* copy outerrel's infomation to fdwstate */
  fdwState->dbserver = fdwState_o->dbserver;
  fdwState->user     = fdwState_o->user;
  fdwState->password = fdwState_o->password;

  /* construct db2Table for the result of join */
  db2Table_o = fdwState_o->db2Table;
  db2Table_i = fdwState_i->db2Table;

  fdwState->db2Table          = (DB2Table*) db2alloc(sizeof (DB2Table), "fdw_state->db2Table");
  /* Give the joinrel a non-empty name so warnings/debug output are readable. */
  fdwState->db2Table->name    = db2strdup ("joinrel", "fdwState->db2Table->name");
  fdwState->db2Table->pgname  = db2strdup ("joinrel", "fdwState->db2Table->pgname");
  fdwState->db2Table->ncols   = 0;
  fdwState->db2Table->npgcols = 0;
  fdwState->db2Table->cols    = (DB2Column **) db2alloc((sizeof (DB2Column*) * (db2Table_o->ncols + db2Table_i->ncols)), "fdw_state->db2Table->cols[%d]",(db2Table_o->ncols + db2Table_i->ncols));

  /* Search db2Column from children's db2Table.
   * Here we assume that children are foreign table, not foreign join.
   * We need capability to track relid chain through join tree to support N-way join.
   */
  foreach (lc, joinrel->reltarget->exprs) {
    Var*      var      = (Var *) lfirst(lc);
    DB2Column* col     = NULL;
    DB2Column* newcol  = NULL;
    int       used_flag = 0;
    int       src_varno;
    int       src_attno;
    int       src_relid;

    if (!IsA(var, Var))
      return false;

    /*
     * joinrel->reltarget->exprs can contain Vars referencing join inputs via
     * OUTER_VAR/INNER_VAR (and sometimes direct baserel RT indexes).  Resolve
     * those to the appropriate child's relid so we can look up the base column
     * descriptor and copy its DB2/PG type metadata.
     */
    src_varno = var->varno;
    src_attno = var->varattno;
    src_relid = src_varno;

    if (src_varno == OUTER_VAR)
      src_relid = outerrel->relid;
    else if (src_varno == INNER_VAR)
      src_relid = innerrel->relid;

    col = db2FindColumnByRelidAttnum(db2Table_o, src_relid, src_attno);
    if (!col) {
      col = db2FindColumnByRelidAttnum(db2Table_i, src_relid, src_attno);
    }

    newcol = (DB2Column*) db2alloc(sizeof(DB2Column), "newcol");
    memset(newcol, 0, sizeof(DB2Column));

    if (col) {
      memcpy(newcol, col, sizeof(DB2Column));
      used_flag = 1;
    } else {
      /*
       * If we can't map an output column back to an underlying foreign table
       * column, join pushdown isn't safe (we'd otherwise create a column with
       * undefined pgtype/colType and crash at execution time).
       */
      ereport(DEBUG2,
              (errmsg("db2_fdw: cannot map join output column (varno=%d attno=%d); disabling join pushdown for this join",
                      var->varno, var->varattno)));
      return false;
    }

    newcol->used = used_flag;
    /*
     * IMPORTANT:
     * db2GetForeignPlan() later maps Vars by Var.varattno to db2Table->cols[*]->pgattnum.
     * For joinrels these Var.varattno values are already the join's attribute numbers,
     * so we must preserve them here (not renumber sequentially), otherwise planning
     * will fail to find columns and leave result bindings uninitialized.
     */
    newcol->pgattnum = var->varattno;

    fdwState->db2Table->cols[fdwState->db2Table->ncols++] = newcol;
  }

  /*
   * IMPORTANT:
   * For base foreign tables, db2Table->npgcols matches the number of PG columns
   * and convertTuple() can map output columns by pgattnum.
   *
   * For join pushdown, however, the executor slot natts equals the number of
   * projected join output columns, while the pgattnum values we carry in the
   * DB2ResultColumn descriptors refer to *base-table* attnums (and can be
   * sparse / non-1..N).  If npgcols == natts, convertTuple() would treat the
   * join as a "simple select" and use pgattnum as the destination index,
   * causing out-of-bounds writes and corrupted tuples.
   *
   * Force convertTuple() to use resnum-based mapping for joinrels.
   */
  fdwState->db2Table->npgcols = 0;

  db2Exit1();
  return true;
}

