#include <postgres.h>
#include <nodes/nodeFuncs.h>
#include <optimizer/pathnode.h>
#include <optimizer/optimizer.h>
#include <access/heapam.h>
#include "db2_fdw.h"
#include "DB2FdwState.h"

/** external prototypes */
extern char*              deparseExpr          (PlannerInfo* root, RelOptInfo* foreignrel, Expr* expr, List** params);
extern bool               is_foreign_pathkey   (PlannerInfo* root, RelOptInfo* baserel, PathKey* pathkey);
extern EquivalenceMember* find_em_for_rel      (PlannerInfo* root, EquivalenceClass* ec, RelOptInfo* rel);

/** local prototypes */
void db2GetForeignPaths(PlannerInfo* root, RelOptInfo* baserel, Oid foreigntableid);

/* db2GetForeignPaths
 * Create a ForeignPath node and add it as only possible path.
 */
void db2GetForeignPaths(PlannerInfo* root, RelOptInfo* baserel, Oid foreigntableid) {
  DB2FdwState* fdwState = (DB2FdwState*) baserel->fdw_private;

  /* Determine whether we can potentially push query pathkeys to the remote side, avoiding a local sort. */
  StringInfoData orderedquery;
  List*          usable_pathkeys = NIL;
  ListCell*      cell;
  char*          delim = " ";

  db2Entry1();
  initStringInfo (&orderedquery);

  foreach (cell, root->query_pathkeys) {
    PathKey*           pathkey      = (PathKey*) lfirst (cell);
    EquivalenceClass*  pathkey_ec   = pathkey->pk_eclass;
    EquivalenceMember* em           = NULL;
    Expr*              em_expr      = NULL;
    char*              sort_clause  = NULL;
    Oid                em_type      = 0;
    bool               can_pushdown = false;

    /*
     * Use the same shippability and EC-member lookup that appendOrderByClause()
     * uses later when it deparses the selected ForeignPath.  The old local
     * helper only compared relids and accepted expressions that deparseExpr()
     * could print even when is_foreign_expr() rejected them.  Such a path
     * advertised remote ordering, but failed during plan creation with
     * "could not find pathkey item to sort" (notably for DISTINCT over CASE
     * expressions).
     */
    can_pushdown = is_foreign_pathkey(root, baserel, pathkey);
    if (can_pushdown) {
      em = find_em_for_rel(root, pathkey_ec, baserel);
      if (em != NULL)
        em_expr = em->em_expr;
      else
        can_pushdown = false;
    }

    if (can_pushdown) {
      em_type = exprType ((Node *) em_expr);

      /* expressions of a type different from this are not safe to push down into ORDER BY clauses */
      switch(em_type){
        case INT8OID:
        case INT2OID:
        case INT4OID:
        case OIDOID:
        case FLOAT4OID:
        case FLOAT8OID:
        case NUMERICOID:
        case DATEOID:
        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
        case TIMEOID:
        case TIMETZOID:
        case INTERVALOID:
        case TEXTOID:
        case CHAROID:
        case BPCHAROID:
        case VARCHAROID:
        case NAMEOID:
          can_pushdown = true;
        break;
        default:
          can_pushdown = false;
        break;
      }
    }

    if (can_pushdown && ((sort_clause = deparseExpr (root, baserel, em_expr, &(fdwState->params))) != NULL)) {
      /* keep usable_pathkeys for later use. */
      usable_pathkeys = lappend (usable_pathkeys, pathkey);

      /* create orderedquery */
      appendStringInfoString (&orderedquery, delim);
      appendStringInfoString (&orderedquery, sort_clause);
      delim = ", ";

      #if PG_VERSION_NUM >= 180000
      appendStringInfoString (&orderedquery, (pathkey->pk_cmptype == COMPARE_LT) ? " ASC" : " DESC");
      #else
      appendStringInfoString (&orderedquery, (pathkey->pk_strategy == BTLessStrategyNumber) ? " ASC" : " DESC");
      #endif
      appendStringInfoString (&orderedquery, (pathkey->pk_nulls_first) ? " NULLS FIRST" : " NULLS LAST");
    } else {
      /* The planner and executor don't have any clever strategy for taking data sorted by a prefix of the query's pathkeys and
       * getting it to be sorted by all of those pathekeys.
       * We'll just end up resorting the entire data set.
       * So, unless we can push down all of the query pathkeys, forget it.
       */
      list_free (usable_pathkeys);
      usable_pathkeys = NIL;
      break;
    }
  }

  /* set order clause */
  if (usable_pathkeys != NIL)
    fdwState->order_clause = orderedquery.data;

  /* add the only path */
  add_path (baserel, (Path *) create_foreignscan_path (root
                                                      ,baserel
                                                      ,NULL  /* default pathtarget */
                                                      ,baserel->rows
  #if PG_VERSION_NUM >= 180000
                                                      ,0  /* no disabled plan nodes */
  #endif  /* PG_VERSION_NUM */
                                                      ,fdwState->startup_cost
                                                      ,fdwState->total_cost
                                                      ,usable_pathkeys
                                                      ,baserel->lateral_relids
                                                      ,NULL  /* no extra plan */
  #if PG_VERSION_NUM >= 170000
                                                      ,NIL   /* no fdw_restrictinfo */
  #endif  /* PG_VERSION_NUM */
                                                      ,NIL
                                                      )
    );
  db2Exit1();
}
