#include <postgres.h>
#include <access/table.h>
#include <nodes/makefuncs.h>
#include <optimizer/appendinfo.h>
#include <optimizer/optimizer.h>
#include <optimizer/pathnode.h>
#include <optimizer/tlist.h>
#include <parser/parsetree.h>
#include <utils/rel.h>
#include "db2_fdw.h"
#include "DB2FdwState.h"

/** external prototypes */
extern bool         is_foreign_expr           (PlannerInfo *root, RelOptInfo *baserel, Expr *expr);
extern void         deparseDirectUpdateSql    (StringInfo buf, PlannerInfo *root, Index rtindex, Relation rel, RelOptInfo *foreignrel, List *targetlist, List *targetAttrs, List *remote_conds, List **params_list, List *returningList, List **retrieved_attrs);
extern void         deparseDirectDeleteSql    (StringInfo buf, PlannerInfo *root, Index rtindex, Relation rel, RelOptInfo *foreignrel, List *remote_conds, List **params_list, List *returningList, List **retrieved_attrs);

/** local prototypes */
       bool         db2PlanDirectModify       (PlannerInfo* root, ModifyTable* plan, Index rtindex, int subplan_index);
#if PG_VERSION_NUM >= 140000
static ForeignScan* find_modifytable_subplan  (PlannerInfo* root, ModifyTable* plan, Index rtindex, int subplan_index);
#else
static void         rebuild_fdw_scan_tlist    (ForeignScan *fscan, List *tlist);
#endif
/* postgresPlanDirectModify
 * Decide whether it is safe to modify a foreign table directly, and if so, rewrite subplan accordingly.
 */
bool db2PlanDirectModify(PlannerInfo* root, ModifyTable* plan, Index rtindex, int subplan_index) {
  bool  fResult = true;

  db2Entry1();
  db2Debug2("plan->operation: %d", plan->operation);
  db2Debug2("plan->returningLists: %x - %d", plan->returningLists, list_length(plan->returningLists));
  /* The table modification must be an UPDATE or DELETE and must not use RETURNING */
  if ((plan->operation == CMD_UPDATE || plan->operation == CMD_DELETE) && plan->returningLists == NIL) {
    #if PG_VERSION_NUM < 140000
    Plan* subplan = (Plan *) list_nth(plan->plans, subplan_index);
    if (IsA(subplan, ForeignScan)) {
      ForeignScan*fscan = (ForeignScan *) subplan;
    #else
    /* Try to locate the ForeignScan subplan that's scanning rtindex. */
    ForeignScan* fscan  = find_modifytable_subplan(root, plan, rtindex, subplan_index);
    db2Debug2("fscan: %x",fscan);
    if (fscan) {
    #endif
      /* It's unsafe to modify a foreign table directly if there are any quals that should be evaluated locally. */
      db2Debug2("fscan->scan.plan.qual: %x",fscan->scan.plan.qual);
      if (fscan->scan.plan.qual == NIL) {
        RelOptInfo*     foreignrel        = NULL;
        RangeTblEntry*  rte              = NULL;
        DB2FdwState* 		fpinfo           = NULL;
        List*           processed_tlist  = NIL;
        List*           targetAttrs      = NIL;

        /* Safe to fetch data about the target foreign rel */
        if (fscan->scan.scanrelid == 0) {
          foreignrel = find_join_rel(root, fscan->fs_relids);
          /* We should have a rel for this foreign join. */
          Assert(foreignrel);
        } else {
          foreignrel = root->simple_rel_array[rtindex];
        }
        rte = root->simple_rte_array[rtindex];
        /* skip deserialization of plan data*/
        fpinfo = (DB2FdwState*) foreignrel->fdw_private;

        /* It's unsafe to update a foreign table directly, 
         * if any expressions to assign to the target columns are unsafe to evaluate remotely. 
         */
        if (plan->operation == CMD_UPDATE) {
          #if PG_VERSION_NUM < 140000
          int   col;

          /* We transmit only columns that were explicitly targets of the UPDATE, so as to avoid unnecessary data transmission. */
          col = -1;
          while ((col = bms_next_member(rte->updatedCols, col)) >= 0) {
            /* bit numbers are offset by FirstLowInvalidHeapAttributeNumber */
            AttrNumber	attno = col + FirstLowInvalidHeapAttributeNumber;
            TargetEntry *tle;

            if (attno <= InvalidAttrNumber) /* shouldn't happen */
              elog(ERROR, "system-column update is not supported");

            tle = get_tle_by_resno(subplan->targetlist, attno);

            if (!tle)
              elog(ERROR, "attribute number %d not found in subplan targetlist", attno);

            if (!is_foreign_expr(root, foreignrel, (Expr *) tle->expr)) {
              fResult = false;
              break;
            }
            targetAttrs = lappend_int(targetAttrs, attno);
          }
          #else
          ListCell*	lc 	= NULL;
          ListCell*	lc2 = NULL;

          /* The expressions of concern are the first N columns of the processed targetlist, 
           * where N is the length of the rel's update_colnos.
           */
          get_translated_update_targetlist(root, rtindex, &processed_tlist, &targetAttrs);
          forboth(lc, processed_tlist, lc2, targetAttrs) {
            TargetEntry*  tle   = lfirst_node(TargetEntry, lc);
            AttrNumber	  attno = lfirst_int(lc2);
            /* update's new-value expressions shouldn't be resjunk */
            Assert(!tle->resjunk);
            if (attno <= InvalidAttrNumber) /* shouldn't happen */
              elog(ERROR, "system-column update is not supported");
            if (!is_foreign_expr(root, foreignrel, (Expr *) tle->expr)) {
              fResult = false;
              break;
            }
          }
          #endif
        }
        db2Debug2("fResult: %s",(fResult) ? "true" : "false");
        if (fResult) {
          StringInfoData  sql;
          Relation	      rel;
          List*           remote_exprs     = NIL;
          List*           params_list      = NIL;
          List*           returningList    = NIL;
          List*           retrieved_attrs  = NIL;

          /* Ok, rewrite subplan so as to modify the foreign table directly. */
          initStringInfo(&sql);

          /* Core code already has some lock on each rel being planned, so we can use NoLock here.
          */
          rel = table_open(rte->relid, NoLock);

          /* Recall the qual clauses that must be evaluated remotely.
          * (These are bare clauses not RestrictInfos, but deparse.c's appendConditions() doesn't care.)
          */
          remote_exprs = fpinfo->final_remote_exprs;

          /* DB2 does not support RETURNIN in UPDATE and DELETE queries */

          /* Construct the SQL command string. */
          switch (plan->operation) {
            case CMD_UPDATE:
              deparseDirectUpdateSql(&sql, root, rtindex, rel,
                          foreignrel,
                          processed_tlist,
                          targetAttrs,
                          remote_exprs, &params_list,
                          returningList, &retrieved_attrs);
              break;
            case CMD_DELETE:
              deparseDirectDeleteSql(&sql, root, rtindex, rel,
                          foreignrel,
                          remote_exprs, &params_list,
                          returningList, &retrieved_attrs);
              break;
            default:
              elog(ERROR, "unexpected operation: %d", (int) plan->operation);
              break;
          }

          /* Update the operation and target relation info. */
          fscan->operation      = plan->operation;
          #if PG_VERSION_NUM >= 140000
          fscan->resultRelation = rtindex;
          #endif
          /* Update the fdw_exprs list that will be available to the executor. */
          fscan->fdw_exprs      = params_list;

          /* Update the fdw_private list that will be available to the executor.
          * Items in the list must match enum FdwDirectModifyPrivateIndex, above.
          */
          #if PG_VERSION_NUM < 150000
          fscan->fdw_private    = list_make4(makeString(sql.data), makeInteger((retrieved_attrs != NIL)), retrieved_attrs, makeInteger(plan->canSetTag));
          #else
          fscan->fdw_private    = list_make4(makeString(sql.data), makeBoolean((retrieved_attrs != NIL)), retrieved_attrs, makeBoolean(plan->canSetTag));
          #endif

          /* Update the foreign-join-related fields. */
          if (fscan->scan.scanrelid == 0) {
            /* No need for the outer subplan. */
            fscan->scan.plan.lefttree = NULL;
          #if PG_VERSION_NUM < 140000
            /* Build new fdw_scan_tlist if UPDATE/DELETE .. RETURNING. */
            if (returningList)
              rebuild_fdw_scan_tlist(fscan, returningList);
          }
          #else
          }
            /* Finally, unset the async-capable flag if it is set, as we currently don't support asynchronous execution of direct modifications. */
          if (fscan->scan.plan.async_capable)
            fscan->scan.plan.async_capable = false;
          #endif
          table_close(rel, NoLock);
        }
      } else {
        fResult = false;
      }
    } else {
      fResult = false;
    }
  } else {
    fResult = false;
  }
  db2Exit1(": %s", (fResult) ? "true" : "false");
  return fResult;
}

#if PG_VERSION_NUM >= 140000
/* find_modifytable_subplan
 * Helper routine for postgresPlanDirectModify to find the ModifyTable subplan node that scans the specified RTI.
 *
 * Returns NULL if the subplan couldn't be identified.
 * That's not a fatal error condition, we just abandon trying to do the update directly.
 */
static ForeignScan* find_modifytable_subplan(PlannerInfo* root, ModifyTable* plan, Index rtindex, int subplan_index) {
  ForeignScan*  fscan   = NULL; 
  Plan*         subplan = outerPlan(plan);

  db2Entry1();
  /* The cases we support are (1) the desired ForeignScan is the immediate child of ModifyTable, or (2) it is the subplan_index'th child of an
   * Append node that is the immediate child of ModifyTable.
   * There is no point in looking further down, as that would mean that local joins are involved, so we can't do the update directly.
   * There could be a Result atop the Append too, acting to compute the UPDATE targetlist values.
   * We ignore that here; the tlist will be checked by our caller.
   * In principle we could examine all the children of the Append, but it's currently unlikely that the core planner would generate such a plan
   * with the children out-of-order.
   * Moreover, such a search risks costing O(N^2) time when there are a lot of children.
   */
  db2Debug3("subplan from outerplan: %x",subplan);
  if (IsA(subplan, Append)) {
    Append* append = (Append*) subplan;

    db2Debug4("subplan is Append");
    if (subplan_index < list_length(append->appendplans)) {
      subplan = (Plan*) list_nth(append->appendplans, subplan_index);
      db2Debug3("subplan from appendplan: %x",subplan);
    }
  } else if (IsA(subplan, Result) && outerPlan(subplan) != NULL && IsA(outerPlan(subplan), Append)) {
    Append* append = (Append*) outerPlan(subplan);

    db2Debug4("subplan is Result");
    if (subplan_index < list_length(append->appendplans)) {
      subplan = (Plan*) list_nth(append->appendplans, subplan_index);
      db2Debug3("subplan from resultplan: %x",subplan);
    }
  }

  /* Now, have we got a ForeignScan on the desired rel? */
  db2Debug3("subplan: %x",subplan);
  #if PG_VERSION_NUM < 160000
  if (IsA(subplan, ForeignScan) && (bms_is_member(rtindex, ((ForeignScan*) subplan)->fs_relids))) {
  #else
  if (IsA(subplan, ForeignScan) && (bms_is_member(rtindex, ((ForeignScan*) subplan)->fs_base_relids))) {
  #endif
      db2Debug4("subplan is ForeignScan");
      fscan = (ForeignScan*) subplan;
  }
  db2Exit1(": %x", fscan);
  return fscan;
}
#else
/* rebuild_fdw_scan_tlist
 * Build new fdw_scan_tlist of given foreign-scan plan node from given tlist
 *
 * There might be columns that the fdw_scan_tlist of the given foreign-scan plan node contains that the given tlist doesn't.
 * The fdw_scan_tlist would have contained resjunk columns such as 'ctid' of the target relation and 'wholerow' of non-target relations,
 * but the tlist might not contain them, for example. 
 * So, adjust the tlist so it contains all the columns specified in the fdw_scan_tlist; else setrefs.c will get confused.
 */
static void rebuild_fdw_scan_tlist(ForeignScan *fscan, List *tlist) {
  List*     new_tlist = tlist;
  List*     old_tlist = fscan->fdw_scan_tlist;
  ListCell* lc;

  foreach(lc, old_tlist) {
    TargetEntry *tle = (TargetEntry *) lfirst(lc);

    if (tlist_member(tle->expr, new_tlist))
      continue;       /* already got it */

    new_tlist = lappend(new_tlist, makeTargetEntry(tle->expr, list_length(new_tlist) + 1, NULL, false));
  }
  fscan->fdw_scan_tlist = new_tlist;
}
#endif