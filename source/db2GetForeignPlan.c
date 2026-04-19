#include <postgres.h>
#include <access/htup_details.h>
#include <catalog/pg_namespace.h>
#include <catalog/pg_proc.h>
#include <catalog/pg_type.h>
#include <optimizer/planmain.h>
#include <optimizer/restrictinfo.h>
#include <optimizer/tlist.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>
#include "db2_fdw.h"
#include "DB2FdwState.h"

/* This enum describes what's kept in the fdw_private list for a ForeignPath.
 * We store:
 *
 * 1) Boolean flag showing if the remote query has the final sort
 * 2) Boolean flag showing if the remote query has the LIMIT clause
 */
enum FdwPathPrivateIndex {
  FdwPathPrivateHasFinalSort, /* has-final-sort flag (as a Boolean node) */
  FdwPathPrivateHasLimit,     /* has-limit flag (as a Boolean node)      */
};

/** external prototypes */
extern bool   is_foreign_expr         (PlannerInfo* root, RelOptInfo* baserel, Expr* expr);
extern void   deparseSelectStmtForRel (StringInfo buf, PlannerInfo* root, RelOptInfo* rel, List* tlist, List* remote_conds, List* pathkeys, bool has_final_sort, bool has_limit, bool is_subquery, List** retrieved_attrs, List** params_list);
extern List*  build_tlist_to_deparse  (RelOptInfo* foreignrel);
extern List*  serializePlanData       (DB2FdwState* fdw_state);

/** local prototypes */
       ForeignScan* db2GetForeignPlan       (PlannerInfo* root, RelOptInfo* foreignrel, Oid foreigntableid, ForeignPath* best_path, List* tlist, List* scan_clauses , Plan* outer_plan);
static void         getUsedColumns          (Expr* expr, RelOptInfo* foreignrel, DB2ResultColumn* resCol);
static void         copyCol2Result          (DB2ResultColumn*  resCol, DB2Column* db2Column);
static int          compareResultColumns    (const void* a, const void* b);

/* postgresGetForeignPlan
 * Create ForeignScan plan node which implements selected best path
 */
ForeignScan* db2GetForeignPlan(PlannerInfo* root, RelOptInfo* foreignrel, Oid foreigntableid, ForeignPath* best_path, List* tlist, List* scan_clauses, Plan* outer_plan) {
  DB2FdwState*   fpinfo            = (DB2FdwState*) foreignrel->fdw_private;
  ForeignScan*   fscan             = NULL;
  List*          fdw_private       = NIL;
  List*          remote_exprs      = NIL;
  List*          local_exprs       = NIL;
  List*          params_list       = NIL;
  List*          fdw_recheck_quals = NIL;
  List*          retrieved_attrs   = NIL;
  List*          ptlist            = NIL;
  int            ptlist_len        = 0; 
  ListCell*      lc                = NULL;
  bool           has_final_sort    = false;
  bool           has_limit         = false;
  Index          scan_relid;
  StringInfoData sql;

  db2Entry1();
  /* Get FDW private data created by db2GetForeignUpperPaths(), if any. */
  if (best_path->fdw_private) {
    #if PG_VERSION_NUM < 150000
    has_final_sort  = intVal(list_nth(best_path->fdw_private, FdwPathPrivateHasFinalSort));
    has_limit       = intVal(list_nth(best_path->fdw_private,	FdwPathPrivateHasLimit));
    #else
    has_final_sort  = boolVal(list_nth(best_path->fdw_private, FdwPathPrivateHasFinalSort));
    has_limit       = boolVal(list_nth(best_path->fdw_private, FdwPathPrivateHasLimit));
    #endif
  }

  db2Debug2("length of tlist: %d", list_length(tlist));

  /*
   * fdw_scan_tlist must contain all base Vars required to evaluate local
   * quals and to compute any non-pushed-down target expressions.
   *
   * Using a non-flattened PathTarget tlist here can leave out Vars that are
   * only referenced inside expressions (e.g. salary + bonus + comm), which can
   * lead to setrefs.c errors like "variable not found in subplan target list".
   */
  ptlist     = build_tlist_to_deparse(foreignrel);
  ptlist_len = list_length(ptlist);

  if (IS_SIMPLE_REL(foreignrel)) {
    DB2ResultColumn** cols      = NULL;
    ListCell*         cell      = NULL;
    int               iResCol   = 0;

    db2Debug3("base relation scan: set scan_relid to %d", foreignrel->relid);
    /* For base relations, set scan_relid as the relid of the relation. */
    scan_relid = foreignrel->relid;

    if (ptlist_len >0) {
      DB2ResultColumn* resCol = NULL;
      /* find all the columns to include in the select list */
      /* examine each SELECT list entry for Var nodes */
      db2Debug3("size of tlist: %d", ptlist_len);
      foreach (cell, ptlist) {
        resCol             = (DB2ResultColumn*)db2alloc(sizeof(DB2ResultColumn), "resCol");
        getUsedColumns ((Expr*) lfirst (cell), foreignrel, resCol);
        db2Debug3("resCol->colName: %s", resCol->colName);
        db2Debug3("resCol->pgattnum: %d", resCol->pgattnum);
        if (resCol->colName != NULL && resCol->pgattnum <= fpinfo->db2Table->npgcols) {
          resCol->next       = fpinfo->resultList;
          db2Debug3("resCol->next: %x", resCol->next);
          fpinfo->resultList = resCol;
          db2Debug3("fpinfo->resultList: %x", fpinfo->resultList);
        } else {
          db2Debug3("about to free resCol: %x, colName: %s, pgattnum: %d", resCol, resCol->colName, resCol->pgattnum);
          db2free(resCol,"resCol");
        }
      }
      for (resCol = fpinfo->resultList; resCol; resCol = resCol->next) {
        iResCol++;
      }
      cols    = (DB2ResultColumn**)db2alloc(iResCol+1 * sizeof(DB2ResultColumn*),"cols(%d)",iResCol+1);
      iResCol = 0;
      for (resCol = fpinfo->resultList; resCol; resCol = resCol->next) {
        db2Debug2("resCol: %x", resCol);
        cols[iResCol] = resCol;
        db2Debug2("cols[%d]         : %x", iResCol, cols[iResCol]);
        db2Debug2("cols[%d]->colName: %s", iResCol, cols[iResCol]->colName);
        db2Debug2("cols[%d]->resnum : %d", iResCol, cols[iResCol]->resnum);
        iResCol++;
        db2Debug2("resCol->next: %x", resCol->next);
      }
      // sort the array in ascending order of the pgattnum, so that we can compare it with the order of columns in the foreign table
      db2Debug3("sorting result cols %d columns by pgattnum", iResCol);
      qsort(cols, iResCol, sizeof(DB2ResultColumn*), compareResultColumns);
      // generate the sorted array into the resultList
      fpinfo->resultList = NULL;
      for (int idx = 0, cidx = 0; idx < iResCol; idx++) {
        db2Debug3("result column %d: %s", idx, cols[idx]->colName);
        if (idx > 0) {
          // check if this column is the same as the one before and if so, skip it
          if (cols[idx]->pgattnum == cols[idx-1]->pgattnum) {
            continue;
          }
        }
        cols[idx]->next    = fpinfo->resultList;
        cols[idx]->resnum  = ++cidx;  // result number must be 1 - based
        db2Debug3("column %s added to result list with resnum %d", cols[idx]->colName, cols[idx]->resnum);
        fpinfo->resultList = cols[idx];
      }
      /* examine each condition for Var nodes */
      db2Debug3("size of conditions: %d", list_length(foreignrel->baserestrictinfo));
      foreach (cell, foreignrel->baserestrictinfo) {
        db2Debug3("examine condition");
        getUsedColumns ((Expr*) lfirst (cell), foreignrel, NULL);
      }
    }
    /* In a base-relation scan, we must apply the given scan_clauses.
     *
     * Separate the scan_clauses into those that can be executed remotely and those that can't.
     * baserestrictinfo clauses that were previously determined to be safe or unsafe by classifyConditions
     * are found in fpinfo->remote_conds and fpinfo->local_conds.
     * Anything else in the scan_clauses list will be a join clause, which we have to check for remote-safety.
     *
     * Note: the join clauses we see here should be the exact same ones previously examined by postgresGetForeignPaths.
     * Possibly it'd be worth passing forward the classification work done then, rather than repeating it here.
     *
     * This code must match "extract_actual_clauses(scan_clauses, false)" except for the additional decision about remote versus local execution.
     */
    foreach(lc, scan_clauses) {
      RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

      /* Ignore any pseudoconstants, they're dealt with elsewhere */
      if (rinfo->pseudoconstant)
        continue;
      if (list_member_ptr(fpinfo->remote_conds, rinfo))
        remote_exprs = lappend(remote_exprs, rinfo->clause);
      else if (list_member_ptr(fpinfo->local_conds, rinfo))
        local_exprs = lappend(local_exprs, rinfo->clause);
      else if (is_foreign_expr(root, foreignrel, rinfo->clause))
        remote_exprs = lappend(remote_exprs, rinfo->clause);
      else
        local_exprs = lappend(local_exprs, rinfo->clause);
    }

    /* For a base-relation scan, we have to support EPQ recheck, which should recheck all the remote quals. */
    fdw_recheck_quals = remote_exprs;
  } else {
    db2Debug3("join relation scan: set scan_relid to 0");
    /* Join relation or upper relation - set scan_relid to 0. */
    scan_relid = 0;

    /* For a join rel, baserestrictinfo is NIL and we are not considering parameterization right now, 
     * so there should be no scan_clauses for a joinrel or an upper rel either.
     */
    Assert(!scan_clauses);

    /* Instead we get the conditions to apply from the fdw_private structure. */
    remote_exprs  = extract_actual_clauses(fpinfo->remote_conds, false);
    local_exprs   = extract_actual_clauses(fpinfo->local_conds, false);

    /* We leave fdw_recheck_quals empty in this case, since we never need to apply EPQ recheck clauses.  In the case of a joinrel, EPQ
     * recheck is handled elsewhere --- see postgresGetForeignJoinPaths().
     * If we're planning an upperrel (ie, remote grouping or aggregation) then there's no EPQ to do because SELECT FOR UPDATE wouldn't be
     * allowed, and indeed we *can't* put the remote clauses into fdw_recheck_quals because the unaggregated Vars won't be available
     * locally.
     * 
     * Build the list of columns to be fetched from the foreign server.
     */
    if (ptlist_len > 0) {
      ListCell*   cell    = NULL;
      int         resnum  = 1;

      /* examine each condition for Tlist nodes; they come in the correct sequence as in the query and do not need to be sorted */
      db2Debug3("size of tlist: %d", ptlist_len);
      foreach (cell, ptlist) {
        DB2ResultColumn* resCol = (DB2ResultColumn*)db2alloc(sizeof(DB2ResultColumn),"DB2ResultColumn* resCol");
        db2Debug3("examine tlist");
        resCol->next       = fpinfo->resultList;
        fpinfo->resultList = resCol;
        resCol->resnum     = resnum;
        getUsedColumns ((Expr*) lfirst (cell), foreignrel, resCol);
        db2Debug3("result column %d: %s", resCol->resnum, resCol->colName);
        resnum++;
      }
    }

    /* Ensure that the outer plan produces a tuple whose descriptor matches our scan tuple slot.  Also, remove the local conditions
     * from outer plan's quals, lest they be evaluated twice, once by the local plan and once by the scan.
     */
    if (outer_plan) {
      db2Debug3("adjusting outer plan's targetlist and quals to match scan's needs");
      /* Right now, we only consider grouping and aggregation beyond joins. 
       * Queries involving aggregates or grouping do not require EPQ mechanism, hence should not have an outer plan here.
       */
      Assert(!IS_UPPER_REL(foreignrel));
      /* First, update the plan's qual list if possible.
       * In some cases the quals might be enforced below the topmost plan level, in which case we'll fail to remove them; it's not worth working
       * harder than this.
       */
      foreach(lc, local_exprs) {
        Node* qual  = lfirst(lc);

        outer_plan->qual = list_delete(outer_plan->qual, qual);
        /* For an inner join the local conditions of foreign scan plan can be part of the joinquals as well.
         * (They might also be in the mergequals or hashquals, but we can't touch those without breaking the plan.)
         */
        if (IsA(outer_plan, NestLoop) || IsA(outer_plan, MergeJoin) || IsA(outer_plan, HashJoin)) {
          Join* join_plan = (Join*) outer_plan;

          if (join_plan->jointype == JOIN_INNER)
            join_plan->joinqual = list_delete(join_plan->joinqual, qual);
        }
      }
      /* Now fix the subplan's tlist --- this might result in inserting a Result node atop the plan tree. */
      outer_plan = change_plan_targetlist(outer_plan, tlist, best_path->path.parallel_safe);
    }
  }

  /* Build the query string to be sent for execution, and identify expressions to be sent as parameters. */
  initStringInfo(&sql);
  deparseSelectStmtForRel(&sql, root, foreignrel, ptlist, remote_exprs, best_path->path.pathkeys, has_final_sort, has_limit, false, &retrieved_attrs, &params_list);
  db2Debug2("deparsed foreign query: %s", sql.data);
  /* Remember remote_exprs for possible use by postgresPlanDirectModify */
  fpinfo->final_remote_exprs = remote_exprs;

  /* Build the fdw_private list that will be available to the executor.
   * Items in the list must match order in enum FdwScanPrivateIndex.
   */
  fpinfo->query             = sql.data;
  fpinfo->retrieved_attr    = retrieved_attrs;
  fdw_private               = serializePlanData(fpinfo);
  
  /* Create the ForeignScan node for the given relation.
   *
   * Note that the remote parameter expressions are stored in the fdw_exprs
   * field of the finished plan node; we can't keep them in private state
   * because then they wouldn't be subject to later planner processing.
   */
  fscan = make_foreignscan(tlist, local_exprs, scan_relid, params_list, fdw_private, ptlist, fdw_recheck_quals, outer_plan);
  db2Exit1(": %x",fscan);
  return fscan;
}

/* getUsedColumns
 * Set "used=true" in db2Table for all columns used in the expression.
 */
static void getUsedColumns (Expr* expr, RelOptInfo* foreignrel, DB2ResultColumn* resCol) {
  ListCell* cell  = NULL;

  db2Entry3();
  if (expr != NULL) {
    db2Debug4("examine node of type: %d", expr->type);
    switch (expr->type) {
      case T_RestrictInfo:
        getUsedColumns (((RestrictInfo*) expr)->clause, foreignrel, resCol);
      break;
      case T_TargetEntry:
        getUsedColumns (((TargetEntry*) expr)->expr, foreignrel, resCol);
      break;
      case T_Const:
      case T_Param:
      case T_CaseTestExpr:
      case T_CoerceToDomainValue:
      case T_CurrentOfExpr:
      case T_NextValueExpr:
      break;
      case T_Var: {
        DB2FdwState*  fpinfo  = (DB2FdwState*) foreignrel->fdw_private;
        Var*          var     = NULL;
        int           index   = 0;

        var = (Var*) expr;
        db2Debug4("var->varattno: %d", var->varattno); 
        /* ignore system columns */
        if (var->varattno < 0)
          break;
        /* if this is a wholerow reference, we need all columns */
        if (var->varattno == 0) {
          DB2ResultColumn* tmpCol = NULL;
          db2Debug4("found whole-row reference, need to add all columns");
          db2Debug4("fpinfo->resultList: %x", fpinfo->resultList);
          // add all columns but the last one here
          for (index = 0; index < (fpinfo->db2Table->ncols - 1); index++) {
            if (fpinfo->db2Table->cols[index]->pgname) {
              tmpCol = (DB2ResultColumn*)db2alloc(sizeof(DB2ResultColumn),"tmpCol");
              tmpCol->resnum     = index+1;
              copyCol2Result(tmpCol,fpinfo->db2Table->cols[index]);
              db2Debug4("db2Table[%d]->colName %s added to result list", index, fpinfo->db2Table->cols[index]->colName);
              tmpCol->next       = fpinfo->resultList;
              db2Debug4("tmpCol-next: %x", tmpCol->next);
              fpinfo->resultList = tmpCol;
              db2Debug4("fpinfo->resultList: %x", fpinfo->resultList);
            }
          }
          // now add the last colum using the resCol passed in, so that the column name in the result list is correct for whole row reference
          copyCol2Result(resCol,fpinfo->db2Table->cols[index]);
          resCol->resnum     = index+1;
          db2Debug4("db2Table[%d]->colName %s added to result list", index, fpinfo->db2Table->cols[index]->colName);
          break;
        } else {
          /* get db2Table column index corresponding to this column (-1 if none) */
          index = fpinfo->db2Table->ncols - 1;
          while (index >= 0 && fpinfo->db2Table->cols[index]->pgattnum != var->varattno) {
            --index;
          }
          if (index == -1) {
            ereport (WARNING, (errcode (ERRCODE_WARNING),errmsg ("column number %d of foreign table \"%s\" does not exist in foreign DB2 table, will be replaced by NULL", var->varattno, fpinfo->db2Table->pgname)));
          } else {
            copyCol2Result(resCol,fpinfo->db2Table->cols[index]);
          }
        }
      }
      break;
      case T_Aggref: {
        Aggref* aggref = (Aggref*) expr;
        /* Resolve aggregate function name (OID -> pg_proc.proname). */
        HeapTuple tuple   = SearchSysCache1(PROCOID, ObjectIdGetDatum(aggref->aggfnoid));
        char*     aggname = NULL;
        char*     nspname = NULL;
        if (HeapTupleIsValid(tuple)) {
          Form_pg_proc procform = (Form_pg_proc) GETSTRUCT(tuple);
          aggname = pstrdup(NameStr(procform->proname));
          /* Optional: capture schema for debugging/qualification decisions. */
          if (OidIsValid(procform->pronamespace)) {
            HeapTuple ntup = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(procform->pronamespace));
            if (HeapTupleIsValid(ntup)) {
              Form_pg_namespace nspform = (Form_pg_namespace) GETSTRUCT(ntup);
              nspname = pstrdup(NameStr(nspform->nspname));
              ReleaseSysCache(ntup);
            }
          }
          ReleaseSysCache(tuple);
        }
        db2Debug4("aggref->aggfnoid=%u name=%s%s%s", aggref->aggfnoid, nspname ? nspname : "", nspname ? "." : "", aggname ? aggname : "<unknown>");
        if (aggname && strcmp(aggname, "count") == 0) {
          DB2FdwState*  fpinfo  = (DB2FdwState*) foreignrel->fdw_private;
          /* if it's a COUNT(*) then we need an additional result */
          DB2Column* col = db2alloc(sizeof(DB2Column),"DB2Column* col");
          db2Debug4("found COUNT aggregate");
          col->colName        = "count";
          col->colType        = -5; // SQL_BIGINT type in DB2, which can hold the result of COUNT(*)
          col->colSize        = 8;
          col->colScale       = 0;
          col->colNulls       = 1;
          col->colChars       = 23; // max number of characters needed to represent a 8-byte integer, including sign
          col->colBytes       = 8;
          col->colPrimKeyPart = 0;
          col->colCodepage    = 0; 
          col->pgname         = "count";
          col->pgattnum       = 0;
          col->pgtype         = INT8OID;
          col->pgtypmod       = -1;
          col->used           = 1;
          col->pkey           = 0;
          col->val_size       = 24;
          col->noencerr       = fpinfo->db2Table->cols[0]->noencerr; // use same noencerr as first column 
          copyCol2Result(resCol,col);
        } else {
          db2Debug4("count aggref->args: %d",list_length(aggref->args));
          foreach (cell, aggref->args) {
            getUsedColumns ((Expr*) lfirst (cell), foreignrel, resCol);
          }
          foreach (cell, aggref->aggorder) {
            getUsedColumns ((Expr*) lfirst (cell), foreignrel, resCol);
          }
          foreach (cell, aggref->aggdistinct) {
            getUsedColumns ((Expr*) lfirst (cell), foreignrel, resCol);
          }
        }
      }
      break;
      case T_WindowFunc:
        foreach (cell, ((WindowFunc*) expr)->args) {
          getUsedColumns ((Expr*) lfirst (cell), foreignrel, resCol);
        }
      break;
      case T_SubscriptingRef: {
        SubscriptingRef* ref = (SubscriptingRef*) expr;
        foreach(cell, ref->refupperindexpr) {
          getUsedColumns((Expr*)lfirst(cell), foreignrel, resCol);
        }
        foreach(cell, ref->reflowerindexpr) {
          getUsedColumns((Expr*)lfirst(cell), foreignrel, resCol);
        }
        getUsedColumns(ref->refexpr, foreignrel, resCol);
        getUsedColumns(ref->refassgnexpr, foreignrel, resCol);
      }
      break;
      case T_FuncExpr:
        foreach (cell, ((FuncExpr*) expr)->args) {
          getUsedColumns ((Expr*) lfirst (cell), foreignrel, resCol);
        }
      break;
      case T_OpExpr:
        foreach (cell, ((OpExpr*) expr)->args) {
          getUsedColumns ((Expr*) lfirst (cell), foreignrel, resCol);
        }
      break;
      case T_DistinctExpr:
        foreach (cell, ((DistinctExpr*) expr)->args) {
          getUsedColumns ((Expr*) lfirst (cell), foreignrel, resCol);
        }
      break;
      case T_NullIfExpr:
        foreach (cell, ((NullIfExpr*) expr)->args) {
          getUsedColumns ((Expr*) lfirst (cell), foreignrel, resCol);
        }
      break;
      case T_ScalarArrayOpExpr:
        foreach (cell, ((ScalarArrayOpExpr*) expr)->args) {
          getUsedColumns ((Expr*) lfirst (cell), foreignrel, resCol);
        }
      break;
      case T_BoolExpr:
        foreach (cell, ((BoolExpr*) expr)->args) {
          getUsedColumns ((Expr*) lfirst (cell), foreignrel, resCol);
        }
      break;
      case T_SubPlan:
        foreach (cell, ((SubPlan*) expr)->args) {
          getUsedColumns ((Expr*) lfirst (cell), foreignrel, resCol);
        }
      break;
      case T_AlternativeSubPlan:
        /* examine only first alternative */
        getUsedColumns ((Expr*) linitial (((AlternativeSubPlan*) expr)->subplans), foreignrel, resCol);
      break;
      case T_NamedArgExpr:
        getUsedColumns (((NamedArgExpr*) expr)->arg, foreignrel, resCol);
      break;
      case T_FieldSelect:
        getUsedColumns (((FieldSelect*) expr)->arg, foreignrel, resCol);
      break;
      case T_RelabelType:
        getUsedColumns (((RelabelType*) expr)->arg, foreignrel, resCol);
      break;
      case T_CoerceViaIO:
        getUsedColumns (((CoerceViaIO*) expr)->arg, foreignrel, resCol);
      break;
      case T_ArrayCoerceExpr:
        getUsedColumns (((ArrayCoerceExpr*) expr)->arg, foreignrel, resCol);
      break;
      case T_ConvertRowtypeExpr:
        getUsedColumns (((ConvertRowtypeExpr*) expr)->arg, foreignrel, resCol);
      break;
      case T_CollateExpr:
        getUsedColumns (((CollateExpr*) expr)->arg, foreignrel, resCol);
      break;
      case T_CaseExpr:
        foreach (cell, ((CaseExpr*) expr)->args) {
          getUsedColumns ((Expr*) lfirst (cell), foreignrel, resCol);
        }
        getUsedColumns (((CaseExpr*) expr)->arg, foreignrel, resCol);
        getUsedColumns (((CaseExpr*) expr)->defresult, foreignrel, resCol);
      break;
      case T_CaseWhen:
        getUsedColumns (((CaseWhen*) expr)->expr, foreignrel, resCol);
        getUsedColumns (((CaseWhen*) expr)->result, foreignrel, resCol);
      break;
      case T_ArrayExpr:
        foreach (cell, ((ArrayExpr*) expr)->elements) {
          getUsedColumns ((Expr*) lfirst (cell), foreignrel, resCol);
        }
      break;
      case T_RowExpr:
        foreach (cell, ((RowExpr*) expr)->args) {
          getUsedColumns ((Expr*) lfirst (cell), foreignrel, resCol);
        }
      break;
      case T_RowCompareExpr:
        foreach (cell, ((RowCompareExpr*) expr)->largs) {
          getUsedColumns ((Expr*) lfirst (cell), foreignrel, resCol);
        }
        foreach (cell, ((RowCompareExpr*) expr)->rargs) {
          getUsedColumns ((Expr*) lfirst (cell), foreignrel, resCol);
        }
      break;
      case T_CoalesceExpr:
        foreach (cell, ((CoalesceExpr*) expr)->args) {
          getUsedColumns ((Expr*) lfirst (cell), foreignrel, resCol);
        }
      break;
      case T_MinMaxExpr:
        foreach (cell, ((MinMaxExpr*) expr)->args) {
          getUsedColumns ((Expr*) lfirst (cell), foreignrel, resCol);
        }
      break;
      case T_XmlExpr:
        foreach (cell, ((XmlExpr*) expr)->named_args) {
          getUsedColumns ((Expr*) lfirst (cell), foreignrel, resCol);
        }
        foreach (cell, ((XmlExpr*) expr)->args) {
          getUsedColumns ((Expr*) lfirst (cell), foreignrel, resCol);
        }
      break;
      case T_NullTest:
        getUsedColumns (((NullTest*) expr)->arg, foreignrel, resCol);
      break;
      case T_BooleanTest:
        getUsedColumns (((BooleanTest*) expr)->arg, foreignrel, resCol);
      break;
      case T_CoerceToDomain:
        getUsedColumns (((CoerceToDomain*) expr)->arg, foreignrel, resCol);
      break;
      case T_PlaceHolderVar:
        getUsedColumns (((PlaceHolderVar*) expr)->phexpr, foreignrel, resCol);
      break;
      case T_SQLValueFunction:
        //nop
      break;                                /* contains no column references */
      default:
        /* We must be able to handle all node types that can appear because we cannot omit a column from the remote query that will be needed.
         * Throw an error if we encounter an unexpected node type.
         */
        ereport (ERROR, (errcode (ERRCODE_FDW_UNABLE_TO_CREATE_REPLY), errmsg ("Internal db2_fdw error: encountered unknown node type %d.", expr->type)));
       break;
    }
  }
  db2Exit3();
}

/* copyCol2Result
 * Copy the column information from the db2Table column to the result column.
 */
static void copyCol2Result(DB2ResultColumn* resCol, DB2Column* column) {
  db2Entry4();
  if (resCol && resCol->colName == NULL) {
    resCol->colName        = db2strdup(column->colName,"resCol->colName");
    resCol->colType        = column->colType;
    resCol->colSize        = column->colSize;
    resCol->colScale       = column->colScale;
    resCol->colNulls       = column->colNulls;
    resCol->colChars       = column->colChars;
    resCol->colBytes       = column->colBytes;
    resCol->colPrimKeyPart = column->colPrimKeyPart;
    resCol->colCodepage    = column->colCodepage;
    resCol->pgbaserelid    = column->pgrelid;
    resCol->pgname         = db2strdup(column->pgname,"resCol->pgname");
    resCol->pgattnum       = column->pgattnum;
    resCol->pgtype         = column->pgtype;
    resCol->pgtypmod       = column->pgtypmod;
    resCol->pkey           = column->pkey;
    resCol->val_size       = column->val_size;
    resCol->noencerr       = column->noencerr;
  }
  db2Exit4();
}

/* compareResultColumns
 * Compare two DB2ResultColumn pointers by their pgattnum.
 */
static int compareResultColumns(const void* a, const void* b) {
  DB2ResultColumn* colA = *(DB2ResultColumn**) a;
  DB2ResultColumn* colB = *(DB2ResultColumn**) b;
  int result = 0;
  db2Entry4();
  result = (colA->pgattnum - colB->pgattnum);
  db2Debug5("comparing %s -> pgattnum %d and %s -> pgattnum %d, result = %d", colA->colName, colA->pgattnum, colB->colName, colB->pgattnum, result);
  db2Exit4(": %d", result);
  return result;
}