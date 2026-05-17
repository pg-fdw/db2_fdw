#include <postgres.h>
#include <common/md5.h>
#include <optimizer/planmain.h>
#include <optimizer/tlist.h>
#include <parser/parsetree.h>
#include <optimizer/optimizer.h>
#include <access/heapam.h>
#include "db2_fdw.h"
#include "DB2FdwState.h"

/** external prototypes */
extern List*        serializePlanData         (DB2FdwState* fdwState);
extern void         checkDataType             (short db2type, int scale, Oid pgtype, const char* tablename, const char* colname);
extern char*        deparseExpr               (PlannerInfo* root, RelOptInfo* rel, Expr* expr, List** params);
extern char*        get_jointype_name         (JoinType jointype);
extern List*        build_tlist_to_deparse    (RelOptInfo* foreignrel);

/** local prototypes */
ForeignScan* db2GetForeignPlan     (PlannerInfo* root, RelOptInfo* foreignrel, Oid foreigntableid, ForeignPath* best_path, List* tlist, List* scan_clauses , Plan* outer_plan);
static void  createQuery           (PlannerInfo* root, RelOptInfo* foreignrel, bool modify, List* query_pathkeys);
static void  getUsedColumns        (Expr* expr, DB2Table* db2Table, int foreignrelid);
static void  deparseFromExprForRel (PlannerInfo* root, RelOptInfo* foreignrel, StringInfo buf, List** params_list);

/** db2GetForeignPlan
 *   Construct a ForeignScan node containing the serialized DB2FdwState,
 *   the RestrictInfo clauses not handled entirely by DB2 and the list
 *   of parameters we need for execution.
 */
ForeignScan* db2GetForeignPlan (PlannerInfo* root, RelOptInfo* foreignrel, Oid foreigntableid, ForeignPath* best_path, List* tlist, List* scan_clauses , Plan* outer_plan) {
  DB2FdwState* fdwState    = (DB2FdwState*) foreignrel->fdw_private;
  List*        fdw_private = NIL;
  int          i;
  bool         need_keys  = false, 
               for_update = false,
               has_trigger;
  Relation     rel;
  Index        scan_relid;                               /* will be 0 for join relations */
  List*        local_exprs    = fdwState->local_conds;
  List*        fdw_scan_tlist = NIL;
  ForeignScan* result         = NULL;

  db2Debug1("> %s::db2GetForeignPlan",__FILE__);
  /* treat base relations and join relations differently */
  if (IS_SIMPLE_REL (foreignrel)) {
    /* for base relations, set scan_relid as the relid of the relation */
    scan_relid = foreignrel->relid;
    /* check if the foreign scan is for an UPDATE or DELETE */
    #if PG_VERSION_NUM < 140000
    if (foreignrel->relid == root->parse->resultRelation && (root->parse->commandType == CMD_UPDATE || root->parse->commandType == CMD_DELETE)) {
    #else
    if (bms_is_member(foreignrel->relid, root->all_result_relids) && (root->parse->commandType == CMD_UPDATE || root->parse->commandType == CMD_DELETE)) {
    #endif  /* PG_VERSION_NUM */
      /* we need the table's primary key columns */
      need_keys = true;
    }
    /* check if FOR [KEY] SHARE/UPDATE was specified */
    if (need_keys || get_parse_rowmark (root->parse, foreignrel->relid)) {
      /* we should add FOR UPDATE */
      for_update = true;
    }
    if (need_keys) {
      /* we need to fetch all primary key columns */
      for (i = 0; i < fdwState->db2Table->ncols; ++i) {
        if (fdwState->db2Table->cols[i]->colPrimKeyPart) {
          fdwState->db2Table->cols[i]->used = 1;
        }
      }
    }
    /*
     * Core code already has some lock on each rel being planned, so we can
     * use NoLock here.
     */
    rel = table_open (foreigntableid, NoLock);
    /* is there an AFTER trigger FOR EACH ROW? */
    has_trigger = (foreignrel->relid == root->parse->resultRelation) 
                && rel->trigdesc 
                && ((root->parse->commandType == CMD_UPDATE && rel->trigdesc->trig_update_after_row) || (root->parse->commandType == CMD_DELETE && rel->trigdesc->trig_delete_after_row));
    table_close (rel, NoLock);
    if (has_trigger) {
      /* we need to fetch and return all columns */
      for (i = 0; i < fdwState->db2Table->ncols; ++i) {
        if (fdwState->db2Table->cols[i]->pgname) {
          fdwState->db2Table->cols[i]->used = 1;
        }
      }
    }
  } else {
    /* we have a join relation, so set scan_relid to 0 */
    scan_relid = 0;
    /*
     * create_scan_plan() and create_foreignscan_plan() pass
     * rel->baserestrictinfo + parameterization clauses through
     * scan_clauses. For a join rel->baserestrictinfo is NIL and we are
     * not considering parameterization right now, so there should be no
     * scan_clauses for a joinrel.
     */
    Assert (!scan_clauses);
    /* Build the list of columns to be fetched from the foreign server. */
    fdw_scan_tlist = build_tlist_to_deparse (foreignrel);
    /*
     * Ensure that the outer plan produces a tuple whose descriptor
     * matches our scan tuple slot. This is safe because all scans and
     * joins support projection, so we never need to insert a Result node.
     * Also, remove the local conditions from outer plan's quals, lest
     * they will be evaluated twice, once by the local plan and once by
     * the scan.
     */
    if (outer_plan) {
      ListCell* lc;
      outer_plan->targetlist = fdw_scan_tlist;
      foreach (lc, local_exprs) {
        Join* join_plan  = (Join*) outer_plan;
        Node* qual       = lfirst (lc);
        outer_plan->qual = list_delete (outer_plan->qual, qual);
        /*
         * For an inner join the local conditions of foreign scan plan
         * can be part of the joinquals as well.
         */
        if (join_plan->jointype == JOIN_INNER) {
           join_plan->joinqual = list_delete (join_plan->joinqual, qual);
        }
      }
    }
  }
  /* create remote query */
  createQuery (root, foreignrel, for_update, best_path->path.pathkeys);
  db2Debug2("  db2_fdw: remote query is: %s", fdwState->query);
  /* get PostgreSQL column data types, check that they match DB2's */
  for (i = 0; i < fdwState->db2Table->ncols; ++i) {
    if (fdwState->db2Table->cols[i]->used) {
      checkDataType (fdwState->db2Table->cols[i]->colType
                    ,fdwState->db2Table->cols[i]->colScale
                    ,fdwState->db2Table->cols[i]->pgtype
                    ,fdwState->db2Table->pgname
                    ,fdwState->db2Table->cols[i]->pgname
                    );
    }
  }
  fdw_private = serializePlanData (fdwState);
  /*
   * Create the ForeignScan node for the given relation.
   *
   * Note that the remote parameter expressions are stored in the fdw_exprs
   * field of the finished plan node; we can't keep them in private state
   * because then they wouldn't be subject to later planner processing.
   */

  result = make_foreignscan (tlist, local_exprs, scan_relid, fdwState->params, fdw_private, fdw_scan_tlist, NIL, outer_plan);
  db2Debug1("< %s::db2GetForeignPlan",__FILE__);
  return result;
}

/** createQuery
 *   Construct a query string for DB2 that
 *   a) contains only the necessary columns in the SELECT list
 *   b) has all the WHERE and ORDER BY clauses that can safely be translated to DB2.
 *   Untranslatable clauses are omitted and left for PostgreSQL to check.
 *   "query_pathkeys" contains the desired sort order of the scan results
 *   which will be translated to ORDER BY clauses if possible.
 *   As a side effect for base relations, we also mark the used columns in db2Table.
 */
static void createQuery (PlannerInfo* root, RelOptInfo* foreignrel, bool modify, List* query_pathkeys) {
  DB2FdwState*   fdwState = (DB2FdwState*) foreignrel->fdw_private;
  ListCell*      cell;
  bool           in_quote = false;
  int            i, index;
  char*          wherecopy, *p, md5[33], parname[10], *separator = "";
  StringInfoData query, result;
  List*          columnlist, *conditions = foreignrel->baserestrictinfo;
  #if PG_VERSION_NUM >= 150000
  const char*    errstr = NULL;
  #endif

  db2Debug1("> %s::createQuery",__FILE__);
  columnlist = foreignrel->reltarget->exprs;
  if (IS_SIMPLE_REL (foreignrel)) {
    db2Debug3("  IS_SIMPLE_REL");
    /* find all the columns to include in the select list */
    /* examine each SELECT list entry for Var nodes */
    db2Debug3("  size of columnlist: %d", list_length(columnlist));
    foreach (cell, columnlist) {
      db2Debug3("  examine column");
      getUsedColumns ((Expr*) lfirst (cell), fdwState->db2Table, foreignrel->relid);
    }
    /* examine each condition for Var nodes */
    db2Debug3("  size of conditions: %d", list_length(conditions));
    foreach (cell, conditions) {
      db2Debug3("  examine condition");
      getUsedColumns ((Expr*) lfirst (cell), fdwState->db2Table, foreignrel->relid);
    }
  }

  /* construct SELECT list */
  initStringInfo (&query);
  for (i = 0; i < fdwState->db2Table->ncols; ++i) {
    db2Debug2("  %s.%s.->used: %d",fdwState->db2Table->name,fdwState->db2Table->cols[i]->colName,fdwState->db2Table->cols[i]->used);
    if (fdwState->db2Table->cols[i]->used) {
      StringInfoData alias;
      initStringInfo (&alias);
      /* table alias is created from range table index */
      ADD_REL_QUALIFIER (&alias, fdwState->db2Table->cols[i]->varno);

      /* add qualified column name */
      appendStringInfo (&query, "%s%s%s", separator, alias.data, fdwState->db2Table->cols[i]->colName);
      separator = ", ";
    }
  }

  /* dummy column if there is no result column we need from DB2 */
  if (separator[0] == '\0')
    appendStringInfo (&query, "'1'");

  /* append FROM clause */
  appendStringInfo (&query, " FROM ");
  deparseFromExprForRel (root, foreignrel, &query, &(fdwState->params));
  db2Debug2("  append from clause: %s", query.data);
  /*
   * For inner joins, all conditions that are pushed down get added
   * to fdwState->joinclauses and have already been added above,
   * so there is no extra WHERE clause.
   */
  if (IS_SIMPLE_REL (foreignrel)) {
    /* append WHERE clauses */
    if (fdwState->where_clause)
      appendStringInfo (&query, "%s", fdwState->where_clause);
  }
  db2Debug2("  append where clause: %s", query.data);

  /* append ORDER BY clause if all its expressions can be pushed down */
  if (fdwState->order_clause)
    appendStringInfo (&query, " ORDER BY%s", fdwState->order_clause);
  db2Debug2("  append order by clause: %s", query.data);

  /* append FOR UPDATE if if the scan is for a modification */
  if (modify)
    appendStringInfo (&query, " FOR UPDATE");
  db2Debug2("  append modify clause: %s", query.data);

  /* get a copy of the where clause without single quoted string literals */
  wherecopy = db2strdup (query.data,"wherecopy");
  for (p = wherecopy; *p != '\0'; ++p) {
    if (*p == '\'')
      in_quote = !in_quote;
    if (in_quote)
      *p = ' ';
  }

  /* remove all parameters that do not actually occur in the query */
  index = 0;
  foreach (cell, fdwState->params) {
    ++index;
    snprintf (parname, 10, ":p%d", index);
    if (strstr (wherecopy, parname) == NULL) {
      /* set the element to NULL to indicate it's gone */
      lfirst (cell) = NULL;
    }
  }

  db2free (wherecopy,"wherecopy");

  /*
   * Calculate MD5 hash of the query string so far.
   * This is needed to find the query in DB2's library cache for EXPLAIN.
   */
#if PG_VERSION_NUM >= 150000
  if (!pg_md5_hash (query.data, strlen (query.data), md5,&errstr)) {
    ereport (ERROR, (errcode (ERRCODE_OUT_OF_MEMORY), errmsg ("out of memory")));
  }
#else
if (!pg_md5_hash (query.data, strlen (query.data), md5)) {
     ereport (ERROR, (errcode (ERRCODE_OUT_OF_MEMORY), errmsg ("out of memory")));
  }
#endif
  /* add comment with MD5 hash to query */
  initStringInfo (&result);
  appendStringInfo (&result, "SELECT /*%s*/ %s", md5, query.data);
  db2free (query.data,"query.data");
  fdwState->query = (result.len > 0) ? db2strdup(result.data,"result.data") : NULL;
  db2free(result.data,"result.data");
  db2Debug2("  query: %s",fdwState->query);
  db2Debug1("< createQuery");
}

/** getUsedColumns
 *   Set "used=true" in db2Table for all columns used in the expression.
 */
static void getUsedColumns (Expr* expr, DB2Table* db2Table, int foreignrelid) {
  ListCell* cell;
  Var*      variable;
  int       index;

  db2Debug1("> getUsedColumns");
  if (expr != NULL) {
    switch (expr->type) {
      case T_RestrictInfo:
        getUsedColumns (((RestrictInfo*) expr)->clause, db2Table, foreignrelid);
      break;
      case T_TargetEntry:
        getUsedColumns (((TargetEntry*) expr)->expr, db2Table, foreignrelid);
      break;
      case T_Const:
      case T_Param:
      case T_CaseTestExpr:
      case T_CoerceToDomainValue:
      case T_CurrentOfExpr:
      case T_NextValueExpr:
      break;
      case T_Var:
        variable = (Var*) expr;
        /* ignore system columns */
        if (variable->varattno < 0)
          break;
        /* if this is a wholerow reference, we need all columns */
        if (variable->varattno == 0) {
          for (index = 0; index < db2Table->ncols; ++index)
            if (db2Table->cols[index]->pgname)
              db2Table->cols[index]->used = 1;
          break;
        }
        /* get db2Table column index corresponding to this column (-1 if none) */
        index = db2Table->ncols - 1;
        while (index >= 0 && db2Table->cols[index]->pgattnum != variable->varattno) {
          --index;
        }
        if (index == -1) {
          ereport (WARNING, (errcode (ERRCODE_WARNING),errmsg ("column number %d of foreign table \"%s\" does not exist in foreign DB2 table, will be replaced by NULL", variable->varattno, db2Table->pgname)));
        } else {
          db2Table->cols[index]->used = 1;
        }
      break;
      case T_Aggref:
        foreach (cell, ((Aggref*) expr)->args) {
          getUsedColumns ((Expr*) lfirst (cell), db2Table, foreignrelid);
        }
        foreach (cell, ((Aggref*) expr)->aggorder) {
          getUsedColumns ((Expr*) lfirst (cell), db2Table, foreignrelid);
        }
        foreach (cell, ((Aggref*) expr)->aggdistinct) {
          getUsedColumns ((Expr*) lfirst (cell), db2Table, foreignrelid);
        }
      break;
      case T_WindowFunc:
        foreach (cell, ((WindowFunc*) expr)->args) {
          getUsedColumns ((Expr*) lfirst (cell), db2Table, foreignrelid);
        }
      break;
      case T_SubscriptingRef: {
        SubscriptingRef* ref = (SubscriptingRef*) expr;
        foreach(cell, ref->refupperindexpr) {
          getUsedColumns((Expr*)lfirst(cell), db2Table, foreignrelid);
        }
        foreach(cell, ref->reflowerindexpr) {
          getUsedColumns((Expr*)lfirst(cell), db2Table, foreignrelid);
        }
        getUsedColumns(ref->refexpr, db2Table, foreignrelid);
        getUsedColumns(ref->refassgnexpr, db2Table, foreignrelid);
      }
      break;
      case T_FuncExpr:
        foreach (cell, ((FuncExpr*) expr)->args) {
          getUsedColumns ((Expr*) lfirst (cell), db2Table, foreignrelid);
        }
      break;
      case T_OpExpr:
        foreach (cell, ((OpExpr*) expr)->args) {
          getUsedColumns ((Expr*) lfirst (cell), db2Table, foreignrelid);
        }
      break;
      case T_DistinctExpr:
        foreach (cell, ((DistinctExpr*) expr)->args) {
          getUsedColumns ((Expr*) lfirst (cell), db2Table, foreignrelid);
        }
      break;
      case T_NullIfExpr:
        foreach (cell, ((NullIfExpr*) expr)->args) {
          getUsedColumns ((Expr*) lfirst (cell), db2Table, foreignrelid);
        }
      break;
      case T_ScalarArrayOpExpr:
        foreach (cell, ((ScalarArrayOpExpr*) expr)->args) {
          getUsedColumns ((Expr*) lfirst (cell), db2Table, foreignrelid);
        }
      break;
      case T_BoolExpr:
        foreach (cell, ((BoolExpr*) expr)->args) {
          getUsedColumns ((Expr*) lfirst (cell), db2Table, foreignrelid);
        }
      break;
      case T_SubPlan:
        foreach (cell, ((SubPlan*) expr)->args) {
          getUsedColumns ((Expr*) lfirst (cell), db2Table, foreignrelid);
        }
      break;
      case T_AlternativeSubPlan:
        /* examine only first alternative */
        getUsedColumns ((Expr*) linitial (((AlternativeSubPlan*) expr)->subplans), db2Table, foreignrelid);
      break;
      case T_NamedArgExpr:
        getUsedColumns (((NamedArgExpr*) expr)->arg, db2Table, foreignrelid);
      break;
      case T_FieldSelect:
        getUsedColumns (((FieldSelect*) expr)->arg, db2Table, foreignrelid);
      break;
      case T_RelabelType:
        getUsedColumns (((RelabelType*) expr)->arg, db2Table, foreignrelid);
      break;
      case T_CoerceViaIO:
        getUsedColumns (((CoerceViaIO*) expr)->arg, db2Table, foreignrelid);
      break;
      case T_ArrayCoerceExpr:
        getUsedColumns (((ArrayCoerceExpr*) expr)->arg, db2Table, foreignrelid);
      break;
      case T_ConvertRowtypeExpr:
        getUsedColumns (((ConvertRowtypeExpr*) expr)->arg, db2Table, foreignrelid);
      break;
      case T_CollateExpr:
        getUsedColumns (((CollateExpr*) expr)->arg, db2Table, foreignrelid);
      break;
      case T_CaseExpr:
        foreach (cell, ((CaseExpr*) expr)->args) {
          getUsedColumns ((Expr*) lfirst (cell), db2Table, foreignrelid);
        }
        getUsedColumns (((CaseExpr*) expr)->arg, db2Table, foreignrelid);
        getUsedColumns (((CaseExpr*) expr)->defresult, db2Table, foreignrelid);
      break;
      case T_CaseWhen:
        getUsedColumns (((CaseWhen*) expr)->expr, db2Table, foreignrelid);
        getUsedColumns (((CaseWhen*) expr)->result, db2Table, foreignrelid);
      break;
      case T_ArrayExpr:
        foreach (cell, ((ArrayExpr*) expr)->elements) {
          getUsedColumns ((Expr*) lfirst (cell), db2Table, foreignrelid);
        }
      break;
      case T_RowExpr:
        foreach (cell, ((RowExpr*) expr)->args) {
          getUsedColumns ((Expr*) lfirst (cell), db2Table, foreignrelid);
        }
      break;
      case T_RowCompareExpr:
        foreach (cell, ((RowCompareExpr*) expr)->largs) {
          getUsedColumns ((Expr*) lfirst (cell), db2Table, foreignrelid);
        }
        foreach (cell, ((RowCompareExpr*) expr)->rargs) {
          getUsedColumns ((Expr*) lfirst (cell), db2Table, foreignrelid);
        }
      break;
      case T_CoalesceExpr:
        foreach (cell, ((CoalesceExpr*) expr)->args) {
          getUsedColumns ((Expr*) lfirst (cell), db2Table, foreignrelid);
        }
      break;
      case T_MinMaxExpr:
        foreach (cell, ((MinMaxExpr*) expr)->args) {
          getUsedColumns ((Expr*) lfirst (cell), db2Table, foreignrelid);
        }
      break;
      case T_XmlExpr:
        foreach (cell, ((XmlExpr*) expr)->named_args) {
          getUsedColumns ((Expr*) lfirst (cell), db2Table, foreignrelid);
        }
        foreach (cell, ((XmlExpr*) expr)->args) {
          getUsedColumns ((Expr*) lfirst (cell), db2Table, foreignrelid);
        }
      break;
      case T_NullTest:
        getUsedColumns (((NullTest*) expr)->arg, db2Table, foreignrelid);
      break;
      case T_BooleanTest:
        getUsedColumns (((BooleanTest*) expr)->arg, db2Table, foreignrelid);
      break;
      case T_CoerceToDomain:
        getUsedColumns (((CoerceToDomain*) expr)->arg, db2Table, foreignrelid);
      break;
      case T_PlaceHolderVar:
        getUsedColumns (((PlaceHolderVar*) expr)->phexpr, db2Table, foreignrelid);
      break;
      case T_SQLValueFunction:
        //nop
      break;                                /* contains no column references */
      default:
        /*
         * We must be able to handle all node types that can
         * appear because we cannot omit a column from the remote
         * query that will be needed.
         * Throw an error if we encounter an unexpected node type.
         */
        ereport (ERROR, (errcode (ERRCODE_FDW_UNABLE_TO_CREATE_REPLY), errmsg ("Internal db2_fdw error: encountered unknown node type %d.", expr->type)));
       break;
    }
  }
  db2Debug1("< getUsedColumns");
}

/** Build the targetlist for given relation to be deparsed as SELECT clause.
 *
 * The output targetlist contains the columns that need to be fetched from the
 * foreign server for the given relation.
 */
//static List* build_tlist_to_deparse (RelOptInfo* foreignrel) {
//  List*         tlist    = NIL;
//  DB2FdwState*  fdwState = (DB2FdwState*) foreignrel->fdw_private;
//
//  db2Debug1("> build_tlist_to_deparse");
//  /* We require columns specified in foreignrel->reltarget->exprs and those required for evaluating the local conditions. */
//  tlist = add_to_flat_tlist (tlist, pull_var_clause ((Node *) foreignrel->reltarget->exprs, PVC_RECURSE_PLACEHOLDERS));
//  tlist = add_to_flat_tlist(tlist, pull_var_clause((Node*) fdwState->local_conds, PVC_RECURSE_PLACEHOLDERS));
//  db2Debug1("< build_tlist_to_deparse");
//  return tlist;
//}

/** deparseFromExprForRel
 *   Construct FROM clause for given relation.
 *   The function constructs ... JOIN ... ON ... for join relation. For a base
 *   relation it just returns the table name.
 *   All tables get an alias based on the range table index.
 */
static void deparseFromExprForRel (PlannerInfo* root, RelOptInfo* foreignrel, StringInfo buf, List** params_list) {
  DB2FdwState* fdwState = (DB2FdwState*) foreignrel->fdw_private;

  db2Debug1("> %s::deparseFromExprForRel",__FILE__);
  db2Debug2("  buf: '%s'",buf->data);
  if (IS_SIMPLE_REL (foreignrel)) {
    db2Debug2("  IS_SIMPLE_REL(%d)",foreignrel->relid);
    appendStringInfo (buf, "%s", fdwState->db2Table->name);
    db2Debug2("  buf: '%s'",buf->data);
    appendStringInfo (buf, " %s%d", REL_ALIAS_PREFIX, foreignrel->relid);
    db2Debug2("  buf: '%s'",buf->data);
  } else {
    if (IS_JOIN_REL(foreignrel)) {
      db2Debug2("  IS_JOIN_REL(%d)",foreignrel->relid);
      /* join relation */
      RelOptInfo*    rel_o      = fdwState->outerrel;
      RelOptInfo*    rel_i      = fdwState->innerrel;
      StringInfoData join_sql_o;
      StringInfoData join_sql_i;
      ListCell*      lc         = NULL;
      bool           is_first   = true;
      char*          where      = NULL;
  
      /* Deparse outer relation */
      initStringInfo (&join_sql_o);
      deparseFromExprForRel (root, rel_o, &join_sql_o, params_list);
      db2Debug2("  outer join: %s",join_sql_o.data);
  
      /* Deparse inner relation */
      initStringInfo (&join_sql_i);
      deparseFromExprForRel (root, rel_i, &join_sql_i, params_list);
      db2Debug2("  inner join: %s",join_sql_i.data);
  
      // For a join relation FROM clause entry is deparsed as (outer relation) <join type> (inner relation) ON joinclauses
      appendStringInfo (buf, "(%s %s JOIN %s ON ", join_sql_o.data, get_jointype_name (fdwState->jointype), join_sql_i.data);
  
      /* we can only get here if the join is pushed down, so there are join clauses */
      db2Debug2("  joinclauses: %x",fdwState->joinclauses);
      Assert (fdwState->joinclauses);
  
      foreach (lc, fdwState->joinclauses) {
          Expr *expr = (Expr *)lfirst(lc);
          /* connect expressions with AND */
          if (!is_first)
              appendStringInfo(buf, " AND ");
          /* deparse and append a join condition */
          where = deparseExpr(root, foreignrel, expr, params_list);
          appendStringInfo(buf, "%s", where);
          is_first = false;
      }
      /* End the FROM clause entry. */
      appendStringInfo (buf, ")");
    } else {
      // this part is only reached when we process a ForeignUpperPath
    }
  }
  db2Debug2("  buf: '%s'",buf->data);
  db2Debug1("< %s::deparseFromExprForRel",__FILE__);
}