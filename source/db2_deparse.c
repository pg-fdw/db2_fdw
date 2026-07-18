#include <stdio.h>
#include <string.h>
#include <postgres.h>

#include <access/htup_details.h>
#include <access/sysattr.h>
#include <access/table.h>

#include <catalog/pg_aggregate.h>
#include <catalog/pg_authid.h>
#include <catalog/pg_collation.h>
#include <catalog/pg_database.h>
#include <catalog/pg_namespace.h>
#include <catalog/pg_operator.h>
#include <catalog/pg_opfamily.h>
#include <catalog/pg_proc.h>
#include <catalog/pg_ts_config.h>
#include <catalog/pg_ts_dict.h>
#include <catalog/pg_type.h>
#include <catalog/pg_collation.h>

#include <commands/defrem.h>

#include <nodes/pg_list.h>
#include <nodes/nodeFuncs.h>
#include <nodes/plannodes.h>
#include <nodes/bitmapset.h>

#include <optimizer/optimizer.h>
#include <optimizer/paths.h>
#include <optimizer/prep.h>
#include <optimizer/tlist.h>

#include <parser/parsetree.h>

#include <utils/date.h>
#include <utils/datetime.h>
#include <utils/builtins.h>
#include <utils/formatting.h>
#include <utils/guc.h>
#include <utils/syscache.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>
#include <utils/typcache.h>

#include "db2_fdw.h"
#include "DB2FdwState.h"

/** This macro is used by deparseExpr to identify PostgreSQL
 * types that can be translated to DB2 SQL.
 */
#define canHandleType(x) ((x) == TEXTOID || (x) == CHAROID || (x) == BPCHAROID \
      || (x) == VARCHAROID || (x) == NAMEOID || (x) == INT8OID || (x) == INT2OID \
      || (x) == INT4OID || (x) == OIDOID || (x) == FLOAT4OID || (x) == FLOAT8OID \
      || (x) == NUMERICOID || (x) == DATEOID || (x) == TIMEOID || (x) == TIMESTAMPOID \
      || (x) == TIMESTAMPTZOID || (x) == INTERVALOID)

/* Global context for foreign_expr_walker's search of an expression tree. */
typedef struct foreign_glob_cxt {
  PlannerInfo* root;        /* global planner state */
  RelOptInfo*  foreignrel;  /* the foreign relation we are planning for */
  Relids       relids;      /* relids of base relations in the underlying scan */
} foreign_glob_cxt;

/** Local (per-tree-level) context for foreign_expr_walker's search.
 * This is concerned with identifying collations used in the expression.
 */
typedef enum {
  FDW_COLLATE_NONE,   /* expression is of a noncollatable type, or it has default collation that is not traceable to a foreign Var */
  FDW_COLLATE_SAFE,   /* collation derives from a foreign Var */
  FDW_COLLATE_UNSAFE, /* collation is non-default and derives from something other than a foreign Var*/
} FDWCollateState;

typedef struct foreign_loc_cxt {
  Oid             collation;  /* OID of current collation, if any */
  FDWCollateState state;      /* state of current collation choice */
} foreign_loc_cxt;

/** Context for deparseExpr */
typedef struct deparse_expr_cxt {
  PlannerInfo* root;        /* global planner state */
  RelOptInfo*  foreignrel;  /* the foreign relation we are planning for */
  RelOptInfo*  scanrel;     /* the underlying scan relation. Same as foreignrel, when that represents a join or a base relation. */
  List**       params_list; /* exprs that will become remote Params */
  StringInfo   buf;         /* output buffer to append to */
} deparse_expr_cxt;

/** external prototypes */
extern short              c2dbType                  (short fcType);
extern bool               is_shippable              (Oid objectId, Oid classId, DB2FdwState* fpinfo);
extern EquivalenceMember* find_em_for_rel           (PlannerInfo* root, EquivalenceClass* ec, RelOptInfo* rel);
extern EquivalenceMember* find_em_for_rel_target    (PlannerInfo* root, EquivalenceClass* ec, RelOptInfo* rel);
extern void               reset_transmission_modes  (int nestlevel);
extern int                set_transmission_modes    (void);
extern bool               is_builtin                (Oid objectId);


/** local prototypes */
void                appendAsType              (StringInfoData* dest, Oid type);
List*               build_tlist_to_deparse    (RelOptInfo* foreignrel);
void                classifyConditions        (PlannerInfo* root, RelOptInfo* baserel, List* input_conds, List** remote_conds, List** local_conds);
void                deparseSelectStmtForRel   (StringInfo buf, PlannerInfo* root, RelOptInfo* rel,List* tlist, List* remote_conds, List* pathkeys, bool has_final_sort, bool has_limit, bool is_subquery, List** retrieved_attrs, List** params_list);
char*               deparseWhereConditions    (PlannerInfo* root, RelOptInfo* rel);
void                deparseTruncateSql        (StringInfo buf, List* rels, DropBehavior behavior, bool restart_seqs);
char*               deparseExpr               (PlannerInfo* root, RelOptInfo* rel, Expr* expr, List** params);
void                deparseStringLiteral      (StringInfo buf, const char* val);
char*               deparseDate               (Datum datum);
char*               deparseTimestamp          (Datum datum, bool hasTimezone);
bool                is_foreign_expr           (PlannerInfo* root, RelOptInfo* baserel, Expr*    expr);
bool                is_foreign_param          (PlannerInfo* root, RelOptInfo* baserel, Expr*    expr);
bool                is_foreign_pathkey        (PlannerInfo* root, RelOptInfo* baserel, PathKey* pathkey);
char*               get_jointype_name         (JoinType jointype);
EquivalenceMember*  find_em_for_rel           (PlannerInfo* root, EquivalenceClass* ec, RelOptInfo* rel);
EquivalenceMember*  find_em_for_rel_target    (PlannerInfo* root, EquivalenceClass* ec, RelOptInfo* rel);


/** local helper (static) prototypes */
static void         appendGroupByClause       (List* tlist, deparse_expr_cxt* context);
static void         appendOrderByClause       (List* pathkeys, bool has_final_sort, deparse_expr_cxt* context);
static void         appendLimitClause         (deparse_expr_cxt* context);
//static void         appendFunctionName        (Oid funcid, deparse_expr_cxt* context);
static void         appendOrderBySuffix       (Oid sortop, Oid sortcoltype, bool nulls_first, deparse_expr_cxt* context);
static void         appendConditions          (List* exprs, deparse_expr_cxt* context);
static void         appendWhereClause         (List* exprs, List* additional_conds, deparse_expr_cxt* context);

static char*        datumToString             (Datum datum, Oid type);
static char*        deparse_type_name         (Oid type_oid, int32 typemod);
static Node*        deparseSortGroupClause    (Index ref, List* tlist, bool force_colno, deparse_expr_cxt* context);
static void         deparseRangeTblRef        (StringInfo buf, PlannerInfo* root, RelOptInfo* foreignrel, bool make_subquery, Index ignore_rel, List** ignore_conds, List* *additional_conds, List** params_list);
static void         deparseSelectSql          (List* tlist, bool is_subquery, List** retrieved_attrs, deparse_expr_cxt* context);
static void         deparseFromExpr           (List* quals, deparse_expr_cxt* context);
static void         deparseFromExprForRel     (StringInfo buf, PlannerInfo* root, RelOptInfo* foreignrel, bool use_alias, Index ignore_rel, List** ignore_conds, List** additional_conds, List** params_list);
static void         deparseColumnRef          (StringInfo buf, int varno, int varattno, RangeTblEntry* rte, bool qualify_col);
static void         deparseRelation           (StringInfo buf, Relation rel);
static void         deparseExprInt            (Expr*              expr, deparse_expr_cxt* ctx);
static void         deparseConstExpr          (Const*             expr, deparse_expr_cxt* ctx);
static void         deparseParamExpr          (Param*             expr, deparse_expr_cxt* ctx);
//static void         deparseVarExpr            (Var*               expr, deparse_expr_cxt* ctx);
static void         deparseVar                (Var*               expr, deparse_expr_cxt* ctx);
static void         deparseOpExpr             (OpExpr*            expr, deparse_expr_cxt* ctx);
static void         deparseScalarArrayOpExpr  (ScalarArrayOpExpr* expr, deparse_expr_cxt* ctx);
static void         deparseDistinctExpr       (DistinctExpr*      expr, deparse_expr_cxt* ctx);
static void         deparseNullTest           (NullTest*          expr, deparse_expr_cxt* ctx);
static void         deparseNullIfExpr         (NullIfExpr*        expr, deparse_expr_cxt* ctx);
static void         deparseBoolExpr           (BoolExpr*          expr, deparse_expr_cxt* ctx);
static void         deparseCaseExpr           (CaseExpr*          expr, deparse_expr_cxt* ctx);
static void         deparseCoalesceExpr       (CoalesceExpr*      expr, deparse_expr_cxt* ctx);
static void         deparseFuncExpr           (FuncExpr*          expr, deparse_expr_cxt* ctx);
static void         deparseAggref             (Aggref*            expr, deparse_expr_cxt* ctx);
static void         deparseOrderedSetAggref   (Aggref*            expr, const char* db2func, deparse_expr_cxt* ctx);
static const char*  db2AggregateInfo          (Oid aggfnoid, char aggkind, bool* isOrderedSet);
static void         deparseCoerceViaIOExpr    (CoerceViaIO*       expr, deparse_expr_cxt* ctx);
static void         deparseSQLValueFuncExpr   (SQLValueFunction*  expr, deparse_expr_cxt* ctx);
static void         deparseConst              (Const* node, deparse_expr_cxt* context, int showtype);
static void         deparseOperatorName       (StringInfo buf, Form_pg_operator opform);
static void         deparseLockingClause      (deparse_expr_cxt* context);
static void         deparseTargetList         (StringInfo buf, RangeTblEntry* rte, Index rtindex, Relation rel, bool is_returning, Bitmapset* attrs_used, bool qualify_col, List** retrieved_attrs);
static void         deparseExplicitTargetList (List* tlist, bool is_returning, List** retrieved_attrs, deparse_expr_cxt* context);
static void         deparseSubqueryTargetList (deparse_expr_cxt* context);
static char*        deparseInterval           (Datum datum);

static bool         foreign_expr_walker       (Node *node, foreign_glob_cxt* glob_cxt, foreign_loc_cxt* outer_cxt, foreign_loc_cxt* case_arg_cxt);

static void         get_relation_column_alias_ids(Var* node, RelOptInfo* foreignrel, int* relno, int* colno);

static bool         is_subquery_var           (Var* node, RelOptInfo* foreignrel, int* relno, int* colno);

static void         printRemoteParam          (int paramindex, Oid paramtype, int32 paramtypmod, deparse_expr_cxt* context);
static void         printRemotePlaceholder    (Oid paramtype, int32 paramtypmod, deparse_expr_cxt* context);

       void         deparseDirectUpdateSql    (StringInfo buf, PlannerInfo *root, Index rtindex, Relation rel, RelOptInfo *foreignrel, List *targetlist, List *targetAttrs, List *remote_conds, List **params_list, List *returningList, List **retrieved_attrs);
static void         deparseReturningList      (StringInfo buf, RangeTblEntry *rte, Index rtindex, Relation rel, bool trig_after_row, List *withCheckOptionList, List *returningList, List **retrieved_attrs);
       void         deparseDeleteSql          (StringInfo buf, RangeTblEntry *rte, Index rtindex, Relation rel, List *returningList, List **retrieved_attrs);
       void         deparseDirectDeleteSql    (StringInfo buf, PlannerInfo *root, Index rtindex, Relation rel, RelOptInfo *foreignrel, List *remote_conds, List **params_list, List *returningList, List **retrieved_attrs);



/* Examine each qual clause in input_conds, and classify them into two groups, which are returned as two lists:
 *	- remote_conds contains expressions that can be evaluated remotely
 *	- local_conds contains expressions that can't be evaluated remotely
 */
void classifyConditions(PlannerInfo* root, RelOptInfo* baserel, List* input_conds, List** remote_conds, List** local_conds) {
  ListCell* lc  = NULL;

  db2Entry1();
  *remote_conds = NIL;
  *local_conds  = NIL;

  foreach(lc, input_conds) {
    RestrictInfo* ri = lfirst_node(RestrictInfo, lc);

    if (is_foreign_expr(root, baserel, ri->clause))
      *remote_conds = lappend(*remote_conds, ri);
    else
      *local_conds = lappend(*local_conds, ri);
  }
  db2Exit1();
}

/** Returns true if given expr is safe to evaluate on the foreign server.
 */
bool is_foreign_expr(PlannerInfo *root, RelOptInfo *baserel, Expr *expr) {
  foreign_glob_cxt glob_cxt;
  foreign_loc_cxt  loc_cxt;
  DB2FdwState*     fpinfo  = (DB2FdwState*) (baserel->fdw_private);
  bool             fResult = false;

  db2Entry1();
  /*
   * baserel->fdw_private is expected to be initialized by the FDW planning
   * callbacks.  If it is missing, we must not dereference it (planner-time
   * crashes have been observed here for upper relations).
   */
  if (fpinfo == NULL) {
    db2Exit1(": %s", "false");
    return false;
  }
  // Check that the expression consists of nodes that are safe to execute remotely.
  glob_cxt.root       = root;
  glob_cxt.foreignrel = baserel;
  /* For an upper relation, use relids from its underneath scan relation, because the upperrel's own relids currently aren't set to anything
   * meaningful by the core code.  For other relation, use their own relids.
   */
  if (IS_UPPER_REL(baserel)) {
    if (fpinfo->outerrel != NULL && fpinfo->outerrel->relids != NULL)
      glob_cxt.relids = fpinfo->outerrel->relids;
    else
      glob_cxt.relids = baserel->relids;
  } else {
    glob_cxt.relids = baserel->relids;
  }
  loc_cxt.collation = InvalidOid;
  loc_cxt.state     = FDW_COLLATE_NONE;
  if (foreign_expr_walker((Node*) expr, &glob_cxt, &loc_cxt, NULL)) {
    // If the expression has a valid collation that does not arise from a foreign var, the expression can not be sent over.
    if (loc_cxt.state != FDW_COLLATE_UNSAFE) {
      /* An expression which includes any mutable functions can't be sent over because its result is not stable.
       * For example, sending now() remote side could cause confusion from clock offsets. 
       * Future versions might be able to make this choice with more granularity.  (We check this last because it requires a lot of expensive catalog lookups.)
       */
      if (!contain_mutable_functions((Node*) expr)) {
        fResult = true;
      }
    }
  }
  db2Exit1(": %s", (fResult) ? "true": "false");
  /* OK to evaluate on the remote server */
  return fResult;
}

/** Check if expression is safe to execute remotely, and return true if so.
 *
 * In addition, *outer_cxt is updated with collation information.
 *
 * case_arg_cxt is NULL if this subexpression is not inside a CASE-with-arg.
 * Otherwise, it points to the collation info derived from the arg expression,
 * which must be consulted by any CaseTestExpr.
 *
 * We must check that the expression contains only node types we can deparse,
 * that all types/functions/operators are safe to send (they are "shippable"),
 * and that all collations used in the expression derive from Vars of the
 * foreign table.  Because of the latter, the logic is pretty close to
 * assign_collations_walker() in parse_collate.c, though we can assume here
 * that the given expression is valid.  Note function mutability is not
 * currently considered here.
 */
static bool foreign_expr_walker(Node* node, foreign_glob_cxt* glob_cxt, foreign_loc_cxt* outer_cxt, foreign_loc_cxt* case_arg_cxt) {
  bool  fResult = true;

  db2Entry1();
  /* Need do nothing for empty subexpressions */
  if (node != NULL) {
    bool            check_type  = true;
    DB2FdwState*    fpinfo      = (DB2FdwState*) glob_cxt->foreignrel->fdw_private;
    foreign_loc_cxt inner_cxt;
    Oid             collation;
    FDWCollateState state;

    /* Set up inner_cxt for possible recursion to child nodes */
    inner_cxt.collation = InvalidOid;
    inner_cxt.state     = FDW_COLLATE_NONE;
    switch (nodeTag(node)) {
      case T_Var: {
        Var*  var = (Var*) node;
        /* If the Var is from the foreign table, we consider its collation (if any) safe to use.
         * If it is from another table, we treat its collation the same way as we would a Param's collation, 
         * ie it's not safe for it to have a non-default collation.
         */
        if (bms_is_member(var->varno, glob_cxt->relids) && var->varlevelsup == 0) {
          /* Var belongs to foreign table
           * System columns other than ctid should not be sent to the remote, since we don't make any effort to ensure
           * that local and remote values match (tableoid, in particular, almost certainly doesn't match).
           */
          if (var->varattno < 0 && var->varattno != SelfItemPointerAttributeNumber)
            return false;

          /* Else check the collation */
          collation = var->varcollid;
          state = OidIsValid(collation) ? FDW_COLLATE_SAFE : FDW_COLLATE_NONE;
        } else {
          /* Var belongs to some other table */
          collation = var->varcollid;
          if (collation == InvalidOid || collation == DEFAULT_COLLATION_OID) {
            /* It's noncollatable, or it's safe to combine with a collatable foreign Var, so set state to NONE. */
            state = FDW_COLLATE_NONE;
          } else {
            /* Do not fail right away, since the Var might appear in a collation-insensitive context. */
            state = FDW_COLLATE_UNSAFE;
          }
        }
      }
      break;
      case T_Const: {
        Const* c = (Const*) node;
  
        /** Constants of regproc and related types can't be shipped
         * unless the referenced object is shippable.  But NULL's ok.
         * (See also the related code in dependency.c.)
         */
        if (!c->constisnull) {
          switch (c->consttype) {
            case REGPROCOID:
            case REGPROCEDUREOID:
              if (!is_shippable(DatumGetObjectId(c->constvalue), ProcedureRelationId, fpinfo))
                return false;
            break;
            case REGOPEROID:
            case REGOPERATOROID:
              if (!is_shippable(DatumGetObjectId(c->constvalue), OperatorRelationId, fpinfo))
                return false;
            break;
            case REGCLASSOID:
              if (!is_shippable(DatumGetObjectId(c->constvalue), RelationRelationId, fpinfo))
                return false;
            break;
            case REGTYPEOID:
              if (!is_shippable(DatumGetObjectId(c->constvalue), TypeRelationId, fpinfo))
                return false;
            break;
            case REGCOLLATIONOID:
              if (!is_shippable(DatumGetObjectId(c->constvalue), CollationRelationId, fpinfo))
                return false;
            break;
            case REGCONFIGOID:
              /* For text search objects only, we weaken the normal shippability criterion to allow all OIDs below FirstNormalObjectId.
               * Without this, none of the initdb-installed TS configurations would be shippable, which would be quite annoying.
               */
              if (DatumGetObjectId(c->constvalue) >= FirstNormalObjectId && !is_shippable(DatumGetObjectId(c->constvalue), TSConfigRelationId, fpinfo))
                return false;
            break;
            case REGDICTIONARYOID:
              if (DatumGetObjectId(c->constvalue) >= FirstNormalObjectId && !is_shippable(DatumGetObjectId(c->constvalue), TSDictionaryRelationId, fpinfo))
                return false;
            break;
            case REGNAMESPACEOID:
              if (!is_shippable(DatumGetObjectId(c->constvalue), NamespaceRelationId, fpinfo))
                return false;
            break;
            case REGROLEOID:
              if (!is_shippable(DatumGetObjectId(c->constvalue), AuthIdRelationId, fpinfo))
                return false;
            break;
            #ifdef REGDATABASEOID
            case REGDATABASEOID:
              if (!is_shippable(DatumGetObjectId(c->constvalue), DatabaseRelationId, fpinfo))
                return false;
            break;
            #endif
          }
        }
        /* If the constant has nondefault collation, either it's of a non-builtin type, or it reflects folding of a CollateExpr.
         * It's unsafe to send to the remote unless it's used in a non-collation-sensitive context.
         */
        collation = c->constcollid;
        state     = (collation == InvalidOid || collation == DEFAULT_COLLATION_OID) ? FDW_COLLATE_NONE : FDW_COLLATE_UNSAFE;
      }
      break;
      case T_Param: {
        Param	   *p = (Param *) node;
  
        /** If it's a MULTIEXPR Param, punt.  We can't tell from here whether the referenced sublink/subplan contains any remote
         *  Vars; if it does, handling that is too complicated to consider supporting at present.  Fortunately, MULTIEXPR
         *  Params are not reduced to plain PARAM_EXEC until the end of planning, so we can easily detect this case.  (Normal
         *  PARAM_EXEC Params are safe to ship because their values come from somewhere else in the plan tree; but a MULTIEXPR
         *  references a sub-select elsewhere in the same targetlist, so we'd be on the hook to evaluate it somehow if we wanted
         *  to handle such cases as direct foreign updates.)
         */
        if (p->paramkind == PARAM_MULTIEXPR)
          return false;
  
        /** Collation rule is same as for Consts and non-foreign Vars. */
        collation = p->paramcollid;
        state     = (collation == InvalidOid || collation == DEFAULT_COLLATION_OID) ? FDW_COLLATE_NONE : FDW_COLLATE_UNSAFE;
      }
      break;
      case T_SubscriptingRef: {
        SubscriptingRef *sr = (SubscriptingRef *) node;
        // Assignment should not be in restrictions.
        if (sr->refassgnexpr != NULL)
          return false;
        // Recurse into the remaining subexpressions.
        // The container subscripts will not affect collation of the SubscriptingRef result, so do those first and reset inner_cxt afterwards.
        if (!foreign_expr_walker((Node *) sr->refupperindexpr, glob_cxt, &inner_cxt, case_arg_cxt))
          return false;
        inner_cxt.collation = InvalidOid;
        inner_cxt.state     = FDW_COLLATE_NONE;
        if (!foreign_expr_walker((Node *) sr->reflowerindexpr, glob_cxt, &inner_cxt, case_arg_cxt))
          return false;
        inner_cxt.collation = InvalidOid;
        inner_cxt.state = FDW_COLLATE_NONE;
        if (!foreign_expr_walker((Node *) sr->refexpr, glob_cxt, &inner_cxt, case_arg_cxt))
          return false;
        // Container subscripting typically yields same collation as refexpr's, but in case it doesn't, use same logic as for function nodes.
        collation = sr->refcollid;
        if (collation == InvalidOid)
          state = FDW_COLLATE_NONE;
        else if (inner_cxt.state == FDW_COLLATE_SAFE && collation == inner_cxt.collation)
          state = FDW_COLLATE_SAFE;
        else if (collation == DEFAULT_COLLATION_OID)
          state = FDW_COLLATE_NONE;
        else
          state = FDW_COLLATE_UNSAFE;
      }
      break;
      case T_FuncExpr: {
        FuncExpr* fe = (FuncExpr*) node;
  
        /* If function used by the expression is not shippable, it can't be sent to remote because it might have incompatible
         * semantics on remote side.
         */
        if (!is_shippable(fe->funcid, ProcedureRelationId, fpinfo))
          return false;
  
        // Recurse to input subexpressions.
        if (!foreign_expr_walker((Node *) fe->args, glob_cxt, &inner_cxt, case_arg_cxt))
          return false;
  
        // If function's input collation is not derived from a foreign Var, it can't be sent to remote.
        if (fe->inputcollid == InvalidOid)
          /* OK, inputs are all noncollatable */ ;
        else if (inner_cxt.state != FDW_COLLATE_SAFE || fe->inputcollid != inner_cxt.collation)
          return false;
  
        /* Detect whether node is introducing a collation not derived from a foreign Var.  (If so, we just mark it unsafe for now
         * rather than immediately returning false, since the parent node might not care.)
         */
        collation = fe->funccollid;
        if (collation == InvalidOid)
          state = FDW_COLLATE_NONE;
        else if (inner_cxt.state == FDW_COLLATE_SAFE && collation == inner_cxt.collation)
          state = FDW_COLLATE_SAFE;
        else if (collation == DEFAULT_COLLATION_OID)
          state = FDW_COLLATE_NONE;
        else
          state = FDW_COLLATE_UNSAFE;
      }
      break;
      case T_OpExpr:
      case T_DistinctExpr: {  /* struct-equivalent to OpExpr */
        OpExpr*   oe = (OpExpr*) node;
        
        // Similarly, only shippable operators can be sent to remote.
        // (If the operator is shippable, we assume its underlying function is too.)
        if (!is_shippable(oe->opno, OperatorRelationId, fpinfo))
          return false;

        // Recurse to input subexpressions.
        if (!foreign_expr_walker((Node *) oe->args, glob_cxt, &inner_cxt, case_arg_cxt))
          return false;

        // If operator's input collation is not derived from a foreign Var, it can't be sent to remote.
        if (oe->inputcollid == InvalidOid)
          /* OK, inputs are all noncollatable */ ;
        else if (inner_cxt.state != FDW_COLLATE_SAFE || oe->inputcollid != inner_cxt.collation)
          return false;

        // Result-collation handling is same as for functions
        collation = oe->opcollid;
        if (collation == InvalidOid)
          state = FDW_COLLATE_NONE;
        else if (inner_cxt.state == FDW_COLLATE_SAFE && collation == inner_cxt.collation)
          state = FDW_COLLATE_SAFE;
        else if (collation == DEFAULT_COLLATION_OID)
          state = FDW_COLLATE_NONE;
        else
          state = FDW_COLLATE_UNSAFE;
      }
      break;
      case T_ScalarArrayOpExpr: {
        ScalarArrayOpExpr *oe = (ScalarArrayOpExpr *) node;
  
        // Again, only shippable operators can be sent to remote.
        if (!is_shippable(oe->opno, OperatorRelationId, fpinfo))
          return false;
  
        // Recurse to input subexpressions.
        if (!foreign_expr_walker((Node *) oe->args, glob_cxt, &inner_cxt, case_arg_cxt))
          return false;
  
        // If operator's input collation is not derived from a foreign Var, it can't be sent to remote.
        if (oe->inputcollid == InvalidOid)
          /* OK, inputs are all noncollatable */ ;
        else if (inner_cxt.state != FDW_COLLATE_SAFE || oe->inputcollid != inner_cxt.collation)
          return false;
  
        // Output is always boolean and so noncollatable.
        collation = InvalidOid;
        state = FDW_COLLATE_NONE;
      }
      break;
      case T_RelabelType: {
        RelabelType* r = (RelabelType*) node;
        // Recurse to input subexpression.
        if (!foreign_expr_walker((Node *) r->arg, glob_cxt, &inner_cxt, case_arg_cxt))
          return false;
        // RelabelType must not introduce a collation not derived from an input foreign Var (same logic as for a real function).
        collation = r->resultcollid;
        if (collation == InvalidOid)
          state = FDW_COLLATE_NONE;
        else if (inner_cxt.state == FDW_COLLATE_SAFE && collation == inner_cxt.collation)
          state = FDW_COLLATE_SAFE;
        else if (collation == DEFAULT_COLLATION_OID)
          state = FDW_COLLATE_NONE;
        else
          state = FDW_COLLATE_UNSAFE;
      }
      break;
      case T_ArrayCoerceExpr: {
        ArrayCoerceExpr *e = (ArrayCoerceExpr *) node;
        // Recurse to input subexpression.
        if (!foreign_expr_walker((Node *) e->arg, glob_cxt, &inner_cxt, case_arg_cxt))
          return false;
        // T_ArrayCoerceExpr must not introduce a collation not derived from an input foreign Var (same logic as for a function).
        collation = e->resultcollid;
        if (collation == InvalidOid)
          state = FDW_COLLATE_NONE;
        else if (inner_cxt.state == FDW_COLLATE_SAFE && collation == inner_cxt.collation)
          state = FDW_COLLATE_SAFE;
        else if (collation == DEFAULT_COLLATION_OID)
          state = FDW_COLLATE_NONE;
        else
          state = FDW_COLLATE_UNSAFE;
      }
      break;
      case T_BoolExpr: {
        BoolExpr*   b = (BoolExpr*) node;
        // Recurse to input subexpressions.
        if (!foreign_expr_walker((Node*) b->args, glob_cxt, &inner_cxt, case_arg_cxt))
          return false;
        // Output is always boolean and so noncollatable.
        collation = InvalidOid;
        state = FDW_COLLATE_NONE;
      }
      break;
      case T_NullTest: {
        NullTest*   nt = (NullTest*) node;
        // Recurse to input subexpressions.
        if (!foreign_expr_walker((Node*) nt->arg, glob_cxt, &inner_cxt, case_arg_cxt))
          return false;
        // Output is always boolean and so noncollatable.
        collation = InvalidOid;
        state     = FDW_COLLATE_NONE;
      }
      break;
      case T_CaseExpr: {
        CaseExpr*       ce = (CaseExpr*) node;
        foreign_loc_cxt arg_cxt;
        foreign_loc_cxt tmp_cxt;
        ListCell*       lc;
        // Recurse to CASE's arg expression, if any.  Its collation has to be saved aside for use while examining CaseTestExprs within the WHEN expressions.
        arg_cxt.collation = InvalidOid;
        arg_cxt.state     = FDW_COLLATE_NONE;
        if (ce->arg) {
          if (!foreign_expr_walker((Node *) ce->arg, glob_cxt, &arg_cxt, case_arg_cxt))
            return false;
        }
  
        // Examine the CaseWhen subexpressions.
        foreach(lc, ce->args) {
          CaseWhen*   cw = lfirst_node(CaseWhen, lc);
          if (ce->arg) {
            /* In a CASE-with-arg, the parser should have produced WHEN clauses of the form "CaseTestExpr = RHS",
             * possibly with an implicit coercion inserted above the CaseTestExpr.  However in an expression that's
             * been through the optimizer, the WHEN clause could be almost anything (since the equality operator
             * could have been expanded into an inline function).
             * In such cases forbid pushdown, because deparseCaseExpr can't handle it.
             */
            Node* whenExpr = (Node*) cw->expr;
            List* opArgs   = NULL;
            if (!IsA(whenExpr, OpExpr))
              return false;
            opArgs = ((OpExpr *) whenExpr)->args;
            if (list_length(opArgs) != 2 || !IsA(strip_implicit_coercions(linitial(opArgs)), CaseTestExpr))
              return false;
          }
          /* Recurse to WHEN expression, passing down the arg info.
           * Its collation doesn't affect the result (really, it should be boolean and thus not have a collation).
           */
          tmp_cxt.collation = InvalidOid;
          tmp_cxt.state     = FDW_COLLATE_NONE;
          if (!foreign_expr_walker((Node *) cw->expr, glob_cxt, &tmp_cxt, &arg_cxt))
            return false;
          /* Recurse to THEN expression. */
          if (!foreign_expr_walker((Node *) cw->result, glob_cxt, &inner_cxt, case_arg_cxt))
            return false;
        }
        // Recurse to ELSE expression.
        if (!foreign_expr_walker((Node *) ce->defresult, glob_cxt, &inner_cxt, case_arg_cxt))
          return false;
        /* Detect whether node is introducing a collation not derived from a foreign Var.  (If so, we just mark it unsafe for now
         * rather than immediately returning false, since the parent node might not care.)  This is the same as for function
         * nodes, except that the input collation is derived from only the THEN and ELSE subexpressions.
         */
        collation = ce->casecollid;
        if (collation == InvalidOid)
          state = FDW_COLLATE_NONE;
        else if (inner_cxt.state == FDW_COLLATE_SAFE && collation == inner_cxt.collation)
          state = FDW_COLLATE_SAFE;
        else if (collation == DEFAULT_COLLATION_OID)
          state = FDW_COLLATE_NONE;
        else
          state = FDW_COLLATE_UNSAFE;
      }
      break;
      case T_CaseTestExpr: {
        CaseTestExpr* c = (CaseTestExpr*) node;
        // Punt if we seem not to be inside a CASE arg WHEN.
        if (!case_arg_cxt)
          return false;
        // Otherwise, any nondefault collation attached to the CaseTestExpr node must be derived from foreign Var(s) in the CASE arg.
        collation = c->collation;
        if (collation == InvalidOid)
          state = FDW_COLLATE_NONE;
        else if (case_arg_cxt->state == FDW_COLLATE_SAFE && collation == case_arg_cxt->collation)
          state = FDW_COLLATE_SAFE;
        else if (collation == DEFAULT_COLLATION_OID)
          state = FDW_COLLATE_NONE;
        else
          state = FDW_COLLATE_UNSAFE;
      }
      break;
      case T_ArrayExpr: {
        ArrayExpr*  a = (ArrayExpr *) node;
        // Recurse to input subexpressions.
        if (!foreign_expr_walker((Node *) a->elements, glob_cxt, &inner_cxt, case_arg_cxt))
          return false;
        // ArrayExpr must not introduce a collation not derived from an input foreign Var (same logic as for a function).
        collation = a->array_collid;
        if (collation == InvalidOid)
          state = FDW_COLLATE_NONE;
        else if (inner_cxt.state == FDW_COLLATE_SAFE && collation == inner_cxt.collation)
          state = FDW_COLLATE_SAFE;
        else if (collation == DEFAULT_COLLATION_OID)
          state = FDW_COLLATE_NONE;
        else
          state = FDW_COLLATE_UNSAFE;
      }
      break;
      case T_List: {
        List*     l  = (List*) node;
        ListCell* lc = NULL;
        // Recurse to component subexpressions.
        foreach(lc, l) {
          if (!foreign_expr_walker((Node *) lfirst(lc), glob_cxt, &inner_cxt, case_arg_cxt))
            return false;
        }
        // When processing a list, collation state just bubbles up from the list elements.
        collation = inner_cxt.collation;
        state     = inner_cxt.state;
        // Don't apply exprType() to the list.
        check_type = false;
      }
      break;
      case T_Aggref: {
        Aggref*   agg = (Aggref*) node;
        ListCell* lc  = NULL;
        // Not safe to pushdown when not in grouping context
        if (!IS_UPPER_REL(glob_cxt->foreignrel))
          return false;
        // Only non-split aggregates are pushable.
        if (agg->aggsplit != AGGSPLIT_SIMPLE)
          return false;
        // As usual, it must be shippable.
        if (!is_shippable(agg->aggfnoid, ProcedureRelationId, fpinfo))
          return false;
        // We must also know how to deparse this specific aggregate; otherwise the walker would say it's safe
        // to push down while deparseAggref silently emits nothing, producing a broken remote query.
        {
          bool isOrderedSet = false;

          if (db2AggregateInfo(agg->aggfnoid, agg->aggkind, &isOrderedSet) == NULL)
            return false;
          // Ordered-set aggregates (PERCENTILE_CONT/PERCENTILE_DISC) are only pushed down in the simple,
          // single-column form: one direct argument (the fraction) that is not an array, and exactly one
          // ORDER BY column.  DB2's WITHIN GROUP (ORDER BY ...) doesn't support the multi-column or
          // array-fraction variants that PostgreSQL allows.
          if (isOrderedSet) {
            if (list_length(agg->aggdirectargs) != 1 || list_length(agg->aggorder) != 1)
              return false;
            if (type_is_array(exprType((Node*) linitial(agg->aggdirectargs))))
              return false;
          }
        }
        // Recurse to direct arguments, if any (used by ordered-set aggregates like PERCENTILE_CONT/PERCENTILE_DISC).
        foreach(lc, agg->aggdirectargs) {
          Node* n = (Node*) lfirst(lc);
          if (!foreign_expr_walker(n, glob_cxt, &inner_cxt, case_arg_cxt))
            return false;
        }
        // Recurse to input args. aggorder and aggdistinct are all present in args, so no need to check their shippability explicitly.
        foreach(lc, agg->args) {
          Node*   n = (Node*) lfirst(lc);
          // If TargetEntry, extract the expression from it
          if (IsA(n, TargetEntry)) {
            TargetEntry* tle = (TargetEntry*) n;
            n = (Node*) tle->expr;
          }
          if (!foreign_expr_walker(n, glob_cxt, &inner_cxt, case_arg_cxt))
            return false;
        }
        // For aggorder elements, check whether the sort operator, if specified, is shippable or not.
        if (agg->aggorder) {
          foreach(lc, agg->aggorder) {
            SortGroupClause*  srt = (SortGroupClause*) lfirst(lc);
            Oid             sortcoltype;
            TypeCacheEntry* typentry;
            TargetEntry*    tle;
  
            tle         = get_sortgroupref_tle(srt->tleSortGroupRef, agg->args);
            sortcoltype = exprType((Node *) tle->expr);
            typentry    = lookup_type_cache(sortcoltype, TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);
            // Check shippability of non-default sort operator.
            if (srt->sortop != typentry->lt_opr && srt->sortop != typentry->gt_opr && !is_shippable(srt->sortop, OperatorRelationId, fpinfo))
              return false;
          }
        }
        // Check aggregate filter
        if (!foreign_expr_walker((Node *) agg->aggfilter, glob_cxt, &inner_cxt, case_arg_cxt))
          return false;
        // If aggregate's input collation is not derived from a foreign Var, it can't be sent to remote.
        if (agg->inputcollid == InvalidOid)
          /* OK, inputs are all noncollatable */ ;
        else if (inner_cxt.state != FDW_COLLATE_SAFE || agg->inputcollid != inner_cxt.collation)
          return false;
        /* Detect whether node is introducing a collation not derived from a foreign Var.  (If so, we just mark it unsafe for now
         * rather than immediately returning false, since the parent node might not care.)
         */
        collation = agg->aggcollid;
        if (collation == InvalidOid)
          state = FDW_COLLATE_NONE;
        else if (inner_cxt.state == FDW_COLLATE_SAFE && collation == inner_cxt.collation)
          state = FDW_COLLATE_SAFE;
        else if (collation == DEFAULT_COLLATION_OID)
          state = FDW_COLLATE_NONE;
        else
          state = FDW_COLLATE_UNSAFE;
      }
      break;
      default:
        /* If it's anything else, assume it's unsafe.
         * This list can be expanded later, but don't forget to add deparse support below.
         */
        return false;
    }
    /* If result type of given expression is not shippable, it can't be sent to remote because it might have incompatible semantics on remote side. */
    if (check_type && !is_shippable(exprType(node), TypeRelationId, fpinfo))
      return false;
    /* Now, merge my collation information into my parent's state. */
    if (state > outer_cxt->state) {
      /* Override previous parent state */
      outer_cxt->collation  = collation;
      outer_cxt->state      = state;
    }	else if (state == outer_cxt->state) {
      /* Merge, or detect error if there's a collation conflict */
      switch (state) {
        case FDW_COLLATE_NONE: {
          /* Nothing + nothing is still nothing */
        }
        break;
        case FDW_COLLATE_SAFE: {
          if (collation != outer_cxt->collation) {
            /** Non-default collation always beats default.*/
            if (outer_cxt->collation == DEFAULT_COLLATION_OID) {
              /* Override previous parent state */
              outer_cxt->collation = collation;
            } else if (collation != DEFAULT_COLLATION_OID) {
              /* Conflict; show state as indeterminate. 
               * We don't want to "return false" right away, since parent node might not care about collation. 
               */
              outer_cxt->state = FDW_COLLATE_UNSAFE;
            }
          }
        }
        break;
        case FDW_COLLATE_UNSAFE: {
          /* We're still conflicted ... */
        }
        break;
      }
    }
  }
  /* It looks OK */
  db2Exit1(": %s", (fResult) ? "true" : "false");
  return fResult;
}

/** Returns true if given expr is something we'd have to send the value of to the foreign server.
 *
 * This should return true when the expression is a shippable node that deparseExpr would add to context->params_list.
 * Note that we don't care if the expression *contains* such a node, only whether one appears at top level. 
 * We need this to detect cases where setrefs.c would recognize a false match between an fdw_exprs item (which came from the params_list)
 * and an entry in fdw_scan_tlist (which we're considering putting the given expression into).
 */
bool is_foreign_param(PlannerInfo* root, RelOptInfo* baserel, Expr* expr) {
  bool  fResult = false;
  db2Entry1();
  db2Debug2("expr: %x", expr);
  if (expr != NULL) {
    db2Debug5("((Node*)expr)->type: %d", nodeTag(expr));
    switch (nodeTag(expr)) {
      case T_Var: {
          /* It would have to be sent unless it's a foreign Var */
          Var*          var    = (Var*) expr;
          DB2FdwState*  fpinfo = (DB2FdwState*) (baserel->fdw_private);
          Relids        relids;

          relids  = (IS_UPPER_REL(baserel)) ? fpinfo->outerrel->relids : baserel->relids;
          fResult = !(bms_is_member(var->varno, relids) && var->varlevelsup == 0);
      }
      break;
      case T_Param:
        /* Params always have to be sent to the foreign server */
        fResult = true;
      default:
      break;
    }
  }
  db2Exit1(": %s", (fResult) ? "true" : "false");
  return fResult;
}

/** Returns true if it's safe to push down the sort expression described by
 * 'pathkey' to the foreign server.
 */
bool is_foreign_pathkey(PlannerInfo* root, RelOptInfo* baserel, PathKey* pathkey) {
  EquivalenceClass* pathkey_ec = pathkey->pk_eclass;
  DB2FdwState*      fpinfo     = (DB2FdwState*) baserel->fdw_private;
  bool              fResult    = false;

  db2Entry1();
  /* is_foreign_expr would detect volatile expressions as well, but checking ec_has_volatile here saves some cycles. */
  if (!pathkey_ec->ec_has_volatile) {
    /* can't push down the sort if the pathkey's opfamily is not shippable */
    if (is_shippable(pathkey->pk_opfamily, OperatorFamilyRelationId, fpinfo)) {
      /* can push if a suitable EC member exists */
      fResult = (find_em_for_rel(root, pathkey_ec, baserel) != NULL);
    }
  }
  db2Exit1(": %s", (fResult) ? "true" : "false");
  return fResult;
}

/* Convert type OID + typmod info into a type name we can ship to the remote server.
 * Someplace else had better have verified that this type name is expected to be known on the remote end.
 *
 * This is almost just format_type_with_typemod(), except that if left to its own devices, that function will make 
 * schema-qualification decisions based on the local search_path, which is wrong. 
 * We must schema-qualify all type names that are not in pg_catalog.  
 * We assume here that built-in types are all in pg_catalog and need not be qualified; otherwise, qualify.
 */
static char* deparse_type_name(Oid type_oid, int32 typemod) {
  bits16  flags   = FORMAT_TYPE_TYPEMOD_GIVEN;
  char*   result  = NULL;

  db2Entry1();
  if (!is_builtin(type_oid))
    flags |= FORMAT_TYPE_FORCE_QUALIFY;

  result  = format_type_extended(type_oid, typemod, flags);
  db2Exit1(": %s", result);
  return result;
}

/** appendAsType
 *   Append "s" to "dest", adding appropriate casts for datetime "type".
 */
void appendAsType (StringInfoData* dest, Oid type) {
  db2Entry1();
  db2Debug2("dest->data: '%s'",dest->data);
  db2Debug2("type: %d",type);
  switch (type) {
    case DATEOID:
      appendStringInfo (dest, "CAST (? AS DATE)");
      break;
    case TIMESTAMPOID:
      appendStringInfo (dest, "CAST (? AS TIMESTAMP)");
      break;
    case TIMESTAMPTZOID:
      appendStringInfo (dest, "CAST (? AS TIMESTAMP)");
      break;
    case TIMEOID:
      appendStringInfo (dest, "(CAST (? AS TIME))");
      break;
    case TIMETZOID:
      appendStringInfo (dest, "(CAST (? AS TIME))");
      break;
    default:
      appendStringInfo (dest, "?");
    break;
  }
  db2Debug2("dest->data: '%s'", dest->data);
  db2Exit1();
}

/** Deparse GROUP BY clause.
 */
static void appendGroupByClause(List* tlist, deparse_expr_cxt* context) {
  Query*      query = context->root->parse;

  db2Entry1();
  /* Nothing to be done, if there's no GROUP BY clause in the query. */
  if (query->groupClause) {
    StringInfo  buf   = context->buf;
    ListCell*   lc    = NULL;
    bool        first = true;

    appendStringInfoString(buf, " GROUP BY ");
    /* Queries with grouping sets are not pushed down, so we don't expect grouping sets here. */
    Assert(!query->groupingSets);

    /* We intentionally print query->groupClause not processed_groupClause, leaving it to the remote planner to get rid of any redundant GROUP BY
     * items again.  This is necessary in case processed_groupClause reduced to empty, and in any case the redundancy situation on the remote might
     * be different than what we think here.
     */
    foreach(lc, query->groupClause) {
      SortGroupClause *grp = (SortGroupClause*) lfirst(lc);
      if (!first)
        appendStringInfoString(buf, ", ");
      first = false;
      deparseSortGroupClause(grp->tleSortGroupRef, tlist, true, context);
    }
    db2Debug5("clause: %s", buf->data);
  }
  db2Exit1();
}

/** Deparse ORDER BY clause defined by the given pathkeys.
 *
 * The clause should use Vars from context->scanrel if !has_final_sort,
 * or from context->foreignrel's targetlist if has_final_sort.
 *
 * We find a suitable pathkey expression (some earlier step
 * should have verified that there is one) and deparse it.
 */
static void appendOrderByClause(List* pathkeys, bool has_final_sort, deparse_expr_cxt* context) {
  ListCell*   lcell     = NULL;
  int         nestlevel = 0;
  StringInfo  buf       = context->buf;
  bool        gotone    = false;

  db2Entry1();
  /* Make sure any constants in the exprs are printed portably */
  nestlevel = set_transmission_modes();

  foreach(lcell, pathkeys) {
    PathKey*            pathkey = lfirst(lcell);
    EquivalenceMember*  em;
    Expr*               em_expr;
    Oid                 oprid;

    if (has_final_sort) {
      /* By construction, context->foreignrel is the input relation to the final sort. */
      em = find_em_for_rel_target(context->root, pathkey->pk_eclass, context->foreignrel);
    } else {
      em = find_em_for_rel(context->root, pathkey->pk_eclass, context->scanrel);
    }
    /* We don't expect any error here; it would mean that shippability wasn't verified earlier.
     * For the same reason, we don't recheck shippability of the sort operator.
     */
    if (em == NULL)
      elog(ERROR, "could not find pathkey item to sort");

    em_expr = em->em_expr;

    /* If the member is a Const expression then we needn't add it to the ORDER BY clause. 
     * This can happen in UNION ALL queries where the union child targetlist has a Const.
     * Adding these would be wasteful, but also, for INT columns, an integer literal would be seen as an ordinal column position rather 
     * than a value to sort by.
     * deparseConst() does have code to handle this, but it seems less effort on all accounts just to skip these for ORDER BY clauses.
     */
    if (IsA(em_expr, Const))
      continue;

    if (!gotone) {
      appendStringInfoString(buf, " ORDER BY ");
      gotone = true;
    } else {
      appendStringInfoString(buf, ", ");
    }
    /* Lookup the operator corresponding to the compare type in the opclass.
     * The datatype used by the opfamily is not necessarily the same as the expression type (for array types for example).
     */
    #if PG_VERSION_NUM < 180000
    oprid = get_opfamily_member(pathkey->pk_opfamily, em->em_datatype, em->em_datatype, pathkey->pk_strategy);
    if (!OidIsValid(oprid))
      elog(ERROR, "missing operator %d(%u,%u) in opfamily %u", pathkey->pk_strategy, em->em_datatype, em->em_datatype, pathkey->pk_opfamily);
    #else
    oprid = get_opfamily_member_for_cmptype(pathkey->pk_opfamily, em->em_datatype, em->em_datatype, pathkey->pk_cmptype);
    if (!OidIsValid(oprid))
      elog(ERROR, "missing operator %d(%u,%u) in opfamily %u", pathkey->pk_cmptype, em->em_datatype, em->em_datatype, pathkey->pk_opfamily);
    #endif
    deparseExprInt(em_expr, context);

    /* Here we need to use the expression's actual type to discover whether the desired operator will be the default or not. */
    appendOrderBySuffix(oprid, exprType((Node *) em_expr), pathkey->pk_nulls_first, context);
  }
  reset_transmission_modes(nestlevel);
  db2Debug5("clause: %s", context->buf->data);
  db2Exit1();
}

/** Deparse LIMIT/OFFSET clause.
 */
static void appendLimitClause(deparse_expr_cxt* context) {
  PlannerInfo*  root      = context->root;
  StringInfo    buf       = context->buf;
  int           nestlevel = 0;

  db2Entry1();
  /* Make sure any constants in the exprs are printed portably */
  nestlevel = set_transmission_modes();

  /*
   * DB2 does not support the Postgres syntax "LIMIT <n> OFFSET <m>".
   * Use DB2 pagination syntax instead:
   *   - "FETCH FIRST <n> ROWS ONLY"               (limit)
   *   - "OFFSET <m> ROWS"                         (offset)
   *   - "OFFSET <m> ROWS FETCH NEXT <n> ROWS ONLY" (limit+offset)
   */

  if (root->parse->limitOffset) {
    appendStringInfoString(buf, " OFFSET ");
    deparseExprInt((Expr*) root->parse->limitOffset, context);
    appendStringInfoString(buf, " ROWS");
  }

  if (root->parse->limitCount) {
    if (root->parse->limitOffset)
      appendStringInfoString(buf, " FETCH NEXT ");
    else
      appendStringInfoString(buf, " FETCH FIRST ");

    deparseExprInt((Expr*) root->parse->limitCount, context);
    appendStringInfoString(buf, " ROWS ONLY");
  }
  reset_transmission_modes(nestlevel);
  db2Debug5("  clause: %s", context->buf->data);
  db2Exit1();
}

/** appendFunctionName
 * Deparses function name from given function oid.
 */
//static void appendFunctionName(Oid funcid, deparse_expr_cxt *context) {
//  StringInfo    buf       = context->buf;
//  HeapTuple     proctup;
//  Form_pg_proc  procform;
//
//  db2Entry1();
//  proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
//  if (!HeapTupleIsValid(proctup))
//    elog(ERROR, "cache lookup failed for function %u", funcid);
//  procform = (Form_pg_proc) GETSTRUCT(proctup);
//
//  /* Print schema name only if it's not pg_catalog */
//  if (procform->pronamespace != PG_CATALOG_NAMESPACE)
//    appendStringInfo(buf, "%s.", quote_identifier(get_namespace_name(procform->pronamespace)));
//
//  /* Always print the function name */
//  appendStringInfoString(buf, quote_identifier(NameStr(procform->proname)));
//  ReleaseSysCache(proctup);
//  db2Exit1(": %s", context->buf->data);
//}

/** Append the ASC, DESC, USING <OPERATOR> and NULLS FIRST / NULLS LAST parts of an ORDER BY clause.
 */
static void appendOrderBySuffix(Oid sortop, Oid sortcoltype, bool nulls_first, deparse_expr_cxt* context) {
  StringInfo      buf = context->buf;
  TypeCacheEntry* typentry;

  db2Entry1();
  /* See whether operator is default < or > for sort expr's datatype. */
  typentry = lookup_type_cache(sortcoltype, TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);

  if (sortop == typentry->lt_opr)
    appendStringInfoString(buf, " ASC");
  else if (sortop == typentry->gt_opr)
    appendStringInfoString(buf, " DESC");
  else {
    HeapTuple         opertup;
    Form_pg_operator  operform;

    appendStringInfoString(buf, " USING ");
    /* Append operator name. */
    opertup = SearchSysCache1(OPEROID, ObjectIdGetDatum(sortop));
    if (!HeapTupleIsValid(opertup))
      elog(ERROR, "cache lookup failed for operator %u", sortop);
    operform = (Form_pg_operator) GETSTRUCT(opertup);
    deparseOperatorName(buf, operform);
    ReleaseSysCache(opertup);
  }
  appendStringInfo(buf, " NULLS %s", (nulls_first) ? "FIRST" : "LAST");

  db2Exit1(": %s", buf->data);
}

/* Print the representation of a parameter to be sent to the remote side.
 *
 * Note: we always label the Param's type explicitly rather than relying on transmitting a numeric type OID in PQsendQueryParams().
 * This allows us to avoid assuming that types have the same OIDs on the remote side as they do locally --- they need only have the same names.
 */
static void printRemoteParam(int paramindex, Oid paramtype, int32 paramtypmod, deparse_expr_cxt* context) {
  StringInfo  buf       = context->buf;
//char*       ptypename = deparse_type_name(paramtype, paramtypmod);

  db2Entry1();
//  appendStringInfo(buf, "$%d::%s", paramindex, ptypename);
  appendStringInfo(buf, ":p%d", paramindex);
  db2Exit1(": %s", buf->data);
}

/* Print the representation of a placeholder for a parameter that will be sent to the remote side at execution time.
 *
 * This is used when we're just trying to EXPLAIN the remote query. 
 * We don't have the actual value of the runtime parameter yet, and we don't want the remote planner to generate a 
 * plan that depends on such a value anyway.
 * Thus, we can't do something simple like "$1::paramtype".
 * Instead, we emit "((SELECT null::paramtype)::paramtype)".
 * In all extant versions of Postgres, the planner will see that as an unknown constant value, which is what we want.
 * This might need adjustment if we ever make the planner flatten scalar subqueries.
 * Note: the reason for the apparently useless outer cast is to ensure that the representation as a whole will be 
 * parsed as an a_expr and not a select_with_parens; the latter would do the wrong thing in the context "x = ANY(...)".
 */
static void printRemotePlaceholder(Oid paramtype, int32 paramtypmod, deparse_expr_cxt* context) {
  StringInfo  buf       = context->buf;
  char*       ptypename = deparse_type_name(paramtype, paramtypmod);

  db2Entry1();
  appendStringInfo(buf, "((SELECT null::%s)::%s)", ptypename, ptypename);
  db2Exit1(": %s", buf->data);
}

/** Deparse conditions from the provided list and append them to buf.
 *
 * The conditions in the list are assumed to be ANDed. This function is used to deparse WHERE clauses, JOIN .. ON clauses and HAVING clauses.
 *
 * Depending on the caller, the list elements might be either RestrictInfos or bare clauses.
 */
static void appendConditions(List* exprs, deparse_expr_cxt* context) {
  int         nestlevel   = 0;
  ListCell*   lc          = NULL;
  bool        is_first    = true;
  StringInfo	buf         = context->buf;

  db2Entry1();
  /* Make sure any constants in the exprs are printed portably */
  nestlevel = set_transmission_modes();

  foreach(lc, exprs) {
    Expr* expr = (Expr*) lfirst(lc);

    /* Extract clause from RestrictInfo, if required */
    if (IsA(expr, RestrictInfo))
      expr = ((RestrictInfo*) expr)->clause;

    /* Connect expressions with "AND" and parenthesize each condition. */
    if (!is_first)
      appendStringInfoString(buf, " AND ");

    appendStringInfoChar(buf, '(');
    deparseExprInt(expr, context);
    appendStringInfoChar(buf, ')');

    is_first = false;
  }
  reset_transmission_modes(nestlevel);
  db2Exit1(": %s", buf->data);
}

/* Append WHERE clause, containing conditions from exprs and additional_conds, to context->buf.
 */
static void appendWhereClause(List* exprs, List* additional_conds, deparse_expr_cxt* context) {
  StringInfo  buf       = context->buf;
  bool        need_and  = false;
  ListCell*   lc        = NULL;

  db2Entry1();
  if (exprs != NIL || additional_conds != NIL)
    appendStringInfoString(buf, " WHERE ");

  /* If there are some filters, append them. */
  if (exprs != NIL) {
    appendConditions(exprs, context);
    need_and = true;
  }

  /* If there are some EXISTS conditions, coming from SEMI-JOINS, append them. */
  foreach(lc, additional_conds) {
    if (need_and)
      appendStringInfoString(buf, " AND ");
    appendStringInfoString(buf, (char*) lfirst(lc));
    need_and = true;
  }
  db2Exit1(": %s", buf->data);
}

/** Appends a sort or group clause.
 *
 * Like get_rule_sortgroupclause(), returns the expression tree, so caller
 * need not find it again.
 */
static Node* deparseSortGroupClause(Index ref, List* tlist, bool force_colno, deparse_expr_cxt* context) {
  StringInfo    buf   = context->buf;
  TargetEntry*  tle   = get_sortgroupref_tle(ref, tlist);
  Expr*         expr  = tle->expr;

  db2Entry1();
  if (force_colno) {
    /* Use column-number form when requested by caller. */
    Assert(!tle->resjunk);
    appendStringInfo(buf, "%d", tle->resno);
  } else if (expr && IsA(expr, Const)) {
    /* Force a typecast here so that we don't emit something like "GROUP BY 2", which will be misconstrued as a column position rather than a constant. */
    deparseConst((Const*) expr, context, 1);
  } else if (!expr || IsA(expr, Var)) {
    deparseExprInt(expr, context);
  } else {
    /* Always parenthesize the expression. */
    appendStringInfoChar(buf, '(');
    deparseExprInt(expr, context);
    appendStringInfoChar(buf, ')');
  }
  db2Debug5("clause: %s", buf->data);
  db2Exit1(": %x", expr);
  return (Node*)expr;
}

/* Returns true if given Var is deparsed as a subquery output column, in which case, *relno and *colno are set to the IDs for the relation and
 * column alias to the Var provided by the subquery.
 */
static bool is_subquery_var(Var* node, RelOptInfo* foreignrel, int* relno, int* colno) {
  bool  fResult = false;

  db2Entry1();
  /* Should only be called in these cases. */
  Assert(IS_SIMPLE_REL(foreignrel) || IS_JOIN_REL(foreignrel));
  /* If the given relation isn't a join relation, it doesn't have any lower subqueries, so the Var isn't a subquery output column. */
  if (IS_JOIN_REL(foreignrel)) {
    DB2FdwState*  fpinfo  = (DB2FdwState*) foreignrel->fdw_private;

    /* If the Var doesn't belong to any lower subqueries, it isn't a subquery output column. */
    if (bms_is_member(node->varno, fpinfo->lower_subquery_rels)) {
      if (bms_is_member(node->varno, fpinfo->outerrel->relids)) {
        /* If outer relation is deparsed as a subquery, the Var is an output column of the subquery; get the IDs for the relation/column alias. */
        if (fpinfo->make_outerrel_subquery) {
          get_relation_column_alias_ids(node, fpinfo->outerrel, relno, colno);
          fResult = true;
        } else {
          /* Otherwise, recurse into the outer relation. */
          fResult = is_subquery_var(node, fpinfo->outerrel, relno, colno);
        }
      } else {
        Assert(bms_is_member(node->varno, fpinfo->innerrel->relids));
        /* If inner relation is deparsed as a subquery, the Var is an output column of the subquery; get the IDs for the relation/column alias. */
        if (fpinfo->make_innerrel_subquery) {
          get_relation_column_alias_ids(node, fpinfo->innerrel, relno, colno);
          fResult = true;
        } else {
          /* Otherwise, recurse into the inner relation. */
          fResult = is_subquery_var(node, fpinfo->innerrel, relno, colno);
        }
      }
    }
  }
  db2Exit1(": %s", (fResult) ? "true": "false");
  return fResult;
}

/* Get the IDs for the relation and column alias to given Var belonging to given relation, which are returned into *relno and *colno.
 */
static void get_relation_column_alias_ids(Var* node, RelOptInfo* foreignrel, int* relno, int* colno) {
  DB2FdwState*  fpinfo  = (DB2FdwState*) foreignrel->fdw_private;
  int           i       = 1;
  ListCell*     lc      = NULL;
  bool          fFound  = false;

  db2Entry1();
  /* Get the relation alias ID */
  *relno = fpinfo->relation_index;
  db2Debug5("relno: %d", *relno);

  /* Get the column alias ID */
  foreach(lc, foreignrel->reltarget->exprs) {
    Var*  tlvar = (Var*) lfirst(lc);

    /* Match reltarget entries only on varno/varattno.  Ideally there would be some cross-check on varnullingrels, but it's unclear what
     * to do exactly; we don't have enough context to know what that value should be.
     */
    if (IsA(tlvar, Var) && tlvar->varno == node->varno && tlvar->varattno == node->varattno) {
      *colno = i;
      db2Debug5("colno: %d", *colno);
      fFound = true;
      break;
    }
    i++;
  }

  if (!fFound) {
    /* Shouldn't get here */
    elog(ERROR, "unexpected expression in subquery output");
  }
  db2Exit1();
}

/* Build the targetlist for given relation to be deparsed as SELECT clause.
 *
 * The output targetlist contains the columns that need to be fetched from the foreign server for the given relation.
 * If foreignrel is an upper relation, then the output targetlist can also contain expressions to be evaluated on
 * foreign server.
 */
List* build_tlist_to_deparse(RelOptInfo* foreignrel) {
  List*         tlist   = NIL;
  DB2FdwState*  fpinfo  = (DB2FdwState*) foreignrel->fdw_private;

  db2Entry1();
  /* For an upper relation, we have already built the target list while checking shippability, so just return that. */
  if (IS_UPPER_REL(foreignrel)) {
    tlist = fpinfo->grouped_tlist;
    db2Debug2("using fpinfo->grouped_tlist");
  } else {
    ListCell* lc  = NULL;

    /* We require columns specified in foreignrel->reltarget->exprs and those required for evaluating the local conditions. */
    db2Debug2("using foreignrel->reltarget->exprs");
    tlist = add_to_flat_tlist(tlist, pull_var_clause((Node*) foreignrel->reltarget->exprs, PVC_RECURSE_PLACEHOLDERS));
    foreach(lc, fpinfo->local_conds) {
      RestrictInfo* rinfo = lfirst_node(RestrictInfo, lc);
      tlist = add_to_flat_tlist(tlist, pull_var_clause((Node*) rinfo->clause, PVC_RECURSE_PLACEHOLDERS));
    }
  }
  db2Exit1(": %x", tlist);
  return tlist;
}

/** Deparse SELECT statement for given relation into buf.
 *
 * tlist contains the list of desired columns to be fetched from foreign server.
 * For a base relation fpinfo->attrs_used is used to construct SELECT clause,
 * hence the tlist is ignored for a base relation.
 *
 * remote_conds is the list of conditions to be deparsed into the WHERE clause
 * (or, in the case of upper relations, into the HAVING clause).
 *
 * If params_list is not NULL, it receives a list of Params and other-relation
 * Vars used in the clauses; these values must be transmitted to the remote
 * server as parameter values.
 *
 * If params_list is NULL, we're generating the query for EXPLAIN purposes,
 * so Params and other-relation Vars should be replaced by dummy values.
 *
 * pathkeys is the list of pathkeys to order the result by.
 *
 * is_subquery is the flag to indicate whether to deparse the specified
 * relation as a subquery.
 *
 * List of columns selected is returned in retrieved_attrs.
 */
void deparseSelectStmtForRel(StringInfo buf, PlannerInfo* root, RelOptInfo* rel,List* tlist, List* remote_conds, List* pathkeys, bool has_final_sort, bool has_limit, bool is_subquery, List** retrieved_attrs, List** params_list) {
  deparse_expr_cxt  context;
  DB2FdwState*      fpinfo  = (DB2FdwState*)rel->fdw_private;
  List*             quals   = NIL;

  db2Entry1();
  //We handle relations for foreign tables, joins between those and upper relations.
  Assert(IS_JOIN_REL(rel) || IS_SIMPLE_REL(rel) || IS_UPPER_REL(rel));

  // Fill portions of context common to upper, join and base relation
  context.buf         = buf;
  context.root        = root;
  context.foreignrel  = rel;
  context.scanrel     = IS_UPPER_REL(rel) ? fpinfo->outerrel : rel;
  context.params_list = params_list;

  // Construct SELECT clause
  deparseSelectSql(tlist, is_subquery, retrieved_attrs, &context);

  /* For upper relations, the WHERE clause is built from the remote conditions of the underlying scan relation; otherwise, we can use the
   * supplied list of remote conditions directly.
   */
  if (IS_UPPER_REL(rel)) {
    DB2FdwState*  ofpinfo = (DB2FdwState*) fpinfo->outerrel->fdw_private;
    quals = ofpinfo->remote_conds;
  } else {
    quals = remote_conds;
  }

  // Construct FROM and WHERE clauses
  deparseFromExpr(quals, &context);

  if (IS_UPPER_REL(rel)) {
    // Append GROUP BY clause
    appendGroupByClause(tlist, &context);

    // Append HAVING clause
    if (remote_conds) {
      appendStringInfoString(buf, " HAVING ");
      appendConditions(remote_conds, &context);
    }
  }

  // Add ORDER BY clause if we found any useful pathkeys
  if (pathkeys)
    appendOrderByClause(pathkeys, has_final_sort, &context);

  // Add LIMIT clause if necessary
  if (has_limit)
    appendLimitClause(&context);

  // Add any necessary FOR UPDATE/SHARE.
  deparseLockingClause(&context);
  db2Exit1(": %s", buf->data);
}

/*
 * Construct a simple SELECT statement that retrieves desired columns
 * of the specified foreign table, and append it to "buf".  The output
 * contains just "SELECT ... ".
 *
 * We also create an integer List of the columns being retrieved, which is
 * returned to *retrieved_attrs, unless we deparse the specified relation
 * as a subquery.
 *
 * tlist is the list of desired columns.  is_subquery is the flag to
 * indicate whether to deparse the specified relation as a subquery.
 * Read prologue of deparseSelectStmtForRel() for details.
 */
static void deparseSelectSql(List *tlist, bool is_subquery, List **retrieved_attrs, deparse_expr_cxt* context) {
  StringInfo          buf         = context->buf;
  RelOptInfo*         foreignrel  = context->foreignrel;
  PlannerInfo*        root        = context->root;
  DB2FdwState*        fpinfo      = (DB2FdwState*) foreignrel->fdw_private;

  db2Entry1();
  // Construct SELECT list
  appendStringInfoString(buf, "SELECT ");

  if (is_subquery) {
    // For a relation that is deparsed as a subquery, emit expressions specified in the relation's reltarget.
    // Note that since this is for the subquery, no need to care about *retrieved_attrs.
    deparseSubqueryTargetList(context);
  }
  else if (IS_JOIN_REL(foreignrel) || IS_UPPER_REL(foreignrel)) {
    // For a join or upper relation the input tlist gives the list of columns required to be fetched from the foreign server.
    deparseExplicitTargetList(tlist, false, retrieved_attrs, context);
  } else {
    /*
     * For base relations, prefer the caller-provided tlist (fdw_scan_tlist) when
     * present.  This ensures the remote SELECT list includes any resjunk Vars
     * needed locally (e.g., for EPQ recheck quals), avoiding result-column list
     * mismatches.
     */
    if (tlist != NIL) {
      deparseExplicitTargetList(tlist, false, retrieved_attrs, context);
    } else {
      // Fallback: use fpinfo->attrs_used.
      RangeTblEntry *rte = planner_rt_fetch(foreignrel->relid, root);

      // Core code already has some lock on each rel being planned, so we can use NoLock here.
      Relation	rel = table_open(rte->relid, NoLock);

      deparseTargetList(buf, rte, foreignrel->relid, rel, false, fpinfo->attrs_used, false, retrieved_attrs);
      table_close(rel, NoLock);
    }
  }
  db2Exit1(": %s", buf->data);
}

/*
 * Construct a FROM clause and, if needed, a WHERE clause, and append those to
 * "buf".
 *
 * quals is the list of clauses to be included in the WHERE clause.
 * (These may or may not include RestrictInfo decoration.)
 */
static void deparseFromExpr(List *quals, deparse_expr_cxt *context) {
  StringInfo  buf               = context->buf;
  RelOptInfo* scanrel           = context->scanrel;
  List*       additional_conds  = NIL;

  db2Entry1();
  /* For upper relations, scanrel must be either a joinrel or a baserel */
  Assert(!IS_UPPER_REL(context->foreignrel) || IS_JOIN_REL(scanrel) || IS_SIMPLE_REL(scanrel));

  /* Construct FROM clause */
  appendStringInfoString(buf, " FROM ");
  deparseFromExprForRel(buf, context->root, scanrel, (bms_membership(scanrel->relids) == BMS_MULTIPLE), (Index) 0, NULL, &additional_conds, context->params_list);
  appendWhereClause(quals, additional_conds, context);
  if (additional_conds != NIL)
    list_free_deep(additional_conds);
  db2Exit1(": %s",buf->data);
}

/*
 * Construct FROM clause for given relation
 *
 * The function constructs ... JOIN ... ON ... for join relation. For a base
 * relation it just returns schema-qualified tablename, with the appropriate
 * alias if so requested.
 *
 * 'ignore_rel' is either zero or the RT index of a target relation.  In the
 * latter case the function constructs FROM clause of UPDATE or USING clause
 * of DELETE; it deparses the join relation as if the relation never contained
 * the target relation, and creates a List of conditions to be deparsed into
 * the top-level WHERE clause, which is returned to *ignore_conds.
 *
 * 'additional_conds' is a pointer to a list of strings to be appended to
 * the WHERE clause, coming from lower-level SEMI-JOINs.
 */
static void deparseFromExprForRel(StringInfo buf, PlannerInfo* root, RelOptInfo* foreignrel, bool use_alias, Index ignore_rel, List** ignore_conds, List** additional_conds, List** params_list) {
  DB2FdwState* fpinfo = (DB2FdwState*) foreignrel->fdw_private;

  db2Entry1();
  if (IS_JOIN_REL(foreignrel)) {
    StringInfoData  join_sql_o;
    StringInfoData  join_sql_i;
    RelOptInfo*     outerrel    = fpinfo->outerrel;
    RelOptInfo*     innerrel    = fpinfo->innerrel;
    bool            outerrel_is_target = false;
    bool            innerrel_is_target = false;
    List*           additional_conds_i = NIL;
    List*           additional_conds_o = NIL;

    if (ignore_rel > 0 && bms_is_member(ignore_rel, foreignrel->relids)) {
      /* If this is an inner join, add joinclauses to *ignore_conds and set it to empty so that those can be deparsed into the WHERE
       * clause.  Note that since the target relation can never be within the nullable side of an outer join, those could safely
       * be pulled up into the WHERE clause (see foreign_join_ok()).
       * Note also that since the target relation is only inner-joined to any other relation in the query, all conditions in the join
       * tree mentioning the target relation could be deparsed into the WHERE clause by doing this recursively.
       */
      if (fpinfo->jointype == JOIN_INNER) {
        *ignore_conds = list_concat(*ignore_conds, fpinfo->joinclauses);
        fpinfo->joinclauses = NIL;
      }
      /* Check if either of the input relations is the target relation. */
      if (outerrel->relid == ignore_rel)
        outerrel_is_target = true;
      else if (innerrel->relid == ignore_rel)
        innerrel_is_target = true;
    }
    /* Deparse outer relation if not the target relation. */
    if (!outerrel_is_target) {
      initStringInfo(&join_sql_o);
      deparseRangeTblRef(&join_sql_o, root, outerrel, fpinfo->make_outerrel_subquery, ignore_rel, ignore_conds, &additional_conds_o, params_list);
      /* If inner relation is the target relation, skip deparsing it.
       * Note that since the join of the target relation with any other relation in the query is an inner join and can never be within
       * the nullable side of an outer join, the join could be interchanged with higher-level joins (cf. identity 1 on outer
       * join reordering shown in src/backend/optimizer/README), which means it's safe to skip the target-relation deparsing here.
       */
      if (innerrel_is_target) {
        Assert(fpinfo->jointype == JOIN_INNER);
        Assert(fpinfo->joinclauses == NIL);
        appendBinaryStringInfo(buf, join_sql_o.data, join_sql_o.len);
        /* Pass EXISTS conditions to upper level */
        if (additional_conds_o != NIL) {
          Assert(*additional_conds == NIL);
          *additional_conds = additional_conds_o;
        }
        db2Exit1();
        return;
      }
    }
    /* Deparse inner relation if not the target relation. */
    if (!innerrel_is_target) {
      initStringInfo(&join_sql_i);
      deparseRangeTblRef(&join_sql_i, root, innerrel, fpinfo->make_innerrel_subquery, ignore_rel, ignore_conds, &additional_conds_i, params_list);
      /* SEMI-JOIN is deparsed as the EXISTS subquery. 
       * It references outer and inner relations, so it should be evaluated as the condition in the upper-level WHERE clause.
       * We deparse the condition and pass it to upper level callers as an additional_conds list.
       * Upper level callers are responsible for inserting conditions from the list where appropriate.
       */
      if (fpinfo->jointype == JOIN_SEMI) {
        deparse_expr_cxt context;
        StringInfoData str;

        /* Construct deparsed condition from this SEMI-JOIN */
        initStringInfo(&str);
        appendStringInfo(&str, "EXISTS (SELECT NULL FROM %s", join_sql_i.data);
        context.buf         = &str;
        context.foreignrel  = foreignrel;
        context.scanrel     = foreignrel;
        context.root        = root;
        context.params_list = params_list;
        /* Append SEMI-JOIN clauses and EXISTS conditions from lower levels to the current EXISTS subquery */
        appendWhereClause(fpinfo->joinclauses, additional_conds_i, &context);
        /* EXISTS conditions, coming from lower join levels, have just been processed. */
        if (additional_conds_i != NIL) {
          list_free_deep(additional_conds_i);
          additional_conds_i = NIL;
        }
        /* Close parentheses for EXISTS subquery */
        appendStringInfoChar(&str, ')');
        *additional_conds = lappend(*additional_conds, str.data);
      }
      /* If outer relation is the target relation, skip deparsing it. See the above note about safety. */
      if (outerrel_is_target) {
        Assert(fpinfo->jointype == JOIN_INNER);
        Assert(fpinfo->joinclauses == NIL);
        appendBinaryStringInfo(buf, join_sql_i.data, join_sql_i.len);
        /* Pass EXISTS conditions to the upper call */
        if (additional_conds_i != NIL) {
          Assert(*additional_conds == NIL);
          *additional_conds = additional_conds_i;
        }
        db2Exit1();
        return;
      }
    }
    /* Neither of the relations is the target relation. */
    Assert(!outerrel_is_target && !innerrel_is_target);
    /*
     * For semijoin FROM clause is deparsed as an outer relation. An inner
     * relation and join clauses are converted to EXISTS condition and
     * passed to the upper level.
     */
    if (fpinfo->jointype == JOIN_SEMI) {
      appendBinaryStringInfo(buf, join_sql_o.data, join_sql_o.len);
    } else {
      /* For a join relation FROM clause, entry is deparsed as ((outer relation) <join type> (inner relation) ON (joinclauses)) */
      appendStringInfo(buf, "(%s %s JOIN %s ON ", join_sql_o.data,  get_jointype_name(fpinfo->jointype), join_sql_i.data);
      /* Append join clause; (TRUE) if no join clause */
      if (fpinfo->joinclauses) {
        deparse_expr_cxt context;

        context.buf         = buf;
        context.foreignrel  = foreignrel;
        context.scanrel     = foreignrel;
        context.root        = root;
        context.params_list = params_list;
        appendStringInfoChar(buf, '(');
        appendConditions(fpinfo->joinclauses, &context);
        appendStringInfoChar(buf, ')');
      } else {
        appendStringInfoString(buf, "(TRUE)");
      }
      /* End the FROM clause entry. */
      appendStringInfoChar(buf, ')');
    }
    /* Construct additional_conds to be passed to the upper caller from current level additional_conds and additional_conds, coming from inner and outer rels. */
    if (additional_conds_o != NIL) {
      *additional_conds = list_concat(*additional_conds, additional_conds_o);
      list_free(additional_conds_o);
    }
    if (additional_conds_i != NIL) {
      *additional_conds = list_concat(*additional_conds, additional_conds_i);
      list_free(additional_conds_i);
    }
  } else {
    RangeTblEntry *rte = planner_rt_fetch(foreignrel->relid, root);
    /* Core code already has some lock on each rel being planned, so we can use NoLock here. */
    Relation	rel = table_open(rte->relid, NoLock);
    deparseRelation(buf, rel);
    /* Add a unique alias to avoid any conflict in relation names due to pulled up subqueries in the query being built for a pushed down join. */
    if (use_alias)
      appendStringInfo(buf, " %s%d", REL_ALIAS_PREFIX, foreignrel->relid);
    table_close(rel, NoLock);
  }
  db2Exit1();
}

/* Append FROM clause entry for the given relation into buf.
 * Conditions from lower-level SEMI-JOINs are appended to additional_conds and should be added to upper level WHERE clause.
 */
static void deparseRangeTblRef(StringInfo buf, PlannerInfo *root, RelOptInfo *foreignrel, bool make_subquery, Index ignore_rel, List **ignore_conds, List **additional_conds, List **params_list) {
  DB2FdwState* fpinfo = (DB2FdwState*) foreignrel->fdw_private;

  db2Entry1();
  /* Should only be called in these cases. */
  Assert(IS_SIMPLE_REL(foreignrel) || IS_JOIN_REL(foreignrel));
  Assert(fpinfo->local_conds == NIL);
  /* If make_subquery is true, deparse the relation as a subquery. */
  if (make_subquery) {
    List* retrieved_attrs;
    int   ncols;

    /* The given relation shouldn't contain the target relation, because this should only happen for input relations for a full join, and
     * such relations can never contain an UPDATE/DELETE target.
     */
    Assert(ignore_rel == 0 || !bms_is_member(ignore_rel, foreignrel->relids));

    /* Deparse the subquery representing the relation. */
    appendStringInfoChar(buf, '(');
    deparseSelectStmtForRel(buf, root, foreignrel, NIL, fpinfo->remote_conds, NIL, false, false, true, &retrieved_attrs, params_list);
    appendStringInfoChar(buf, ')');

    /* Append the relation alias. */
    appendStringInfo(buf, " %s%d", SUBQUERY_REL_ALIAS_PREFIX, fpinfo->relation_index);

    /* Append the column aliases if needed.  Note that the subquery emits expressions specified in the relation's reltarget
     * (see deparseSubqueryTargetList).
     */
    ncols = list_length(foreignrel->reltarget->exprs);
    if (ncols > 0) {
      int i;

      appendStringInfoChar(buf, '(');
      for (i = 1; i <= ncols; i++) {
        if (i > 1) {
          appendStringInfoString(buf, ", ");
        }
        appendStringInfo(buf, "%s%d", SUBQUERY_COL_ALIAS_PREFIX, i);
      }
      appendStringInfoChar(buf, ')');
    }
  } else {
    deparseFromExprForRel(buf, root, foreignrel, true, ignore_rel, ignore_conds, additional_conds, params_list);
  }
  db2Exit1(": %s", buf->data);
}

/** deparseWhereConditions
 *   Classify conditions into remote_conds or local_conds.
 *   Those conditions that can be pushed down will be collected into
 *   an DB2 WHERE clause that is returned.
 */
char* deparseWhereConditions (PlannerInfo* root, RelOptInfo * rel) {
  List*          conditions = rel->baserestrictinfo;
  DB2FdwState*   fdwState   = (DB2FdwState*) rel->fdw_private;
  ListCell*      cell;
  char*          where;
  char*          keyword = "WHERE";
  StringInfoData where_clause;

  db2Entry1();
  initStringInfo (&where_clause);
  foreach (cell, conditions) {
    /* check if the condition can be pushed down */
    where = deparseExpr (root, rel, ((RestrictInfo*) lfirst (cell))->clause, &(fdwState->params));
    if (where != NULL) {
      fdwState->remote_conds = lappend (fdwState->remote_conds, ((RestrictInfo*) lfirst (cell))->clause);

      /* append new WHERE clause to query string */
      appendStringInfo (&where_clause, " %s %s", keyword, where);
      keyword = "AND";
      db2free (where, "where");
    } else {
      fdwState->local_conds = lappend (fdwState->local_conds, ((RestrictInfo*) lfirst (cell))->clause);
    }
  }
  db2Exit1(": %s",where_clause.data);
  return where_clause.data;
}

/* Construct a simple "TRUNCATE rel" statement */
void deparseTruncateSql(StringInfo buf, List* rels, DropBehavior behavior, bool restart_seqs) {
  ListCell* cell  = NULL;

  db2Entry1();
  appendStringInfoString(buf, "TRUNCATE ");
  foreach(cell, rels) {
    Relation	rel = lfirst(cell);

    if (cell != list_head(rels))
      appendStringInfoString(buf, ", ");
    deparseRelation(buf, rel);
  }
  appendStringInfo(buf, " %s IDENTITY", restart_seqs ? "RESTART" : "CONTINUE");
  if (behavior == DROP_RESTRICT)
    appendStringInfoString(buf, " RESTRICT");
  else if (behavior == DROP_CASCADE)
    appendStringInfoString(buf, " CASCADE");
  db2Exit1(": %s",buf->data);
}

/* Construct name to use for given column, and emit it into buf.
 * If it has a column_name FDW option, use that instead of attribute name.
 *
 * If qualify_col is true, qualify column name with the alias of relation.
 */
static void deparseColumnRef(StringInfo buf, int varno, int varattno, RangeTblEntry *rte, bool qualify_col) {
  db2Entry1();
  /* We support fetching the remote side's CTID and OID. */
  if (varattno == SelfItemPointerAttributeNumber) {
    if (qualify_col)
      ADD_REL_QUALIFIER(buf, varno);
    appendStringInfoString(buf, "ctid");
  } else if (varattno < 0) {
    /* All other system attributes are fetched as 0, except for table OID, which is fetched as the local table OID.
     * However, we must be careful; the table could be beneath an outer join, in which case it must go to NULL whenever the rest of the row does.
     */
    Oid fetchval = 0;

    if (varattno == TableOidAttributeNumber)
      fetchval = rte->relid;

    if (qualify_col) {
      appendStringInfoString(buf, "CASE WHEN (");
      ADD_REL_QUALIFIER(buf, varno);
      appendStringInfo(buf, "*)::text IS NOT NULL THEN %u END", fetchval);
    } else {
      appendStringInfo(buf, "%u", fetchval);
    }
  } else if (varattno == 0) {
    /* Whole row reference */
    Relation	  rel;
    Bitmapset*  attrs_used;
    /* Required only to be passed down to deparseTargetList(). */
    List*       retrieved_attrs;

    /* The lock on the relation will be held by upper callers, so it's fine to open it with no lock here. */
    rel = table_open(rte->relid, NoLock);

    /* The local name of the foreign table can not be recognized by the foreign server and the table it references on foreign server might
     * have different column ordering or different columns than those declared locally. Hence we have to deparse whole-row reference as
     * ROW(columns referenced locally). Construct this by deparsing a "whole row" attribute.
     */
    attrs_used = bms_add_member(NULL, 0 - FirstLowInvalidHeapAttributeNumber);

    /* In case the whole-row reference is under an outer join then it has to go NULL whenever the rest of the row goes NULL. Deparsing a join
     * query would always involve multiple relations, thus qualify_col would be true.
     */
    if (qualify_col) {
      appendStringInfoString(buf, "CASE WHEN (");
      ADD_REL_QUALIFIER(buf, varno);
      appendStringInfoString(buf, "*)::text IS NOT NULL THEN ");
    }

    appendStringInfoString(buf, "ROW(");
    deparseTargetList(buf, rte, varno, rel, false, attrs_used, qualify_col, &retrieved_attrs);
    appendStringInfoChar(buf, ')');

    /* Complete the CASE WHEN statement started above. */
    if (qualify_col)
      appendStringInfoString(buf, " END");

    table_close(rel, NoLock);
    bms_free(attrs_used);
  } else {
    char*     colname = NULL;
    List*     options = NIL;
    ListCell* lc      = NULL;

    /* varno must not be any of OUTER_VAR, INNER_VAR and INDEX_VAR. */
    Assert(!IS_SPECIAL_VARNO(varno));

    /* If it's a column of a foreign table, and it has the column_name FDW option, use that value. */
    options = GetForeignColumnOptions(rte->relid, varattno);
    foreach(lc, options) {
      DefElem*  def = (DefElem*) lfirst(lc);
      if (strcmp(def->defname, "column_name") == 0) {
        colname = defGetString(def);
        break;
      }
    }

    /* If it's a column of a regular table or it doesn't have column_name FDW option, use attribute name. */
    if (colname == NULL)
      colname = get_attname(rte->relid, varattno, false);

    if (qualify_col)
      ADD_REL_QUALIFIER(buf, varno);

    appendStringInfoString(buf, quote_identifier(str_toupper (colname, strlen (colname), DEFAULT_COLLATION_OID)));
  }
  db2Exit1(": %s", buf->data);
}

/* Append remote name of specified foreign table to buf.
 * Use value of table_name FDW option (if any) instead of relation's name.
 * Similarly, schema_name FDW option overrides schema name.
 */
static void deparseRelation(StringInfo buf, Relation rel) {
  ForeignTable* table   = NULL;
  const char*   nspname = NULL;
  const char*   relname = NULL;
  ListCell*     lc      = NULL;

  db2Entry1();
  /* obtain additional catalog information. */
  table = GetForeignTable(RelationGetRelid(rel));

  /*
   * Use value of FDW options if any, instead of the name of object itself.
   */
  foreach(lc, table->options) {
    DefElem*  def = (DefElem*) lfirst(lc);

    if (strcmp(def->defname, "schema") == 0)
      nspname = defGetString(def);
    else if (strcmp(def->defname, "table") == 0)
    relname = defGetString(def);
  }

  /* Note: we could skip printing the schema name if it's pg_catalog, but that doesn't seem worth the trouble. */
  if (nspname == NULL)
    nspname = get_namespace_name(RelationGetNamespace(rel));
  if (relname == NULL)
    relname = RelationGetRelationName(rel);

  appendStringInfo(buf, "%s.%s", quote_identifier(nspname), quote_identifier(relname));
  db2Debug5("relation: %s",buf->data);
  db2Exit1();
}

/* Append a SQL string literal representing "val" to buf. */
void deparseStringLiteral(StringInfo buf, const char* val) {
  const char* valptr = NULL;

  db2Entry1();
  /* Rather than making assumptions about the remote server's value of standard_conforming_strings, always use E'foo' syntax if there are any
   * backslashes.
   * This will fail on remote servers before 8.1, but those are long out of support.
   */
  if (strchr(val, '\\') != NULL) {
    appendStringInfoChar(buf, ESCAPE_STRING_SYNTAX);
  }
  appendStringInfoChar(buf, '\'');
  for (valptr = val; *valptr; valptr++) {
    char    ch = *valptr;

    if (SQL_STR_DOUBLE(ch, true)) {
      appendStringInfoChar(buf, ch);
    }
    appendStringInfoChar(buf, ch);
  }
  appendStringInfoChar(buf, '\'');
  db2Debug5("literal: %s",buf->data);
  db2Exit1();
}

/** deparseExpr
 *   Create and return an DB2 SQL string from "expr".
 *   Returns NULL if that is not possible, else an allocated string.
 *   As a side effect, all Params incorporated in the WHERE clause
 *   will be stored in "params".
 */
char* deparseExpr (PlannerInfo* root, RelOptInfo* rel, Expr* expr, List** params) {
  char* retValue = NULL;
  db2Entry1();
  if (expr != NULL) {
    deparse_expr_cxt* ctx = db2alloc(sizeof(deparse_expr_cxt),"deparseExpr.context");
    StringInfoData    buf;

    initStringInfo(&buf);

    ctx->root        = root;
    ctx->foreignrel  = rel;
    ctx->scanrel     = rel;
    ctx->buf         = &buf;
    ctx->params_list = params;
    deparseExprInt(expr, ctx);
    retValue = (buf.len > 0) ? db2strdup(buf.data,"buf.data") : NULL;
    db2free(ctx->buf->data,"ctx->buf->data");
    db2free(ctx, "ctx");
  }
  db2Exit1(": %s", retValue);
  return retValue;
}

static void deparseExprInt           (Expr*              expr, deparse_expr_cxt* ctx) {
  db2Entry1();
  db2Debug2("expr: %x",expr);
  if (expr != NULL) {
    db2Debug2("expr->type: %d",expr->type);
    switch (expr->type) {
      case T_Const: {
        deparseConstExpr((Const*)expr, ctx);
      }
      break;
      case T_Param: {
        deparseParamExpr((Param*) expr, ctx);
      }
      break;
      case T_Var: {
//        deparseVarExpr ((Var*)expr, ctx);
        deparseVar ((Var*)expr, ctx);
      }
      break;
      case T_OpExpr: {
        deparseOpExpr ((OpExpr*)expr, ctx);
      }
      break;
      case T_ScalarArrayOpExpr: {
        deparseScalarArrayOpExpr ((ScalarArrayOpExpr*)expr, ctx);
      }
      break;
      case T_DistinctExpr: {
        deparseDistinctExpr ((DistinctExpr*)expr, ctx);
      }
      break;
      case T_NullIfExpr: {
        deparseNullIfExpr ((NullIfExpr*)expr, ctx);
      }
      break;
      case T_BoolExpr: {
        deparseBoolExpr ((BoolExpr*)expr, ctx);
      }
      break;
      case T_RelabelType: {
        deparseExprInt (((RelabelType*)expr)->arg, ctx);
      }
      break;
      case T_CoerceToDomain: {
        deparseExprInt (((CoerceToDomain*)expr)->arg, ctx);
      }
      break;
      case T_CaseExpr: {
        deparseCaseExpr ((CaseExpr*)expr, ctx);
      }
      break;
      case T_CoalesceExpr: {
        deparseCoalesceExpr ((CoalesceExpr*)expr, ctx);
      }
      break;
      case T_NullTest: {
        deparseNullTest((NullTest*) expr, ctx);
      }
      break;
      case T_FuncExpr: {
        deparseFuncExpr((FuncExpr*)expr, ctx);
      }
      break;
      case T_CoerceViaIO: {
        deparseCoerceViaIOExpr((CoerceViaIO*) expr, ctx);
      }
      break;
      case T_SQLValueFunction: {
        deparseSQLValueFuncExpr((SQLValueFunction*)expr, ctx);
      }
      break;
      case T_Aggref: {
        deparseAggref((Aggref*)expr, ctx);
      }
      break;
      default: {
        /* we cannot translate this to DB2 */
        db2Debug2("expression cannot be translated to DB2");
      }
      break;
    }
  }
  db2Exit1(": %s", ctx->buf->data);
}

static void deparseConstExpr         (Const*             expr, deparse_expr_cxt* ctx) {
  db2Entry1();
  if (expr->constisnull) {
    /* only translate NULLs of a type DB2 can handle */
    if (canHandleType (expr->consttype)) {
      appendStringInfo (ctx->buf, "NULL");
    }
  } else {
    /* get a string representation of the value */
    char* c = datumToString (expr->constvalue, expr->consttype);
    if (c != NULL) {
      appendStringInfo (ctx->buf, "%s", c);
    }
  }
  db2Exit1();
}

static void deparseParamExpr         (Param*             expr, deparse_expr_cxt* ctx) {
  ListCell* cell  = NULL;
  char      parname[10];

  db2Entry1();
  /* don't try to handle interval parameters */
  if (!canHandleType (expr->paramtype) || expr->paramtype == INTERVALOID) {
    db2Debug2("!canHhandleType(expr->paramtype %d) || rxpr->paramtype == INTERVALOID)", expr->paramtype);
  } else {
    /* find the index in the parameter list */
    int index = 0;
    foreach (cell, *(ctx->params_list)) {
      ++index;
      if (equal (expr, (Node*) lfirst (cell)))
        break;
    }
    if (cell == NULL) {
      /* add the parameter to the list */
      ++index;
      *(ctx->params_list) = lappend (*(ctx->params_list), expr);
    }
    /* parameters will be called :p1, :p2 etc. */
    snprintf (parname, 10, ":p%d", index);
    appendAsType (ctx->buf, expr->paramtype);
  }
  db2Exit1();
}

//static void deparseVarExpr           (Var*               expr, deparse_expr_cxt* ctx) {
//  const DB2Table*  var_table = NULL;  /* db2Table that belongs to a Var */
//
//  db2Entry1();
//  /* check if the variable belongs to one of our foreign tables */
//  if (IS_SIMPLE_REL (ctx->foreignrel)) {
//    if (expr->varno == ctx->foreignrel->relid && expr->varlevelsup == 0)
//      var_table = ((DB2FdwState*)ctx->foreignrel->fdw_private)->db2Table;
//  } else {
//    DB2FdwState* joinstate  = (DB2FdwState*) ctx->foreignrel->fdw_private;
//    DB2FdwState* outerstate = (DB2FdwState*) joinstate->outerrel->fdw_private;
//    DB2FdwState* innerstate = (DB2FdwState*) joinstate->innerrel->fdw_private;
//    /* we can't get here if the foreign table has no columns, so this is safe */
//    if (expr->varno == outerstate->db2Table->cols[0]->varno && expr->varlevelsup == 0)
//      var_table = outerstate->db2Table;
//    if (expr->varno == innerstate->db2Table->cols[0]->varno && expr->varlevelsup == 0)
//      var_table = innerstate->db2Table;
//  }
//  if (var_table) {
//    /* the variable belongs to a foreign table, replace it with the name */
//    /* we cannot handle system columns */
//    db2Debug2("varattno: %d",expr->varattno);
//    if (expr->varattno > 0) {
//      /** Allow boolean columns here.
//       * They will be rendered as ("COL" <> 0).
//       */
//      if (!(canHandleType (expr->vartype) || expr->vartype == BOOLOID)) {
//        db2Debug2("!(canHandleType (vartype %d) || vartype == BOOLOID",expr->vartype);
//      } else {
//        /* get var_table column index corresponding to this column (-1 if none) */
//        int index = var_table->ncols - 1;
//        while (index >= 0 && var_table->cols[index]->pgattnum != expr->varattno) {
//          --index;
//        }
//        /* if no DB2 column corresponds, translate as NULL */
//        if (index == -1) {
//          appendStringInfo (ctx->buf, "NULL");
//        } else {
//          /** Don't try to convert a column reference if the type is
//           * converted from a non-string type in DB2 to a string type
//           * in PostgreSQL because functions and operators won't work the same.
//           */
//          short db2type = c2dbType(var_table->cols[index]->colType);
//          db2Debug2("db2type: %d", db2type);
//          if ((expr->vartype == TEXTOID || expr->vartype == BPCHAROID || expr->vartype == VARCHAROID)  && db2type != DB2_VARCHAR && db2type != DB2_CHAR) {
//            db2Debug2("vartype: %d", expr->vartype);
//          } else {
//            /* work around the lack of booleans in DB2 */
//            if (expr->vartype == BOOLOID) {
//              appendStringInfo (ctx->buf, "(");
//            }
//            /* qualify with an alias based on the range table index */
//            appendStringInfo(ctx->buf, "%s%d.%s", "r", var_table->cols[index]->varno, var_table->cols[index]->colName);
//            /* work around the lack of booleans in DB2 */
//            if (expr->vartype == BOOLOID) {
//              appendStringInfo (ctx->buf, " <> 0)");
//            }
//          }
//        }
//      }
//    }
//  }
//  db2Exit1();
//}

/* Deparse given Var node into context->buf.
 *
 * If the Var belongs to the foreign relation, just print its remote name. 
 * Otherwise, it's effectively a Param (and will in fact be a Param at run time).  
 * Handle it the same way we handle plain Params --- see deparseParam for comments.
 */
static void deparseVar(Var* expr, deparse_expr_cxt* ctx) {
  int     relno  = 0;
  int     colno  = 0;
  /* Qualify columns when multiple relations are involved. */
  bool    qualify_col = (bms_membership( ctx->scanrel->relids) == BMS_MULTIPLE);
  bool    is_query_var = false;

  db2Entry1();
  /* If the Var belongs to the foreign relation that is deparsed as a subquery, use the relation and column alias to the Var provided by the
   * subquery, instead of the remote name.
   */
  if (is_subquery_var(expr, ctx->scanrel, &relno, &colno)) {
    appendStringInfo(ctx->buf, "%s%d.%s%d", SUBQUERY_REL_ALIAS_PREFIX, relno, SUBQUERY_COL_ALIAS_PREFIX, colno);
    return;
  }
  #if PG_VERSION_NUM < 160000
  is_query_var = bms_is_member(expr->varno, ctx->scanrel->relids);
  db2Debug2("bms_is_member(%d,%d): %s",expr->varno, ctx->scanrel->relids, is_query_var ? "true":"false");
  #else
  is_query_var = bms_is_member(expr->varno, ctx->root->all_query_rels);  
  db2Debug2("bms_is_member(%d,%d): %s",expr->varno, ctx->root->all_query_rels, is_query_var ? "true":"false");
  #endif
  db2Debug2("expr->varlevelsup: %d",expr->varlevelsup);
  if (is_query_var && expr->varlevelsup == 0) {
    deparseColumnRef(ctx->buf, expr->varno, expr->varattno, planner_rt_fetch(expr->varno, ctx->root), qualify_col);
  } else {
    /* Treat like a Param */
    if (ctx->params_list) {
      int       pindex  = 0;
      ListCell* lc      = NULL;

      /* find its index in params_list */
      foreach(lc, *ctx->params_list) {
        pindex++;
        if (equal(expr, (Node*) lfirst(lc)))
          break;
      }
      if (lc == NULL) {
        /* not in list, so add it */
        pindex++;
        *ctx->params_list = lappend(*ctx->params_list, expr);
      }
      printRemoteParam(pindex, expr->vartype, expr->vartypmod, ctx);
    } else {
      printRemotePlaceholder(expr->vartype, expr->vartypmod, ctx);
    }
  }
  db2Exit1(": %s",ctx->buf->data);
}

static void deparseOpExpr            (OpExpr*            expr, deparse_expr_cxt* ctx) {
  char*     opername    = NULL;
  char      oprkind     = 0x00;
  Oid       rightargtype= 0;
  Oid       leftargtype = 0;
  Oid       schema      = 0;
  HeapTuple tuple       ;

  db2Entry1();
  /* get operator name, kind, argument type and schema */
  tuple = SearchSysCache1 (OPEROID, ObjectIdGetDatum (expr->opno));
  if (!HeapTupleIsValid (tuple)) {
    elog (ERROR, "cache lookup failed for operator %u", expr->opno);
  }
  opername     = db2strdup (((Form_pg_operator) GETSTRUCT (tuple))->oprname.data,"opername");
  oprkind      = ((Form_pg_operator) GETSTRUCT (tuple))->oprkind;
  leftargtype  = ((Form_pg_operator) GETSTRUCT (tuple))->oprleft;
  rightargtype = ((Form_pg_operator) GETSTRUCT (tuple))->oprright;
  schema       = ((Form_pg_operator) GETSTRUCT (tuple))->oprnamespace;
  ReleaseSysCache (tuple);
  /* ignore operators in other than the pg_catalog schema */
  if (schema != PG_CATALOG_NAMESPACE) {
    db2Debug2("schema != PG_CATALOG_NAMESPACE");
  } else {
    if (!canHandleType (rightargtype)) {
      db2Debug2("!canHandleType rightargtype(%d)", rightargtype);
    } else {
      /** Don't translate operations on two intervals.
     * INTERVAL YEAR TO MONTH and INTERVAL DAY TO SECOND don't mix well.
     */
      if (leftargtype == INTERVALOID && rightargtype == INTERVALOID) {
        db2Debug2("leftargtype == INTERVALOID && rightargtype == INTERVALOID");
      } else {
        /* the operators that we can translate */
        if ((strcmp (opername, ">")    == 0 && rightargtype != TEXTOID && rightargtype != BPCHAROID    && rightargtype != NAMEOID && rightargtype != CHAROID)
        ||  (strcmp (opername, "<")    == 0 && rightargtype != TEXTOID && rightargtype != BPCHAROID    && rightargtype != NAMEOID && rightargtype != CHAROID)
        ||  (strcmp (opername, ">=")   == 0 && rightargtype != TEXTOID && rightargtype != BPCHAROID    && rightargtype != NAMEOID && rightargtype != CHAROID)
        ||  (strcmp (opername, "<=")   == 0 && rightargtype != TEXTOID && rightargtype != BPCHAROID    && rightargtype != NAMEOID && rightargtype != CHAROID)
        ||  (strcmp (opername, "-")    == 0 && rightargtype != DATEOID && rightargtype != TIMESTAMPOID && rightargtype != TIMESTAMPTZOID)
        ||   strcmp (opername, "=")    == 0 || strcmp (opername, "<>")  == 0 || strcmp (opername, "+")   == 0 ||   strcmp (opername, "*")    == 0 
        ||   strcmp (opername, "~~")   == 0 || strcmp (opername, "!~~") == 0 || strcmp (opername, "~~*") == 0 ||   strcmp (opername, "!~~*") == 0 
        ||   strcmp (opername, "^")    == 0 || strcmp (opername, "%")   == 0 || strcmp (opername, "&")   == 0 ||   strcmp (opername, "|/")   == 0
        ||   strcmp (opername, "@")  == 0) {
          char* left = NULL;

          left = deparseExpr (ctx->root, ctx->foreignrel, linitial(expr->args), ctx->params_list);
          db2Debug2("left: %s", left);
          if (left != NULL) {
            if (oprkind == 'b') {
              /* binary operator */
              char* right = NULL;

              right = deparseExpr (ctx->root, ctx->foreignrel, lsecond(expr->args), ctx->params_list);
              db2Debug2("right: %s", right);
              if (right != NULL) {
                if (strcmp (opername, "~~") == 0) {
                  appendStringInfo (ctx->buf, "(%s LIKE %s ESCAPE '\\')", left, right);
                } else if (strcmp (opername, "!~~") == 0) {
                  appendStringInfo (ctx->buf, "(%s NOT LIKE %s ESCAPE '\\')", left, right);
                } else if (strcmp (opername, "~~*") == 0) {
                  appendStringInfo (ctx->buf, "(UPPER(%s) LIKE UPPER(%s) ESCAPE '\\')", left, right);
                } else if (strcmp (opername, "!~~*") == 0) {
                  appendStringInfo (ctx->buf, "(UPPER(%s) NOT LIKE UPPER(%s) ESCAPE '\\')", left, right);
                } else if (strcmp (opername, "^") == 0) {
                  appendStringInfo (ctx->buf, "POWER(%s, %s)", left, right);
                } else if (strcmp (opername, "%") == 0) {
                  appendStringInfo (ctx->buf, "MOD(%s, %s)", left, right);
                } else if (strcmp (opername, "&") == 0) {
                  appendStringInfo (ctx->buf, "BITAND(%s, %s)", left, right);
                } else {
                  /* the other operators have the same name in DB2 */
                  appendStringInfo (ctx->buf, "(%s %s %s)", left, opername, right);
                }
                db2free(right,"right");
              }
            } else {
              /* unary operator */
              if (strcmp (opername, "|/") == 0) {
                appendStringInfo (ctx->buf, "SQRT(%s)", left);
              } else if (strcmp (opername, "@") == 0) {
                appendStringInfo (ctx->buf, "ABS(%s)", left);
              } else {
                /* unary + or - */
                appendStringInfo (ctx->buf, "(%s%s)", opername, left);
              }
            }
            db2free(left,"left");
          }
        } else {
          /* cannot translate this operator */
          db2Debug2("cannot translate this opername: %s", opername);
        }
      }
    }
  }
  db2free (opername,"opername");
  db2Exit1();
}

static void deparseScalarArrayOpExpr (ScalarArrayOpExpr* expr, deparse_expr_cxt* ctx) {
  char*              opername;
  Oid                leftargtype;
  Oid                schema;
  HeapTuple          tuple;

  db2Entry1();
  tuple = SearchSysCache1 (OPEROID, ObjectIdGetDatum (expr->opno));
  if (!HeapTupleIsValid (tuple)) {
    elog (ERROR, "cache lookup failed for operator %u", expr->opno);
  }
  opername    = db2strdup(((Form_pg_operator) GETSTRUCT (tuple))->oprname.data,"opername");
  leftargtype =           ((Form_pg_operator) GETSTRUCT (tuple))->oprleft;
  schema      =           ((Form_pg_operator) GETSTRUCT (tuple))->oprnamespace;
  ReleaseSysCache (tuple);
  /* get the type's output function */
  tuple = SearchSysCache1 (TYPEOID, ObjectIdGetDatum (leftargtype));
  if (!HeapTupleIsValid (tuple)) {
    elog (ERROR, "cache lookup failed for type %u", leftargtype);
  }
  ReleaseSysCache (tuple);
  /* ignore operators in other than the pg_catalog schema */
  if (schema != PG_CATALOG_NAMESPACE) {
    db2Debug2("schema != PG_CATALOG_NAMESPACE");
  } else {
    /* don't try to push down anything but IN and NOT IN expressions */
    if ((strcmp (opername, "=") != 0 || !expr->useOr) && (strcmp (opername, "<>") != 0 || expr->useOr)) {
      db2Debug2("don't try to push down anything but IN and NOT IN expressions");
    } else {
      if (!canHandleType (leftargtype)) {
        db2Debug2("cannot Handle Type leftargtype (%d)", leftargtype);
      } else {
        char* left  = NULL;
        char* right = NULL;

        left = deparseExpr (ctx->root, ctx->foreignrel,linitial (expr->args), ctx->params_list);
        // check if anything has been added beyond the initial "("
        if (left != NULL) {
          Expr* rightexpr = NULL;
          bool  bResult   = true;

          /* the second (=last) argument can be Const, ArrayExpr or ArrayCoerceExpr */
          rightexpr = (Expr*)llast(expr->args);
          switch (rightexpr->type) {
            case T_Const: {
              StringInfoData buf;
              /* the second (=last) argument is a Const of ArrayType */
              Const* constant = (Const*) rightexpr;
              /* using NULL in place of an array or value list is valid in DB2 and PostgreSQL */
              initStringInfo(&buf);
              if (constant->constisnull) {
                appendStringInfo(&buf, "NULL");
                right = db2strdup(buf.data,"buf.data");
              } else {
                Datum          datum;
                bool           isNull;
                ArrayIterator  iterator = array_create_iterator (DatumGetArrayTypeP (constant->constvalue), 0, NULL);
                bool           first_arg = true;

                /* loop through the array elements */
                while (array_iterate (iterator, &datum, &isNull)) {
                  char *c;
                  if (isNull) {
                    c = "NULL";
                  } else {
                    c = datumToString (datum, leftargtype);
                    db2Debug2("c: %s",c);
                    if (c == NULL) {
                      array_free_iterator (iterator);
                      bResult = false;
                      break;
                    }
                  }
                  /* append the argument */
                  appendStringInfo (&buf, "%s%s", first_arg ? "" : ", ", c);
                  first_arg = false;
                }
                array_free_iterator (iterator);
                db2Debug2("first_arg: %s", first_arg ? "true":"false");
                if (first_arg) {
                  // don't push down empty arrays
                  // since the semantics for NOT x = ANY(<empty array>) differ
                  bResult = false;
                }
                if (bResult) {
                  right = db2strdup(buf.data,"buf.data");
                }
              }
              db2free(buf.data,"buf.data");
            }
            break;
            case T_ArrayCoerceExpr: {
              /* the second (=last) argument is an ArrayCoerceExpr */
              ArrayCoerceExpr* arraycoerce = (ArrayCoerceExpr *) rightexpr;
              /* if the conversion requires more than binary coercion, don't push it down */
              if (arraycoerce->elemexpr && arraycoerce->elemexpr->type != T_RelabelType) {
                db2Debug2("arraycoerce->elemexpr && arraycoerce->elemexpr->type != T_RelabelType");
                bResult = false;
                break;
              }
              /* the actual array is here */
              rightexpr = arraycoerce->arg;
            }
            /* fall through ! */
            case T_ArrayExpr: {
              /* the second (=last) argument is an ArrayExpr */
              StringInfoData buf;
              char*          element   = NULL;
              ArrayExpr*     array     = (ArrayExpr*) rightexpr;
              ListCell*      cell      = NULL;
              bool           first_arg = true;
              
              initStringInfo(&buf);
              /* loop the array arguments */
              foreach (cell, array->elements) {
                element = deparseExpr (ctx->root, ctx->foreignrel, (Expr*) lfirst (cell), ctx->params_list);
                if (element == NULL) {
                  /* if any element cannot be converted, give up */
                  db2free(buf.data,"buf.data");
                  bResult = false;
                  break;
                }
                appendStringInfo(&buf,"%s%s",(first_arg) ? "": ", ",element);
                first_arg = false;
              }
              db2Debug2("first_arg: %s", first_arg ? "true" : "false");
              if (first_arg) {
                /* don't push down empty arrays, since the semantics for NOT x = ANY(<empty array>) differ */
                db2free(buf.data,"buf.data");
                bResult = false;
                break;
              }
              right = (bResult) ? db2strdup(buf.data,"buf.data") : NULL;
              db2free(buf.data,"buf.data");
            }
            break;
            default: {
              db2Debug2("rightexpr->type(%d) default ",rightexpr->type);
              bResult = false;
            }
            break;
          }
          // only when there is a usable result otherwise keep value to null
          if (bResult) {
            appendStringInfo (ctx->buf, "(%s %s IN (%s))",left, expr->useOr ? "" : "NOT", right);
          }
          db2free(left,"left");
          db2free(right,"right");
        }
      }
    }
  }
  db2Exit1();
}

static void deparseDistinctExpr      (DistinctExpr*      expr, deparse_expr_cxt* ctx) {
  Oid       rightargtype = 0;
  HeapTuple tuple;

  db2Entry1();
  tuple = SearchSysCache1 (OPEROID, ObjectIdGetDatum ((expr)->opno));
  if (!HeapTupleIsValid (tuple)) {
    elog (ERROR, "cache lookup failed for operator %u", (expr)->opno);
  }
  rightargtype = ((Form_pg_operator) GETSTRUCT (tuple))->oprright;
  ReleaseSysCache (tuple);
  if (!canHandleType (rightargtype)) {
    db2Debug2("cannot Handle Type rightargtype (%d)",rightargtype);
  } else {
    char* left  = NULL;

    left = deparseExpr (ctx->root, ctx->foreignrel, linitial ((expr)->args), ctx->params_list);
    if (left != NULL) {
      char* right = NULL;

      right = deparseExpr (ctx->root, ctx->foreignrel, lsecond ((expr)->args), ctx->params_list);
      if (right != NULL) {
        appendStringInfo (ctx->buf, "( %s IS DISTINCT FROM %s)", left, right);
      }
      db2free(right,"right");
    }
    db2free(left,"left");
  }
  db2Exit1();
}

/** Deparse IS [NOT] NULL expression.
 */
static void deparseNullTest          (NullTest*          expr, deparse_expr_cxt* ctx) {
  StringInfo	buf = ctx->buf;

  db2Entry1();
  appendStringInfoChar(buf, '(');
  deparseExprInt (expr->arg, ctx);

  /** For scalar inputs, we prefer to print as IS [NOT] NULL, which is
   *  shorter and traditional.  If it's a rowtype input but we're applying a
   *  scalar test, must print IS [NOT] DISTINCT FROM NULL to be semantically
   *  correct.
   */
  if (expr->argisrow || !type_is_rowtype(exprType((Node *) expr->arg))) {
    if (expr->nulltesttype == IS_NULL)
      appendStringInfoString(buf, " IS NULL)");
    else
      appendStringInfoString(buf, " IS NOT NULL)");
  } else {
    if (expr->nulltesttype == IS_NULL)
      appendStringInfoString(buf, " IS NOT DISTINCT FROM NULL)");
    else
      appendStringInfoString(buf, " IS DISTINCT FROM NULL)");
  }
  db2Exit1(": %s", buf->data);
}

static void deparseNullIfExpr        (NullIfExpr*        expr, deparse_expr_cxt* ctx) {
  Oid       rightargtype = 0;
  HeapTuple tuple;

  db2Entry1();
  tuple        = SearchSysCache1 (OPEROID, ObjectIdGetDatum ((expr)->opno));
  if (!HeapTupleIsValid (tuple)) {
    elog (ERROR, "cache lookup failed for operator %u", (expr)->opno);
  }
  rightargtype = ((Form_pg_operator) GETSTRUCT (tuple))->oprright;
  ReleaseSysCache (tuple);
  if (!canHandleType (rightargtype)) {
    db2Debug2("cannot Handle Type rightargtype (%d)",rightargtype);
  } else {
    char* left = NULL;
    left = deparseExpr (ctx->root, ctx->foreignrel, linitial((expr)->args), ctx->params_list);

    if (left != NULL) {
      char* right = NULL;

      right = deparseExpr (ctx->root, ctx->foreignrel, lsecond((expr)->args), ctx->params_list);
      if (right != NULL) {
        appendStringInfo (ctx->buf, "NULLIF(%s,%s)", left, right);
      }
      db2free(right,"right");
    }
    db2free(left,"left");
  }
  db2Exit1(": %s", ctx->buf->data);
}

static void deparseBoolExpr          (BoolExpr*          expr, deparse_expr_cxt* ctx) {
  ListCell*         cell = NULL;
  char*             arg  = NULL;
  StringInfoData    buf;

  db2Entry1();
  initStringInfo(&buf);
  arg = deparseExpr (ctx->root, ctx->foreignrel, linitial(expr->args), ctx->params_list);
  if (arg != NULL) {
    bool bBreak = false;
    appendStringInfo (&buf, "(%s%s", expr->boolop == NOT_EXPR ? "NOT " : "", arg);
    for_each_cell(cell, expr->args, lnext(expr->args, list_head(expr->args))) { 
      db2free(arg,"arg");
      arg = deparseExpr (ctx->root, ctx->foreignrel, (Expr*)lfirst(cell), ctx->params_list);
      if (arg != NULL) {
        appendStringInfo (&buf, " %s %s", expr->boolop == AND_EXPR ? "AND":"OR", arg);
      } else {
        bBreak = true;
        break;
      }
    }
    if (!bBreak) {
      appendStringInfo (ctx->buf, "%s)", buf.data);
    }
  }
  db2free(buf.data,"buf.data");
  db2free(arg,"arg");
  db2Exit1(": %s", ctx->buf->data);
}

static void deparseCaseExpr          (CaseExpr*          expr, deparse_expr_cxt* ctx) {
  db2Entry1();
  if (!canHandleType (expr->casetype)) {
    db2Debug2("cannot Handle Type caseexpr->casetype (%d)", expr->casetype);
  } else {
    StringInfoData buf;
    bool           bBreak    = false;
    char*          arg       = NULL;
    ListCell*      cell      = NULL;

    initStringInfo   (&buf);
    appendStringInfo (&buf, "CASE");

    if (expr->arg != NULL) {
      /* for the form "CASE arg WHEN ...", add first expression */
      arg = deparseExpr (ctx->root, ctx->foreignrel, expr->arg, ctx->params_list);
      db2Debug2("CASE %s WHEN ...", arg);
      if (arg == NULL) {
        appendStringInfo (&buf, " %s", arg);
      } else {
        bBreak = true;
      }
    }
    if (!bBreak) {
      /* append WHEN ... THEN clauses */
      foreach (cell, expr->args) {
        CaseWhen* whenclause = (CaseWhen*) lfirst (cell);
        /* WHEN */
        if (expr->arg == NULL) {
          /* for CASE WHEN ..., use the whole expression */
          arg = deparseExpr (ctx->root, ctx->foreignrel, whenclause->expr, ctx->params_list);
        } else {
          /* for CASE arg WHEN ..., use only the right branch of the equality */
          arg = deparseExpr (ctx->root, ctx->foreignrel, lsecond (((OpExpr*) whenclause->expr)->args), ctx->params_list);
        }
        db2Debug2("WHEN %s ", arg);
        if (arg != NULL) {
          appendStringInfo (&buf, " WHEN %s", arg);
        } else {
          bBreak = true;
          break;
        }    /* THEN */
        arg = deparseExpr (ctx->root, ctx->foreignrel, whenclause->result, ctx->params_list);
        db2Debug2(" THEN %s ", arg);
        if (arg != NULL) {
          appendStringInfo (&buf, " THEN %s", arg);
        } else {
          bBreak = true;
          break;
        }
      }
      if (!bBreak) {
        /* append ELSE clause if appropriate */
        if (expr->defresult != NULL) {
          arg = deparseExpr (ctx->root, ctx->foreignrel, expr->defresult, ctx->params_list);
          db2Debug2("  ELSE %s", arg);
          if (arg != NULL) {
            appendStringInfo (&buf, " ELSE %s", arg);
          } else {
            bBreak = true;
          }
        }
        /* append END */
        appendStringInfo (&buf, " END");
      }
    }
    if (!bBreak) {
      appendStringInfo(ctx->buf,"%s",buf.data);
    }
    db2free(buf.data,"buf.data");
  }
  db2Exit1(": %s", ctx->buf->data);
}

static void deparseCoalesceExpr      (CoalesceExpr*      expr, deparse_expr_cxt* ctx) {
  db2Entry1();
  if (!canHandleType (expr->coalescetype)) {
    db2Debug2("cannot Handle Type coalesceexpr->coalescetype (%d)", expr->coalescetype);
  } else {
    StringInfoData result;
    char*          arg       = NULL;
    bool           first_arg = true;
    ListCell*      cell      = NULL;

    initStringInfo   (&result);
    appendStringInfo (&result, "COALESCE(");
    foreach (cell, expr->args) {
      arg = deparseExpr (ctx->root, ctx->foreignrel, (Expr*)lfirst(cell),ctx->params_list);
      db2Debug2("arg: %s", arg);
      if (arg != NULL) {
        appendStringInfo(&result, ((first_arg) ? "%s" : ", %s"), arg);
        first_arg = false;
      } else {
        break;
      }
    }
    if (arg != NULL) {
      appendStringInfo (ctx->buf, "%s)",result.data);
    }
    db2free(result.data,"result.data");
  }
  db2Exit1(": %s", ctx->buf->data);
}

static void deparseFuncExpr          (FuncExpr*          expr, deparse_expr_cxt* ctx) {
  db2Entry1();
  if (!canHandleType (expr->funcresulttype)) {
    db2Debug2("cannot handle funct->funcresulttype: %d",expr->funcresulttype);
  } else if (expr->funcformat == COERCE_IMPLICIT_CAST) {
      /* do nothing for implicit casts */
      db2Debug2("COERCE_IMPLICIT_CAST == expr->funcformat(%d)",expr->funcformat);
      deparseExprInt (linitial(expr->args), ctx);
  } else {
    Oid       schema;
    char*     opername;
    HeapTuple tuple;
    
    /* get function name and schema */
    tuple = SearchSysCache1 (PROCOID, ObjectIdGetDatum (expr->funcid));
    if (!HeapTupleIsValid (tuple)) {
      elog (ERROR, "cache lookup failed for function %u", expr->funcid);
    }
    opername = db2strdup (((Form_pg_proc) GETSTRUCT (tuple))->proname.data,"opername");
    db2Debug2("opername: %s",opername);
    schema = ((Form_pg_proc) GETSTRUCT (tuple))->pronamespace;
    db2Debug2("schema: %d",schema);
    ReleaseSysCache (tuple);
    /* ignore functions in other than the pg_catalog schema */
    if (schema != PG_CATALOG_NAMESPACE) {
      db2Debug2("T_FuncExpr: schema(%d) != PG_CATALOG_NAMESPACE", schema);
    } else {
      /* the "normal" functions that we can translate */
      if (strcmp (opername, "abs")          == 0 || strcmp (opername, "acos")         == 0 || strcmp (opername, "asin")             == 0
      ||  strcmp (opername, "atan")         == 0 || strcmp (opername, "atan2")        == 0 || strcmp (opername, "ceil")             == 0
      ||  strcmp (opername, "ceiling")      == 0 || strcmp (opername, "char_length")  == 0 || strcmp (opername, "character_length") == 0
      ||  strcmp (opername, "concat")       == 0 || strcmp (opername, "cos")          == 0 || strcmp (opername, "exp")              == 0
      ||  strcmp (opername, "initcap")      == 0 || strcmp (opername, "length")       == 0 || strcmp (opername, "lower")            == 0
      ||  strcmp (opername, "lpad")         == 0 || strcmp (opername, "ltrim")        == 0 || strcmp (opername, "mod")              == 0
      ||  strcmp (opername, "octet_length") == 0 || strcmp (opername, "position")     == 0 || strcmp (opername, "pow")              == 0
      ||  strcmp (opername, "power")        == 0 || strcmp (opername, "replace")      == 0 || strcmp (opername, "round")            == 0
      ||  strcmp (opername, "rpad")         == 0 || strcmp (opername, "rtrim")        == 0 || strcmp (opername, "sign")             == 0
      ||  strcmp (opername, "sin")          == 0 || strcmp (opername, "sqrt")         == 0 || strcmp (opername, "strpos")           == 0
      ||  strcmp (opername, "substr")       == 0 || strcmp (opername, "tan")          == 0 || strcmp (opername, "to_char")          == 0
      ||  strcmp (opername, "to_date")      == 0 || strcmp (opername, "to_number")    == 0 || strcmp (opername, "to_timestamp")     == 0
      ||  strcmp (opername, "translate")    == 0 || strcmp (opername, "trunc")        == 0 || strcmp (opername, "upper")            == 0
      || (strcmp (opername, "substring")    == 0 && list_length (expr->args) == 3)) {
        ListCell*      cell;
        char*          arg       = NULL;
        bool           ok        = true;
        bool           first_arg = true;
        StringInfoData buf;

        initStringInfo (&buf);
        if (strcmp (opername, "ceiling") == 0)
          appendStringInfo (&buf, "CEIL(");
        else if (strcmp (opername, "char_length") == 0 || strcmp (opername, "character_length") == 0)
          appendStringInfo (&buf, "LENGTH(");
        else if (strcmp (opername, "pow") == 0)
          appendStringInfo (&buf, "POWER(");
        else if (strcmp (opername, "octet_length") == 0)
          appendStringInfo (&buf, "LENGTHB(");
        else if (strcmp (opername, "position") == 0 || strcmp (opername, "strpos") == 0)
          appendStringInfo (&buf, "INSTR(");
        else if (strcmp (opername, "substring") == 0)
          appendStringInfo (&buf, "SUBSTR(");
        else
          appendStringInfo (&buf, "%s(", opername);
        foreach (cell, expr->args) {
          arg = deparseExpr (ctx->root, ctx->foreignrel, lfirst (cell), ctx->params_list);
          if (arg != NULL) {
            appendStringInfo (&buf, "%s%s", (first_arg) ? ", " : "",arg);
            first_arg = false;
            db2free(arg,"arg");
          } else {
            ok = false;
            db2Debug2("T_FuncExpr: function %s that we cannot render for DB2", opername);
            break;
          }
        }
        appendStringInfo (&buf, ")");
        // copy to return value when successful
        if (ok) {
          appendStringInfo(ctx->buf,"%s",buf.data);
        }
        db2free(buf.data,"buf.data");
      } else if (strcmp (opername, "date_part") == 0) {
        char* left = NULL;

        /* special case: EXTRACT */
        left = deparseExpr (ctx->root, ctx->foreignrel, linitial (expr->args), ctx->params_list);
        if (left == NULL) {
          db2Debug2("T_FuncExpr: function %s that we cannot render for DB2", opername);
        } else {
          /* can only handle these fields in DB2 */
          if (strcmp (left, "'year'")          == 0 || strcmp (left, "'month'")           == 0
          ||  strcmp (left, "'day'")           == 0 || strcmp (left, "'hour'")            == 0
          ||  strcmp (left, "'minute'")        == 0 || strcmp (left, "'second'")          == 0
          ||  strcmp (left, "'timezone_hour'") == 0 || strcmp (left, "'timezone_minute'") == 0) {
            char* right = NULL;

            /* remove final quote */
            left[strlen (left) - 1] = '\0';
            right = deparseExpr (ctx->root, ctx->foreignrel, lsecond (expr->args), ctx->params_list);
            if (right == NULL) {
              db2Debug2("T_FuncExpr: function %s that we cannot render for DB2", opername);
            } else {
              appendStringInfo (ctx->buf, "EXTRACT(%s FROM %s)", left + 1, right);
            }
            db2free(right,"right");
          } else {
            db2Debug2("T_FuncExpr: function %s that we cannot render for DB2", opername);
          }
        }
        db2free (left,"left");
      } else if (strcmp (opername, "now") == 0 || strcmp (opername, "transaction_timestamp") == 0) {
        /* special case: current timestamp */
        appendStringInfo (ctx->buf, "(CAST (?/*:now*/ AS TIMESTAMP))");
      } else {
        /* function that we cannot render for DB2 */
        db2Debug2("T_FuncExpr: function %s that we cannot render for DB2", opername);
      }
    }
    db2free (opername,"opername");
  }
  db2Exit1(": %s", ctx->buf->data);
}

static void deparseCoerceViaIOExpr   (CoerceViaIO*       expr, deparse_expr_cxt* ctx) {
  db2Entry1();
  /* We will only handle casts of 'now'.   */
  /* only casts to these types are handled */
  if (expr->resulttype != DATEOID && expr->resulttype != TIMESTAMPOID && expr->resulttype != TIMESTAMPTZOID) {
    db2Debug2("only casts to DATEOID, TIMESTAMPOID and TIMESTAMPTZOID are handled");
  } else if (expr->arg->type != T_Const) {
  /* the argument must be a Const */
    db2Debug2("T_CoerceViaIO: the argument must be a Const");
  } else {
    Const* constant = (Const *) expr->arg;
    if (constant->constisnull || (constant->consttype != CSTRINGOID && constant->consttype != TEXTOID)) {
    /* the argument must be a not-NULL text constant */
      db2Debug2("T_CoerceViaIO: the argument must be a not-NULL text constant");
    } else {
      /* get the type's output function */
      HeapTuple tuple = SearchSysCache1 (TYPEOID, ObjectIdGetDatum (constant->consttype));
      regproc   typoutput;
      if (!HeapTupleIsValid (tuple)) {
        elog (ERROR, "cache lookup failed for type %u", constant->consttype);
      }
      typoutput = ((Form_pg_type) GETSTRUCT (tuple))->typoutput;
      ReleaseSysCache (tuple);
      /* the value must be "now" */
      if (strcmp (DatumGetCString (OidFunctionCall1 (typoutput, constant->constvalue)), "now") != 0) {
        db2Debug2("value must be 'now'");
      } else {
        switch (expr->resulttype) {
          case DATEOID:
            appendStringInfo(ctx->buf, "TRUNC(CAST (CAST(?/*:now*/ AS TIMESTAMP) AS DATE))");
          break;
          case TIMESTAMPOID:
            appendStringInfo(ctx->buf, "(CAST (CAST (?/*:now*/ AS TIMESTAMP) AS TIMESTAMP))");
          break;
          case TIMESTAMPTZOID:
            appendStringInfo(ctx->buf, "(CAST (?/*:now*/ AS TIMESTAMP))");
          break;
          case TIMEOID:
            appendStringInfo(ctx->buf, "(CAST (CAST (?/*:now*/ AS TIME) AS TIME))");
          break;
          case TIMETZOID:
            appendStringInfo(ctx->buf, "(CAST (?/*:now*/ AS TIME))");
          break;
        }
      }
    }
  }
  db2Exit1(": %s", ctx->buf->data);
}

static void deparseSQLValueFuncExpr  (SQLValueFunction*  expr, deparse_expr_cxt* ctx) {
  db2Entry1();
  switch (expr->op) {
    case SVFOP_CURRENT_DATE:
      appendStringInfo(ctx->buf, "TRUNC(CAST (CAST(?/*:now*/ AS TIMESTAMP) AS DATE))");
    break;
    case SVFOP_CURRENT_TIMESTAMP:
      appendStringInfo(ctx->buf, "(CAST (?/*:now*/ AS TIMESTAMP))");
    break;
    case SVFOP_LOCALTIMESTAMP:
      appendStringInfo(ctx->buf, "(CAST (CAST (?/*:now*/ AS TIMESTAMP) AS TIMESTAMP))");
    break;
    case SVFOP_CURRENT_TIME:
      appendStringInfo(ctx->buf, "(CAST (?/*:now*/ AS TIME))");
    break;
    case SVFOP_LOCALTIME:
      appendStringInfo(ctx->buf, "(CAST (CAST (?/*:now*/ AS TIME) AS TIME))");
    break;
    default:
      /* don't push down other functions */
      db2Debug2("op %d cannot be translated to DB2", expr->op);
    break;
  }
  db2Exit1(": %s", ctx->buf->data);
}

/** db2AggregateInfo
 *   Determine whether the given aggregate (identified by its pg_proc OID and aggkind) is one we know how to
 *   deparse for DB2, and if so return the DB2 SQL keyword to use for it.
 *   *isOrderedSet is set to indicate whether it needs WITHIN GROUP (ORDER BY ...) syntax (ordered-set
 *   aggregates such as PERCENTILE_CONT/PERCENTILE_DISC) rather than plain FUNC(args) syntax.
 *   Returns NULL if the aggregate is not supported for pushdown.
 */
static const char* db2AggregateInfo (Oid aggfnoid, char aggkind, bool* isOrderedSet) {
  HeapTuple    tuple;
  const char*  db2func = NULL;

  db2Entry2();
  *isOrderedSet = false;
  tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(aggfnoid));
  if (!HeapTupleIsValid(tuple)) {
    elog(ERROR, "cache lookup failed for function %u", aggfnoid);
  } else {
    Form_pg_proc procform = (Form_pg_proc) GETSTRUCT(tuple);
    const char*  aggname  = NameStr(procform->proname);

    if (aggkind == AGGKIND_NORMAL) {
      if      (strcmp(aggname, "count") == 0) db2func = "COUNT";
      else if (strcmp(aggname, "sum")   == 0) db2func = "SUM";
      else if (strcmp(aggname, "avg")   == 0) db2func = "AVG";
      else if (strcmp(aggname, "min")   == 0) db2func = "MIN";
      else if (strcmp(aggname, "max")   == 0) db2func = "MAX";
    } else if (aggkind == AGGKIND_ORDERED_SET) {
      if (strcmp(aggname, "percentile_cont") == 0) {
        db2func       = "PERCENTILE_CONT";
        *isOrderedSet = true;
      } else if (strcmp(aggname, "percentile_disc") == 0) {
        db2func       = "PERCENTILE_DISC";
        *isOrderedSet = true;
      }
    }
    db2Debug2("aggref->aggfnoid=%u name=%s aggkind=%c -> %s", aggfnoid, aggname, aggkind, db2func ? db2func : "<unsupported>");
    ReleaseSysCache(tuple);
  }
  db2Exit2(": %s", db2func ? db2func : "<unsupported>");
  return db2func;
}

/** deparseOrderedSetAggref
 *   Deparse an ordered-set aggregate, i.e. PERCENTILE_CONT/PERCENTILE_DISC, using DB2's
 *   FUNC(direct-arg) WITHIN GROUP (ORDER BY sort-key [ASC|DESC]) syntax.
 *   Caller (foreign_expr_walker) has already verified there is exactly one direct argument and exactly
 *   one ORDER BY column.
 */
static void deparseOrderedSetAggref (Aggref* expr, const char* db2func, deparse_expr_cxt* ctx) {
  StringInfoData    directArgBuf;
  StringInfoData    sortBuf;
  deparse_expr_cxt  context;
  SortGroupClause*  srt;
  TargetEntry*      tle;
  TypeCacheEntry*   typentry;

  db2Entry1();

  context.root        = ctx->root;
  context.foreignrel   = ctx->foreignrel;
  context.scanrel      = ctx->scanrel;
  context.params_list  = ctx->params_list;

  /* Deparse the single direct argument (e.g. the fraction for PERCENTILE_CONT/PERCENTILE_DISC). */
  initStringInfo(&directArgBuf);
  context.buf = &directArgBuf;
  deparseExprInt((Expr*) linitial(expr->aggdirectargs), &context);

  /* Deparse the single ORDER BY column, along with its sort direction. */
  srt      = (SortGroupClause*) linitial(expr->aggorder);
  tle      = get_sortgroupref_tle(srt->tleSortGroupRef, expr->args);
  typentry = lookup_type_cache(exprType((Node*) tle->expr), TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);

  initStringInfo(&sortBuf);
  context.buf = &sortBuf;
  deparseExprInt(tle->expr, &context);
  appendStringInfoString(&sortBuf, (srt->sortop == typentry->gt_opr) ? " DESC" : " ASC");

  if (directArgBuf.len > 0 && sortBuf.len > 0) {
    appendStringInfo(ctx->buf, "%s(%s) WITHIN GROUP (ORDER BY %s)", db2func, directArgBuf.data, sortBuf.data);
  } else {
    db2Debug2("could not deparse ordered-set aggregate args");
  }
  db2free(directArgBuf.data, "directArgBuf.data");
  db2free(sortBuf.data, "sortBuf.data");

  db2Exit1(": %s", ctx->buf->data);
}

static void deparseAggref            (Aggref*            expr, deparse_expr_cxt* ctx) {
  db2Entry1();
  if (expr == NULL) {
    db2Debug2("expr is NULL");
  } else {
    bool         isOrderedSet = false;
    const char*  db2func      = db2AggregateInfo(expr->aggfnoid, expr->aggkind, &isOrderedSet);

    if (db2func == NULL) {
      /* Unknown/unsupported aggregate: don't emit SQL. (foreign_expr_walker should already have refused to push this down.) */
      db2Debug2("aggregate (fnoid=%u) not supported for DB2 deparse", expr->aggfnoid);
    } else if (isOrderedSet) {
      deparseOrderedSetAggref(expr, db2func, ctx);
    } else {
      StringInfoData    result;
      bool              distinct = (expr->aggdistinct != NIL);
      bool              ok = true;

      initStringInfo(&result);
      appendStringInfo(&result, "%s(", db2func);
      if (distinct) {
        appendStringInfoString(&result, "DISTINCT ");
      }
      if (expr->aggstar) {
        /* COUNT(*) */
        appendStringInfoString(&result, "*");
      } else {
        ListCell*         lc;
        bool              first_arg = true;

        foreach (lc, expr->args) {
          Node*             argnode = (Node*) lfirst(lc);
          Expr*             argexpr = NULL;
          StringInfoData    cbuf;
          deparse_expr_cxt  context;

          initStringInfo(&cbuf);
          context.root        = ctx->root;
          context.buf         = &cbuf;
          context.foreignrel  = ctx->foreignrel;
          context.scanrel     = ctx->scanrel;
          context.params_list = ctx->params_list;

          if (argnode == NULL) {
            ok = false;
            break;
          }
          if (argnode->type == T_TargetEntry) {
            argexpr = ((TargetEntry*) argnode)->expr;
          } else {
            argexpr = (Expr*) argnode;
          }
          deparseExprInt(argexpr, &context);
          if (cbuf.len <= 0) {
            ok = false;
            break;
          }
          appendStringInfo(&result, "%s%s", cbuf.data, first_arg ? "" : ", ");
          db2free(cbuf.data,"cbuf.data");
          first_arg = false;
        }
      }
      if (ok) {
        appendStringInfo(ctx->buf, "%s)",result.data);
      } else {
        db2Debug2("parsed aggref so far: %s", result.data);
        db2Debug2("could not deparse aggregate args");
      }
      db2free(result.data,"result.data");
    }
  }
  db2Exit1(": %s", ctx->buf->data);
}

/** datumToString
 *   Convert a Datum to a string by calling the type output function.
 *   Returns the result or NULL if it cannot be converted to DB2 SQL.
 */
static char* datumToString (Datum datum, Oid type) {
  StringInfoData result;
  regproc        typoutput;
  HeapTuple      tuple;
  char*          str;
  char*          p;
  db2Entry1();
  /* get the type's output function */
  tuple = SearchSysCache1 (TYPEOID, ObjectIdGetDatum (type));
  if (!HeapTupleIsValid (tuple)) {
    elog (ERROR, "cache lookup failed for type %u", type);
  }
  typoutput = ((Form_pg_type) GETSTRUCT (tuple))->typoutput;
  ReleaseSysCache (tuple);

  /* render the constant in DB2 SQL */
  switch (type) {
    case TEXTOID:
    case CHAROID:
    case BPCHAROID:
    case VARCHAROID:
    case NAMEOID:
      str = DatumGetCString (OidFunctionCall1 (typoutput, datum));
      /*
       * Don't try to convert empty strings to DB2.
       * DB2 treats empty strings as NULL.
       */
      if (str[0] == '\0')
        return NULL;

      /* quote string */
      initStringInfo (&result);
      appendStringInfo (&result, "'");
      for (p = str; *p; ++p) {
        if (*p == '\'')
          appendStringInfo (&result, "'");
        appendStringInfo (&result, "%c", *p);
      }
      appendStringInfo (&result, "'");
    break;
    case INT8OID:
    case INT2OID:
    case INT4OID:
    case OIDOID:
    case FLOAT4OID:
    case FLOAT8OID:
    case NUMERICOID:
      str = DatumGetCString (OidFunctionCall1 (typoutput, datum));
      initStringInfo (&result);
      appendStringInfo (&result, "%s", str);
    break;
    case DATEOID:
      str = deparseDate (datum);
      initStringInfo (&result);
      appendStringInfo (&result, "(CAST ('%s' AS DATE))", str);
    break;
    case TIMESTAMPOID:
      str = deparseTimestamp (datum, false);
      initStringInfo (&result);
      appendStringInfo (&result, "(CAST ('%s' AS TIMESTAMP))", str);
    break;
    case TIMESTAMPTZOID:
      str = deparseTimestamp (datum, false);
      initStringInfo (&result);
      appendStringInfo (&result, "(CAST ('%s' AS TIMESTAMP))", str);
    break;
    case TIMEOID:
      str = deparseTimestamp (datum, false);
      initStringInfo (&result);
      appendStringInfo (&result, "(CAST ('%s' AS TIME))", str);
    break;
    case TIMETZOID:
      str = deparseTimestamp (datum, false);
      initStringInfo (&result);
      appendStringInfo (&result, "(CAST ('%s' AS TIME))", str);
    break;
    case INTERVALOID:
      str = deparseInterval (datum);
      if (str == NULL)
        return NULL;
      initStringInfo (&result);
      appendStringInfo (&result, "%s", str);
    break;
    default:
      return NULL;
  }
  db2Exit1(": %s", result.data);
  return result.data;
}

/** deparseDate
 *   Render a PostgreSQL date so that DB2 can parse it.
 */
char* deparseDate (Datum datum) {
  struct pg_tm   datetime_tm;
  StringInfoData s;
  db2Entry1();
  if (DATE_NOT_FINITE (DatumGetDateADT (datum)))
    ereport (ERROR, (errcode (ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE), errmsg ("infinite date value cannot be stored in DB2")));

  /* get the parts */
  (void) j2date (DatumGetDateADT (datum) + POSTGRES_EPOCH_JDATE, &(datetime_tm.tm_year), &(datetime_tm.tm_mon), &(datetime_tm.tm_mday));

  if (datetime_tm.tm_year < 0)
    ereport (ERROR, (errcode (ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE), errmsg ("BC date value cannot be stored in DB2")));

  initStringInfo (&s);
  appendStringInfo (&s, "%04d-%02d-%02d 00:00:00", datetime_tm.tm_year > 0 ? datetime_tm.tm_year : -datetime_tm.tm_year + 1, datetime_tm.tm_mon, datetime_tm.tm_mday);
  db2Exit1(": %s", s.data);
  return s.data;
}

/** deparseTimestamp
 *   Render a PostgreSQL timestamp so that DB2 can parse it.
 */
char* deparseTimestamp (Datum datum, bool hasTimezone) {
  struct pg_tm   datetime_tm;
  int32          tzoffset;
  fsec_t         datetime_fsec;
  StringInfoData s;
  db2Entry1();
  /* this is sloppy, but DatumGetTimestampTz and DatumGetTimestamp are the same */
  if (TIMESTAMP_NOT_FINITE (DatumGetTimestampTz (datum)))
    ereport (ERROR, (errcode (ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE), errmsg ("infinite timestamp value cannot be stored in DB2")));

  /* get the parts */
  tzoffset = 0;
  (void) timestamp2tm (DatumGetTimestampTz (datum), hasTimezone ? &tzoffset : NULL, &datetime_tm, &datetime_fsec, NULL, NULL);

  if (datetime_tm.tm_year < 0)
    ereport (ERROR, (errcode (ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE), errmsg ("BC date value cannot be stored in DB2")));

  initStringInfo (&s);
  if (hasTimezone)
    appendStringInfo (&s, "%04d-%02d-%02d %02d:%02d:%02d.%06d%+03d:%02d",
      datetime_tm.tm_year > 0 ? datetime_tm.tm_year : -datetime_tm.tm_year + 1,
      datetime_tm.tm_mon, datetime_tm.tm_mday, datetime_tm.tm_hour,
      datetime_tm.tm_min, datetime_tm.tm_sec, (int32) datetime_fsec,
      -tzoffset / 3600, ((tzoffset > 0) ? tzoffset % 3600 : -tzoffset % 3600) / 60);
  else
    appendStringInfo (&s, "%04d-%02d-%02d %02d:%02d:%02d.%06d",
      datetime_tm.tm_year > 0 ? datetime_tm.tm_year : -datetime_tm.tm_year + 1,
      datetime_tm.tm_mon, datetime_tm.tm_mday, datetime_tm.tm_hour,
      datetime_tm.tm_min, datetime_tm.tm_sec, (int32) datetime_fsec);
  db2Exit1(": '%s'", s.data);
  return s.data;
}

/* Deparse given constant value into context->buf.
 * This function has to be kept in sync with ruleutils.c's get_const_expr.
 *
 * As in that function, showtype can be -1 to never show "::typename" decoration, +1 to always show it, or 0 to show it only if the constant
 * wouldn't be assumed to be the right type by default.
 *
 * In addition, this code allows showtype to be -2 to indicate that we should not show "::typename" decoration if the constant is printed as
 * an untyped literal or NULL (while in other cases, behaving as for showtype == 0).
 */
static void deparseConst(Const *node, deparse_expr_cxt *context, int showtype) {
  StringInfo  buf           = context->buf;
  Oid         typoutput;
  bool        typIsVarlena;
  char*       extval;
  bool        isfloat       = false;
  bool        isstring      = false;
  bool        needlabel;

  db2Entry1();
  if (node->constisnull) {
    appendStringInfoString(buf, "NULL");
    if (showtype >= 0)
      appendStringInfo(buf, "::%s", deparse_type_name(node->consttype, node->consttypmod));
    return;
  }

  getTypeOutputInfo(node->consttype, &typoutput, &typIsVarlena);
  extval = OidOutputFunctionCall(typoutput, node->constvalue);

  switch (node->consttype) {
    case INT2OID:
    case INT4OID:
    case INT8OID:
    case OIDOID:
    case FLOAT4OID:
    case FLOAT8OID:
    case NUMERICOID: {
      /* No need to quote unless it's a special value such as 'NaN'.
       * See comments in get_const_expr().
       */
      if (strspn(extval, "0123456789+-eE.") == strlen(extval)) {
        if (extval[0] == '+' || extval[0] == '-')
          appendStringInfo(buf, "(%s)", extval);
        else
          appendStringInfoString(buf, extval);
        if (strcspn(extval, "eE.") != strlen(extval))
          isfloat = true; /* it looks like a float */
      } else {
        appendStringInfo(buf, "'%s'", extval);
      }
    }
    break;
    case BITOID:
    case VARBITOID:
      appendStringInfo(buf, "B'%s'", extval);
    break;
    case BOOLOID:
      if (strcmp(extval, "t") == 0)
        appendStringInfoString(buf, "true");
      else
        appendStringInfoString(buf, "false");
    break;
    default:
      deparseStringLiteral(buf, extval);
      isstring = true;
    break;
  }
  pfree(extval);
  if (showtype == -1) {
    db2Exit1();
    return;           /* never print type label */
  }
  /* For showtype == 0, append ::typename unless the constant will be implicitly typed as the right type when it is read in.
   * XXX this code has to be kept in sync with the behavior of the parser, especially make_const.
   */
  switch (node->consttype) {
    case BOOLOID:
    case INT4OID:
    case UNKNOWNOID:
      needlabel = false;
    break;
    case NUMERICOID:
      needlabel = !isfloat || (node->consttypmod >= 0);
    break;
    default:
      if (showtype == -2) {
        /* label unless we printed it as an untyped string */
        needlabel = !isstring;
      } else {
        needlabel = true;
      }
    break;
  }
  if (needlabel || showtype > 0)
    appendStringInfo(buf, "::%s", deparse_type_name(node->consttype, node->consttypmod));
  db2Exit1();
}

/** Print the name of an operator.
 */
static void deparseOperatorName(StringInfo buf, Form_pg_operator opform) {
  char* opname = NULL;

  db2Entry1();
  /* opname is not a SQL identifier, so we should not quote it. */
  opname = NameStr(opform->oprname);

  /* Print schema name only if it's not pg_catalog */
  if (opform->oprnamespace != PG_CATALOG_NAMESPACE) {
    const char *opnspname;

    opnspname = get_namespace_name(opform->oprnamespace);
    /* Print fully qualified operator name. */
    appendStringInfo(buf, "OPERATOR(%s.%s)", quote_identifier(opnspname), opname);
  } else {
    /* Just print operator name. */
    appendStringInfoString(buf, opname);
  }
  db2Exit1();
}

/** deparsedeparseInterval
 *   Render a PostgreSQL timestamp so that DB2 can parse it.
 */
static char* deparseInterval (Datum datum) {
  #if PG_VERSION_NUM >= 150000
  struct pg_itm tm;
  #else
  struct pg_tm tm;
  #endif
  fsec_t         fsec=0;
  StringInfoData s;
  char*          sign;
  int            idx = 0;

  db2Entry1();
  #if PG_VERSION_NUM >= 150000
  interval2itm (*DatumGetIntervalP (datum), &tm);
  #else
  if (interval2tm (*DatumGetIntervalP (datum), &tm, &fsec) != 0) {
    elog (ERROR, "could not convert interval to tm");
  }
  #endif
  /* only translate intervals that can be translated to INTERVAL DAY TO SECOND */
//  if (tm.tm_year != 0 || tm.tm_mon != 0)
//    return NULL;

  /* DB2 intervals have only one sign */
  if (tm.tm_mday < 0 || tm.tm_hour < 0 || tm.tm_min < 0 || tm.tm_sec < 0 || fsec < 0) {
    sign = "-";
    /* all signs must match */
    if (tm.tm_mday > 0 || tm.tm_hour > 0 || tm.tm_min > 0 || tm.tm_sec > 0 || fsec > 0)
      return NULL;
    tm.tm_mday = -tm.tm_mday;
    tm.tm_hour = -tm.tm_hour;
    tm.tm_min  = -tm.tm_min;
    tm.tm_sec  = -tm.tm_sec;
    fsec       = -fsec;
  } else {
    sign = "+";
  }
  initStringInfo (&s);
  if (tm.tm_year > 0) {
    appendStringInfo(&s, ((tm.tm_year > 1) ? "%d YEARS" : "%d YEAR"),tm.tm_year);
  }
  idx += tm.tm_year;
  if (tm.tm_mon > 0) {
    appendStringInfo(&s," %s ",(idx > 0 ) ? sign : "");
    appendStringInfo(&s, ((tm.tm_mon > 1) ? "%d MONTHS" : "%d MONTH"),tm.tm_mon);
  }
  idx += tm.tm_mon;
  if (tm.tm_mday > 0) {
    appendStringInfo(&s," %s ",(idx > 0 ) ? sign : "");
    appendStringInfo(&s, ((tm.tm_mday > 1) ? "%d DAYS" : "%d DAY"),tm.tm_mday);
  }
  idx += tm.tm_mday;
  if (tm.tm_hour > 0) {
    appendStringInfo(&s," %s ",(idx > 0 ) ? sign : "");
    #if PG_VERSION_NUM >= 150000
    appendStringInfo(&s, ((tm.tm_hour > 1) ? "%ld HOURS" : "%ld HOUR"),tm.tm_hour);
    #else
    appendStringInfo(&s, ((tm.tm_hour > 1) ? "%d HOURS" : "%d HOUR"),tm.tm_hour);
    #endif
  }
  idx += tm.tm_hour;
  if (tm.tm_min > 0) {
    appendStringInfo(&s," %s ",(idx > 0 ) ? sign : "");
    appendStringInfo(&s, ((tm.tm_min > 1) ? "%d MINUTES" : "%d MINUTE"),tm.tm_min);
  }
  idx += tm.tm_min;
  if (tm.tm_sec > 0) {
    appendStringInfo(&s," %s ",(idx > 0 ) ? sign : "");
    appendStringInfo(&s, ((tm.tm_sec > 1) ? "%d SECONDS" : "%d SECOND"),tm.tm_sec);
  }
  idx += tm.tm_sec;

//  #if PG_VERSION_NUM >= 150000
//  appendStringInfo (&s, "INTERVAL '%s%d %02ld:%02d:%02d.%06d' DAY TO SECOND", sign, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, fsec);
//  #else
//  appendStringInfo (&s, "INTERVAL '%s%d %02d:%02d:%02d.%06d' DAY(9) TO SECOND(6)", sign, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, fsec);
//  #endif
  db2Exit1(": '%s'",s.data);
  return s.data;
}

/** Deparse the appropriate locking clause (FOR UPDATE or FOR SHARE) for a given relation (context->scanrel).
 */
static void deparseLockingClause(deparse_expr_cxt *context) {
  StringInfo    buf     = context->buf;
  PlannerInfo*  root    = context->root;
  RelOptInfo*   rel     = context->scanrel;
  DB2FdwState*  fpinfo  = (DB2FdwState*) rel->fdw_private;
  int           relid   = -1;

  db2Entry1();
  while ((relid = bms_next_member(rel->relids, relid)) >= 0) {
    /* Ignore relation if it appears in a lower subquery.  Locking clause for such a relation is included in the subquery if necessary. */
    if (bms_is_member(relid, fpinfo->lower_subquery_rels))
      continue;

    /* Add FOR UPDATE/SHARE if appropriate.
     * We apply locking during the initial row fetch, rather than later on as is done for local tables. 
     * The extra roundtrips involved in trying to duplicate the local semantics exactly don't seem worthwhile
     * (see also comments for RowMarkType).
     *
     * Note: because we actually run the query as a cursor, this assumes that DECLARE CURSOR ... FOR UPDATE is supported, which it isn't before 8.3.
     */
    #if PG_VERSION_NUM < 140000
    if (relid == root->parse->resultRelation && (root->parse->commandType == CMD_UPDATE || root->parse->commandType == CMD_DELETE)) {
    #else
    if (bms_is_member(relid, root->all_result_relids) && (root->parse->commandType == CMD_UPDATE || root->parse->commandType == CMD_DELETE)) {
    #endif
      /* Relation is UPDATE/DELETE target, so use FOR UPDATE */
      appendStringInfoString(buf, " FOR UPDATE");

      /* Add the relation alias if we are here for a join relation */
      if (IS_JOIN_REL(rel))
        appendStringInfo(buf, " OF %s%d", REL_ALIAS_PREFIX, relid);
    } else {
      PlanRowMark *rc = get_plan_rowmark(root->rowMarks, relid);
      if (rc) {
        /* Relation is specified as a FOR UPDATE/SHARE target, so handle that.
         * (But we could also see LCS_NONE, meaning this isn't a target relation after all.)
         *
         * For now, just ignore any [NO] KEY specification, 
         * since (a) it's not clear what that means for a remote table that we don't have complete information about,
         * and (b) it wouldn't work anyway on older remote servers.
         * Likewise, we don't worry about NOWAIT.
         */
        switch (rc->strength) {
          case LCS_NONE:
            /* No locking needed */
          break;
          case LCS_FORKEYSHARE:
          case LCS_FORSHARE:
            appendStringInfoString(buf, " FOR SHARE");
          break;
          case LCS_FORNOKEYUPDATE:
          case LCS_FORUPDATE:
            appendStringInfoString(buf, " FOR UPDATE");
          break;
        }
        /* Add the relation alias if we are here for a join relation */
        if (bms_membership(rel->relids) == BMS_MULTIPLE && rc->strength != LCS_NONE)
          appendStringInfo(buf, " OF %s%d", REL_ALIAS_PREFIX, relid);
      }
    }
  }
  db2Exit1();
}

/** Output join name for given join type 
 */
char* get_jointype_name (JoinType jointype) {
  char* type = NULL;
  db2Entry1();
  switch (jointype) {
    case JOIN_INNER:
      type = "INNER";
    break;
    case JOIN_LEFT:
      type = "LEFT";
    break;
    case JOIN_RIGHT:
      type = "RIGHT";
    break;
    case JOIN_FULL:
      type= "FULL";
    break;
    default:
      /* Shouldn't come here, but protect from buggy code. */
      elog (ERROR, "unsupported join type %d", jointype);
    break;
  }
  db2Exit1(": %s", type);
  return type;
}

/** Emit a target list that retrieves the columns specified in attrs_used.
 * This is used for both SELECT and RETURNING targetlists; the is_returning parameter is true only for a RETURNING targetlist.
 * The tlist text is appended to buf, and we also create an integer List of the columns being retrieved, which is returned to *retrieved_attrs.
 * If qualify_col is true, add relation alias before the column name.
 */
static void deparseTargetList(StringInfo buf, RangeTblEntry *rte, Index rtindex, Relation rel, bool is_returning, Bitmapset *attrs_used, bool qualify_col, List **retrieved_attrs) {
  TupleDesc tupdesc = RelationGetDescr(rel);
  bool      have_wholerow;
  bool      first;
  int       i;

  db2Entry1();
  *retrieved_attrs = NIL;

  /* If there's a whole-row reference, we'll need all the columns. */
  have_wholerow = bms_is_member(0 - FirstLowInvalidHeapAttributeNumber, attrs_used);

  first = true;
  for (i = 1; i <= tupdesc->natts; i++) {
    #if PG_VERISON_NUM < 180000
    Form_pg_attribute attr = TupleDescAttr(tupdesc, i - 1);

    /* Ignore dropped attributes. */
    if (attr->attisdropped)
      continue;
    #else
    /* Ignore dropped attributes. */
    if (TupleDescCompactAttr(tupdesc, i - 1)->attisdropped)
      continue;
    #endif

    if (have_wholerow || bms_is_member(i - FirstLowInvalidHeapAttributeNumber, attrs_used)) {
      if (!first)
        appendStringInfoString(buf, ", ");
      else if (is_returning)
        appendStringInfoString(buf, " RETURNING ");
      first = false;
      deparseColumnRef(buf, rtindex, i, rte, qualify_col);
      *retrieved_attrs = lappend_int(*retrieved_attrs, i);
    }
  }

  /* Add ctid if needed.  We currently don't support retrieving any other system columns. */
  if (bms_is_member(SelfItemPointerAttributeNumber - FirstLowInvalidHeapAttributeNumber, attrs_used)) {
    if (!first)
      appendStringInfoString(buf, ", ");
    else if (is_returning)
      appendStringInfoString(buf, " RETURNING ");
    first = false;
    if (qualify_col) {
      ADD_REL_QUALIFIER(buf, rtindex);
    }
    appendStringInfoString(buf, "ctid");
    *retrieved_attrs = lappend_int(*retrieved_attrs, SelfItemPointerAttributeNumber);
  }
  /* Don't generate bad syntax if no undropped columns */
  if (first && !is_returning)
    appendStringInfoString(buf, "NULL");
  db2Exit1(": %s", buf->data);
}

/** Deparse given targetlist and append it to context->buf.
 *
 * tlist is list of TargetEntry's which in turn contain Var nodes.
 *
 * retrieved_attrs is the list of continuously increasing integers starting from 1. It has same number of entries as tlist.
 *
 * This is used for both SELECT and RETURNING targetlists; the is_returning parameter is true only for a RETURNING targetlist.
 */
static void deparseExplicitTargetList(List* tlist, bool is_returning, List** retrieved_attrs, deparse_expr_cxt* context) {
  ListCell*   lc  = NULL;
  StringInfo  buf = context->buf;
  int         i   = 0;

  db2Entry1();
  *retrieved_attrs = NIL;

  foreach(lc, tlist) {
    TargetEntry *tle = lfirst_node(TargetEntry, lc);

    if (i > 0)
      appendStringInfoString(buf, ", ");
    else if (is_returning)
      appendStringInfoString(buf, " RETURNING ");

    deparseExprInt((Expr*) tle->expr, context);

    *retrieved_attrs = lappend_int(*retrieved_attrs, i + 1);
    i++;
  }

  if (i == 0 && !is_returning)
    appendStringInfoString(buf, "NULL");
  db2Exit1(": %s", buf->data);
}

/* Emit expressions specified in the given relation's reltarget.
 *
 * This is used for deparsing the given relation as a subquery.
 */
static void deparseSubqueryTargetList(deparse_expr_cxt* context) {
  bool        first = true;
  ListCell*   lc;

  db2Entry1();
  /* Should only be called in these cases. */
  Assert(IS_SIMPLE_REL(context->foreignrel) || IS_JOIN_REL(context->foreignrel));
  foreach(lc, context->foreignrel->reltarget->exprs) {
    if (!first)
      appendStringInfoString(context->buf, ", ");
    first = false;
    deparseExprInt((Expr*)lfirst(lc), context);
  }

  /* Don't generate bad syntax if no expressions */
  if (first)
    appendStringInfoString(context->buf, "NULL");
  db2Exit1(": %s", context->buf->data);
}

/* Given an EquivalenceClass and a foreign relation, find an EC member that can be used to sort the relation remotely according to a pathkey using this EC.
 *
 * If there is more than one suitable candidate, return an arbitrary one of them.  If there is none, return NULL.
 *
 * This checks that the EC member expression uses only Vars from the given rel and is shippable.  Caller must separately verify that the pathkey's
 * ordering operator is shippable.
 */
EquivalenceMember* find_em_for_rel(PlannerInfo* root, EquivalenceClass* ec, RelOptInfo* rel) {
  DB2FdwState*              fpinfo  = (DB2FdwState*) rel->fdw_private;
  EquivalenceMember*        em      = NULL;
  #if PG_VERSION_NUM < 180000
  ListCell*                 lc      = NULL;
  #else
  EquivalenceMemberIterator it;
  #endif

  db2Entry1();

  #if PG_VERSION_NUM < 180000
  foreach(lc, ec->ec_members) {
    em = (EquivalenceMember *) lfirst(lc);
  #else
  setup_eclass_member_iterator(&it, ec, rel->relids);
  while ((em = eclass_member_iterator_next(&it)) != NULL) {
  #endif
    /* Note we require !bms_is_empty, else we'd accept constant expressions which are not suitable for the purpose. */
    if (bms_is_subset(em->em_relids, rel->relids) 
    &&  !bms_is_empty(em->em_relids) 
    &&  bms_is_empty(bms_intersect(em->em_relids, fpinfo->hidden_subquery_rels)) 
    &&  is_foreign_expr(root, rel, em->em_expr))
    break;
  }
  db2Exit1(": %x",em);
  return em;
}

/* Find an EquivalenceClass member that is to be computed as a sort column in the given rel's reltarget, and is shippable.
 *
 * If there is more than one suitable candidate, return an arbitrary one of them.  If there is none, return NULL.
 *
 * This checks that the EC member expression uses only Vars from the given rel and is shippable.  Caller must separately verify that the pathkey's
 * ordering operator is shippable.
 */
EquivalenceMember* find_em_for_rel_target(PlannerInfo* root, EquivalenceClass* ec, RelOptInfo* rel) {
  PathTarget* target = rel->reltarget;
  ListCell*   lc1;
  int         i = 0;

  db2Entry1();
  foreach(lc1, target->exprs) {
    Expr*       expr  = (Expr *) lfirst(lc1);
    Index       sgref = get_pathtarget_sortgroupref(target, i);
    ListCell*   lc2   = NULL;

    /* Ignore non-sort expressions */
    if (sgref == 0 || get_sortgroupref_clause_noerr(sgref, root->parse->sortClause) == NULL) {
      i++;
      continue;
    }

    /* We ignore binary-compatible relabeling on both ends */
    while (expr && IsA(expr, RelabelType))
      expr = ((RelabelType*) expr)->arg;

    /* Locate an EquivalenceClass member matching this expr, if any.
     * Ignore child members.
     */
    foreach(lc2, ec->ec_members) {
      EquivalenceMember*  em      = (EquivalenceMember*) lfirst(lc2);
      Expr*               em_expr = NULL;

      /* Don't match constants */
      if (em->em_is_const)
        continue;

      /* Child members should not exist in ec_members */
      Assert(!em->em_is_child);

      /* Match if same expression (after stripping relabel) */
      em_expr = em->em_expr;
      while (em_expr && IsA(em_expr, RelabelType))
        em_expr = ((RelabelType *) em_expr)->arg;

      if (!equal(em_expr, expr))
        continue;

      /* Check that expression (including relabels!) is shippable */
      if (is_foreign_expr(root, rel, em->em_expr)) {
        db2Exit1(": %x", em);
        return em;
      }
    }
    i++;
  }
  db2Exit1(": %x", NULL);
  return NULL;
}

/* deparse remote UPDATE statement
 *
 * 'buf' is the output buffer to append the statement to 'rtindex' is the RT index of the associated target relation
 * 'rel' is the relation descriptor for the target relation
 * 'foreignrel' is the RelOptInfo for the target relation or the join relation containing all base relations in the query
 * 'targetlist' is the tlist of the underlying foreign-scan plan node (note that this only contains new-value expressions and junk attrs)
 * 'targetAttrs' is the target columns of the UPDATE
 * 'remote_conds' is the qual clauses that must be evaluated remotely
 * '*params_list' is an output list of exprs that will become remote Params
 * 'returningList' is the RETURNING targetlist
 * '*retrieved_attrs' is an output list of integers of columns being retrieved 	by RETURNING (if any)
 */
void deparseDirectUpdateSql(StringInfo buf, PlannerInfo *root, Index rtindex, Relation rel, RelOptInfo *foreignrel, List *targetlist, List *targetAttrs, List *remote_conds, List **params_list, List *returningList, List **retrieved_attrs) {
  deparse_expr_cxt  context;
  int               nestlevel         = 0;
  bool              first             = true;
  RangeTblEntry*    rte               = planner_rt_fetch(rtindex, root);
  ListCell*         lc                = NULL;
  ListCell*         lc2               = NULL;
  List*             additional_conds  = NIL;

  db2Entry1();
  /* Set up context struct for recursion */
  context.root        = root;
  context.foreignrel  = foreignrel;
  context.scanrel     = foreignrel;
  context.buf         = buf;
  context.params_list = params_list;

  appendStringInfoString(buf, "UPDATE ");
  deparseRelation(buf, rel);
  if (foreignrel->reloptkind == RELOPT_JOINREL)
    appendStringInfo(buf, " %s%d", REL_ALIAS_PREFIX, rtindex);
  appendStringInfoString(buf, " SET ");

  /* Make sure any constants in the exprs are printed portably */
  nestlevel = set_transmission_modes();

  first = true;
  forboth(lc, targetlist, lc2, targetAttrs)  {
    TargetEntry*  tle     = lfirst_node(TargetEntry, lc);
    int           attnum  = lfirst_int(lc2);

    /* update's new-value expressions shouldn't be resjunk */
    Assert(!tle->resjunk);

    if (!first)
      appendStringInfoString(buf, ", ");
    first = false;

    deparseColumnRef(buf, rtindex, attnum, rte, false);
    appendStringInfoString(buf, " = ");
    deparseExprInt((Expr*) tle->expr, &context);
  }

  reset_transmission_modes(nestlevel);

  if (foreignrel->reloptkind == RELOPT_JOINREL) {
    List* ignore_conds = NIL;

    appendStringInfoString(buf, " FROM ");
    deparseFromExprForRel(buf, root, foreignrel, true, rtindex, &ignore_conds, &additional_conds, params_list);
    remote_conds = list_concat(remote_conds, ignore_conds);
  }

  appendWhereClause(remote_conds, additional_conds, &context);

  if (additional_conds != NIL)
    list_free_deep(additional_conds);

  if (foreignrel->reloptkind == RELOPT_JOINREL)
    deparseExplicitTargetList(returningList, true, retrieved_attrs, &context);
  else
    deparseReturningList(buf, rte, rtindex, rel, false, NIL, returningList, retrieved_attrs);
  db2Exit1(": %s",buf->data);
}

/*
 * Add a RETURNING clause, if needed, to an INSERT/UPDATE/DELETE.
 */
static void deparseReturningList(StringInfo buf, RangeTblEntry *rte, Index rtindex, Relation rel, bool trig_after_row, List *withCheckOptionList, List *returningList, List **retrieved_attrs) {
  Bitmapset  *attrs_used = NULL;

  db2Entry1();
  if (trig_after_row) {
    /* whole-row reference acquires all non-system columns */
    attrs_used = bms_make_singleton(0 - FirstLowInvalidHeapAttributeNumber);
  }

  if (withCheckOptionList != NIL) {
    /* We need the attrs, non-system and system, mentioned in the local query's WITH CHECK OPTION list.
     *
     * Note: we do this to ensure that WCO constraints will be evaluated on the data actually inserted/updated on the remote side, which
     * might differ from the data supplied by the core code, for example as a result of remote triggers.
     */
    pull_varattnos((Node *) withCheckOptionList, rtindex, &attrs_used);
  }

  if (returningList != NIL) {
    /* We need the attrs, non-system and system, mentioned in the local query's RETURNING list. */
    pull_varattnos((Node *) returningList, rtindex, &attrs_used);
  }

  if (attrs_used != NULL)
    deparseTargetList(buf, rte, rtindex, rel, true, attrs_used, false, retrieved_attrs);
  else
    *retrieved_attrs = NIL;
  db2Exit1(": %s", buf->data);
}

/* deparse remote DELETE statement
 * The statement text is appended to buf, and we also create an integer List of the columns being retrieved by RETURNING (if any), which is returned to *retrieved_attrs.
 */
void deparseDeleteSql(StringInfo buf, RangeTblEntry *rte, Index rtindex, Relation rel, List *returningList, List **retrieved_attrs) {
  db2Entry1();

  appendStringInfoString(buf, "DELETE FROM ");
  deparseRelation(buf, rel);
  appendStringInfoString(buf, " WHERE ctid = $1");
  deparseReturningList(buf, rte, rtindex, rel, rel->trigdesc && rel->trigdesc->trig_delete_after_row, NIL, returningList, retrieved_attrs);

  db2Exit1(": %s", buf->data);
}

/* deparse remote DELETE statement
 *
 * 'buf' is the output buffer to append the statement to  'rtindex' is the RT index of the associated target relation
 * 'rel' is the relation descriptor for the target relation
 * 'foreignrel' is the RelOptInfo for the target relation or the join relation containing all base relations in the query
 * 'remote_conds' is the qual clauses that must be evaluated remotely
 * '*params_list' is an output list of exprs that will become remote Params
 * 'returningList' is the RETURNING targetlist
 * '*retrieved_attrs' is an output list of integers of columns being retrieved by RETURNING (if any)
 */
void deparseDirectDeleteSql(StringInfo buf, PlannerInfo *root, Index rtindex, Relation rel, RelOptInfo *foreignrel, List *remote_conds, List **params_list, List *returningList, List **retrieved_attrs) {
  deparse_expr_cxt context;
  List	   *additional_conds = NIL;

  db2Entry1();
  /* Set up context struct for recursion */
  context.root = root;
  context.foreignrel = foreignrel;
  context.scanrel = foreignrel;
  context.buf = buf;
  context.params_list = params_list;

  appendStringInfoString(buf, "DELETE FROM ");
  deparseRelation(buf, rel);
  if (foreignrel->reloptkind == RELOPT_JOINREL)
    appendStringInfo(buf, " %s%d", REL_ALIAS_PREFIX, rtindex);

  if (foreignrel->reloptkind == RELOPT_JOINREL)
  {
    List	   *ignore_conds = NIL;

    appendStringInfoString(buf, " USING ");
    deparseFromExprForRel(buf, root, foreignrel, true, rtindex, &ignore_conds, &additional_conds, params_list);
    remote_conds = list_concat(remote_conds, ignore_conds);
  }

  appendWhereClause(remote_conds, additional_conds, &context);

  if (additional_conds != NIL)
    list_free_deep(additional_conds);

  if (foreignrel->reloptkind == RELOPT_JOINREL)
    deparseExplicitTargetList(returningList, true, retrieved_attrs, &context);
  else
    deparseReturningList(buf, planner_rt_fetch(rtindex, root), rtindex, rel, false, NIL, returningList, retrieved_attrs);
  db2Exit1(": %s", buf->data);
}
