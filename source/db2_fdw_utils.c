#include <postgres.h>
#include <catalog/pg_namespace.h>
#include <catalog/pg_operator.h>
#include <catalog/pg_proc.h>
#include <commands/vacuum.h>
#include <mb/pg_wchar.h>
#include <utils/builtins.h>
#include <utils/array.h>
#include <utils/date.h>
#include <utils/datetime.h>
#include <utils/guc.h>
#include <utils/syscache.h>
#include <nodes/pathnodes.h>
#include <optimizer/optimizer.h>
#include <access/heapam.h>
#include "db2_fdw.h"
#include "DB2FdwState.h"

/** external prototypes */
extern void         db2GetLob                 (DB2Session* session, DB2Column* column, int cidx, char** value, long* value_len);
extern void         db2Shutdown               (void);
extern short        c2dbType                  (short fcType);
extern void         db2Debug1                 (const char* message, ...);
extern void         db2Debug2                 (const char* message, ...);
extern void         db2Debug3                 (const char* message, ...);
extern void*        db2alloc                  (const char* type, size_t size);
extern void*        db2strdup                 (const char* source);
extern void         db2free                   (void* p);

/** local prototypes */
void                appendAsType              (StringInfoData* dest, Oid type);
char*               deparseExpr               (DB2Session* session, RelOptInfo* foreignrel, Expr*              expr, const DB2Table* db2Table, List** params);
char*               deparseConstExpr          (DB2Session* session, RelOptInfo* foreignrel, Const*             expr, const DB2Table* db2Table, List** params);
char*               deparseParamExpr          (DB2Session* session, RelOptInfo* foreignrel, Param*             expr, const DB2Table* db2Table, List** params);
char*               deparseVarExpr            (DB2Session* session, RelOptInfo* foreignrel, Var*               expr, const DB2Table* db2Table, List** params);
char*               deparseOpExpr             (DB2Session* session, RelOptInfo* foreignrel, OpExpr*            expr, const DB2Table* db2Table, List** params);
char*               deparseScalarArrayOpExpr  (DB2Session* session, RelOptInfo* foreignrel, ScalarArrayOpExpr* expr, const DB2Table* db2Table, List** params);
char*               deparseDistinctExpr       (DB2Session* session, RelOptInfo* foreignrel, DistinctExpr*      expr, const DB2Table* db2Table, List** params);
char*               deparseNullIfExpr         (DB2Session* session, RelOptInfo* foreignrel, NullIfExpr*        expr, const DB2Table* db2Table, List** params);
char*               deparseBoolExpr           (DB2Session* session, RelOptInfo* foreignrel, BoolExpr*          expr, const DB2Table* db2Table, List** params);
char*               deparseCaseExpr           (DB2Session* session, RelOptInfo* foreignrel, CaseExpr*          expr, const DB2Table* db2Table, List** params);
char*               deparseCoalesceExpr       (DB2Session* session, RelOptInfo* foreignrel, CoalesceExpr*      expr, const DB2Table* db2Table, List** params);
char*               deparseFuncExpr           (DB2Session* session, RelOptInfo* foreignrel, FuncExpr*          expr, const DB2Table* db2Table, List** params);
char*               deparseCoerceViaIOExpr    (CoerceViaIO* expr);
char*               deparseSQLValueFuncExpr   (SQLValueFunction* expr);
char*               datumToString             (Datum datum, Oid type);
char*               guessNlsLang              (char* nls_lang);
char*               deparseDate               (Datum datum);
char*               deparseTimestamp          (Datum datum, bool hasTimezone);
char*               deparseInterval           (Datum datum);
void                exitHook                  (int code, Datum arg);
void                convertTuple              (DB2FdwState* fdw_state, Datum* values, bool* nulls) ;
void                errorContextCallback      (void* arg);

/** appendAsType
 *   Append "s" to "dest", adding appropriate casts for datetime "type".
 */
void appendAsType (StringInfoData* dest, Oid type) {
  db2Debug1("> %s::appendAsType", __FILE__);
  db2Debug2("  dest->data: '%s'",dest->data);
  db2Debug2("  type: %d",type);
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
  db2Debug2("  dest->data: '%s'", dest->data);
  db2Debug1("< %s::appendAsType", __FILE__);
}

/** This macro is used by deparseExpr to identify PostgreSQL
 * types that can be translated to DB2 SQL.
 */
#define canHandleType(x) ((x) == TEXTOID || (x) == CHAROID || (x) == BPCHAROID \
      || (x) == VARCHAROID || (x) == NAMEOID || (x) == INT8OID || (x) == INT2OID \
      || (x) == INT4OID || (x) == OIDOID || (x) == FLOAT4OID || (x) == FLOAT8OID \
      || (x) == NUMERICOID || (x) == DATEOID || (x) == TIMEOID || (x) == TIMESTAMPOID \
      || (x) == TIMESTAMPTZOID || (x) == INTERVALOID)

/** deparseExpr
 *   Create and return an DB2 SQL string from "expr".
 *   Returns NULL if that is not possible, else an allocated string.
 *   As a side effect, all Params incorporated in the WHERE clause
 *   will be stored in "params".
 */
char* deparseExpr (DB2Session* session, RelOptInfo* foreignrel, Expr* expr, const DB2Table* db2Table, List** params) {
  char* retValue = NULL;
  db2Debug1("> %s::deparseExpr", __FILE__);
  db2Debug2("  expr: %x",expr);
  if (expr != NULL) {
    db2Debug2("  expr->type: %d",expr->type);
    switch (expr->type) {
      case T_Const: {
        retValue = deparseConstExpr(session, foreignrel, (Const*)expr, db2Table, params);
      }
      break;
      case T_Param: {
        retValue = deparseParamExpr(session, foreignrel, (Param*) expr, db2Table, params);
      }
      break;
      case T_Var: {
        retValue = deparseVarExpr (session, foreignrel, (Var*)expr, db2Table, params);
      }
      break;
      case T_OpExpr: {
        retValue = deparseOpExpr (session, foreignrel, (OpExpr*)expr, db2Table, params);
      }
      break;
      case T_ScalarArrayOpExpr: {
        retValue = deparseScalarArrayOpExpr (session, foreignrel, (ScalarArrayOpExpr*)expr, db2Table, params);
      }
      break;
      case T_DistinctExpr: {
        retValue = deparseDistinctExpr(session, foreignrel, (DistinctExpr*)expr, db2Table, params);
      }
      break;
      case T_NullIfExpr: {
        retValue = deparseNullIfExpr(session, foreignrel, (NullIfExpr*)expr, db2Table, params);
      }
      break;
      case T_BoolExpr: {
        retValue = deparseBoolExpr(session, foreignrel, (BoolExpr*)expr, db2Table, params);
      }
      break;
      case T_RelabelType: {
        retValue = deparseExpr (session, foreignrel, ((RelabelType*)expr)->arg, db2Table, params);
      }
      break;
      case T_CoerceToDomain: {
        retValue = deparseExpr (session, foreignrel, ((CoerceToDomain*)expr)->arg, db2Table, params);
      }
      break;
      case T_CaseExpr: {
        retValue = deparseCaseExpr(session, foreignrel, (CaseExpr*)expr, db2Table, params);
      }
      break;
      case T_CoalesceExpr: {
        retValue = deparseCoalesceExpr(session, foreignrel, (CoalesceExpr*)expr, db2Table, params);
      }
      break;
      case T_NullTest: {
        StringInfoData result;
        char*          arg = NULL;
        db2Debug2("  T_NullTest");
        arg = deparseExpr(session, foreignrel, ((NullTest*) expr)->arg, db2Table, params);
        db2Debug2("  T_NullTest arg: %s", arg);
        if (arg != NULL) {
          initStringInfo (&result);
          appendStringInfo (&result, "(%s IS %sNULL)", arg, ((NullTest*)expr)->nulltesttype == IS_NOT_NULL ? "NOT " : "");
        }
        retValue = (arg == NULL) ? arg : result.data;
      }
      break;
      case T_FuncExpr: {
        retValue = deparseFuncExpr(session, foreignrel, (FuncExpr*)expr, db2Table, params);
      }
      break;
      case T_CoerceViaIO: {
        retValue = deparseCoerceViaIOExpr((CoerceViaIO*) expr);
      }
      break;
      case T_SQLValueFunction: {
        retValue = deparseSQLValueFuncExpr((SQLValueFunction*)expr);
      }
      break;
      default: {
        /* we cannot translate this to DB2 */
        db2Debug2("  expression cannot be translated to DB2", __FILE__);
      }
      break;
    }
  }
  db2Debug1("< %s::deparseExpr: %s", __FILE__, retValue);
  return retValue;
}

char* deparseConstExpr         (DB2Session* session, RelOptInfo* foreignrel, Const*             expr, const DB2Table* db2Table, List** params) {
  char*  value    = NULL;

  db2Debug1("> %s::deparseConstExpr", __FILE__);
  if (expr->constisnull) {
    /* only translate NULLs of a type DB2 can handle */
    if (canHandleType (expr->consttype)) {
      StringInfoData result;
      initStringInfo (&result);
      appendStringInfo (&result, "NULL");
      value = result.data;
    }
  } else {
    /* get a string representation of the value */
    char* c = datumToString (expr->constvalue, expr->consttype);
    if (c != NULL) {
      StringInfoData result;
      initStringInfo (&result);
      appendStringInfo (&result, "%s", c);
      value = result.data;
    }
  }
  db2Debug1("< %s::deparseConstExpr: %s", __FILE__, value);
  return value;
}

char* deparseParamExpr         (DB2Session* session, RelOptInfo* foreignrel, Param*             expr, const DB2Table* db2Table, List** params) {
  char*     value = NULL;
  ListCell* cell  = NULL;
  char      parname[10];

  db2Debug1("> %s::deparseParamExpr", __FILE__);
  /* don't try to handle interval parameters */
  if (!canHandleType (expr->paramtype) || expr->paramtype == INTERVALOID) {
    db2Debug2("  !canHhandleType(expr->paramtype %d) || rxpr->paramtype == INTERVALOID)", expr->paramtype);
  } else {
    StringInfoData result;
    /* find the index in the parameter list */
    int index = 0;
    foreach (cell, *params) {
      ++index;
      if (equal (expr, (Node *) lfirst (cell)))
        break;
    }
    if (cell == NULL) {
      /* add the parameter to the list */
      ++index;
      *params = lappend (*params, expr);
    }
    /* parameters will be called :p1, :p2 etc. */
    snprintf (parname, 10, ":p%d", index);
    initStringInfo (&result);
    appendAsType (&result, expr->paramtype);
    value = result.data;
  }
  db2Debug1("< %s::deparseParamExpr: %s", __FILE__, value);
  return value;
}

char* deparseVarExpr           (DB2Session* session, RelOptInfo* foreignrel, Var*               expr, const DB2Table* db2Table, List** params) {
  char*            value     = NULL;
  const DB2Table*  var_table = NULL;  /* db2Table that belongs to a Var */

  db2Debug1("> %s::deparseVarExpr", __FILE__);
  /* check if the variable belongs to one of our foreign tables */
  #ifdef JOIN_API
  if (IS_SIMPLE_REL (foreignrel)) {
  #endif /* JOIN_API */
    if (expr->varno == foreignrel->relid && expr->varlevelsup == 0)
      var_table = db2Table;
  #ifdef JOIN_API
  } else {
    DB2FdwState* joinstate  = (DB2FdwState*) foreignrel->fdw_private;
    DB2FdwState* outerstate = (DB2FdwState*) joinstate->outerrel->fdw_private;
    DB2FdwState* innerstate = (DB2FdwState*) joinstate->innerrel->fdw_private;
    /* we can't get here if the foreign table has no columns, so this is safe */
    if (expr->varno == outerstate->db2Table->cols[0]->varno && expr->varlevelsup == 0)
      var_table = outerstate->db2Table;
    if (expr->varno == innerstate->db2Table->cols[0]->varno && expr->varlevelsup == 0)
      var_table = innerstate->db2Table;
  }
  #endif /* JOIN_API */
  if (var_table) {
    /* the variable belongs to a foreign table, replace it with the name */
    /* we cannot handle system columns */
    db2Debug2("  varattno: %d",expr->varattno);
    if (expr->varattno > 0) {
      /** Allow boolean columns here.
       * They will be rendered as ("COL" <> 0).
       */
      if (!(canHandleType (expr->vartype) || expr->vartype == BOOLOID)) {
        db2Debug2("  !(canHandleType (vartype %d) || vartype == BOOLOID",expr->vartype);
      } else {
        /* get var_table column index corresponding to this column (-1 if none) */
        int index = var_table->ncols - 1;
        while (index >= 0 && var_table->cols[index]->pgattnum != expr->varattno) {
          --index;
        }
        /* if no DB2 column corresponds, translate as NULL */
        if (index == -1) {
          StringInfoData result;
          initStringInfo (&result);
          appendStringInfo (&result, "NULL");
          value = result.data;
        } else {
          /** Don't try to convert a column reference if the type is
           * converted from a non-string type in DB2 to a string type
           * in PostgreSQL because functions and operators won't work the same.
           */
          short db2type = c2dbType(var_table->cols[index]->colType);
          db2Debug2("  db2type: %d", db2type);
          if ((expr->vartype == TEXTOID || expr->vartype == BPCHAROID || expr->vartype == VARCHAROID)  && db2type != DB2_VARCHAR && db2type != DB2_CHAR) {
            db2Debug2("  vartype: %d", expr->vartype);
          } else {
            StringInfoData result;
            StringInfoData alias;

            initStringInfo (&result);
            /* work around the lack of booleans in DB2 */
            if (expr->vartype == BOOLOID) {
              appendStringInfo (&result, "(");
            }
            /* qualify with an alias based on the range table index */
            initStringInfo (&alias);
            ADD_REL_QUALIFIER (&alias, var_table->cols[index]->varno);
            appendStringInfo (&result, "%s%s", alias.data, var_table->cols[index]->colName);
            /* work around the lack of booleans in DB2 */
            if (expr->vartype == BOOLOID) {
              appendStringInfo (&result, " <> 0)");
            }
            value = result.data;
          }
        }
      }
    }
  } else {
    // don't try to handle type interval
    if (!canHandleType (expr->vartype) || expr->vartype == INTERVALOID) {
      db2Debug2("  !canHandleType (vartype %d) || vartype == INTERVALOID", expr->vartype);
    } else {
      StringInfoData result;
      ListCell*      cell   = NULL;
      int            index  = 0;

      /* find the index in the parameter list */
      foreach (cell, *params) {
        ++index;
        if (equal (expr, (Node*) lfirst (cell)))
          break;
      }
      if (cell == NULL) {
        /* add the parameter to the list */
        ++index;
        *params = lappend (*params, expr);
      }
      /* parameters will be called :p1, :p2 etc. */
      initStringInfo (&result);
      appendStringInfo (&result, ":p%d", index);
      value = result.data;
    }
  }
  db2Debug1("< %s::deparseVarExpr: %s", __FILE__, value);
  return value;
}

char* deparseOpExpr            (DB2Session* session, RelOptInfo* foreignrel, OpExpr*            expr, const DB2Table* db2Table, List** params) {
  char*     value       = NULL;
  char*     opername    = NULL;
  char      oprkind     = 0x00;
  Oid       rightargtype= 0;
  Oid       leftargtype = 0;
  Oid       schema      = 0;
  HeapTuple tuple       ;

  /* get operator name, kind, argument type and schema */
  tuple = SearchSysCache1 (OPEROID, ObjectIdGetDatum (expr->opno));
  if (!HeapTupleIsValid (tuple)) {
    elog (ERROR, "cache lookup failed for operator %u", expr->opno);
  }
  opername     = db2strdup (((Form_pg_operator) GETSTRUCT (tuple))->oprname.data);
  oprkind      = ((Form_pg_operator) GETSTRUCT (tuple))->oprkind;
  leftargtype  = ((Form_pg_operator) GETSTRUCT (tuple))->oprleft;
  rightargtype = ((Form_pg_operator) GETSTRUCT (tuple))->oprright;
  schema       = ((Form_pg_operator) GETSTRUCT (tuple))->oprnamespace;
  ReleaseSysCache (tuple);
  /* ignore operators in other than the pg_catalog schema */
  if (schema != PG_CATALOG_NAMESPACE) {
    db2Debug2("  schema != PG_CATALOG_NAMESPACE");
  } else {
    if (!canHandleType (rightargtype)) {
      db2Debug2("  !canHandleType rightargtype(%d)", rightargtype);
    } else {
      /** Don't translate operations on two intervals.
     * INTERVAL YEAR TO MONTH and INTERVAL DAY TO SECOND don't mix well.
     */
      if (leftargtype == INTERVALOID && rightargtype == INTERVALOID) {
        db2Debug2("  leftargtype == INTERVALOID && rightargtype == INTERVALOID");
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
          char* left = deparseExpr (session, foreignrel, linitial (expr->args), db2Table, params);
          db2Debug2("  left: %s", left);
          if (left != NULL) {
            if (oprkind == 'b') {
              /* binary operator */
              char* right = deparseExpr (session, foreignrel, lsecond (expr->args), db2Table, params);
              db2Debug2("  right: %s", right);
              if (right != NULL) {
                StringInfoData result;
                initStringInfo (&result);
                if (strcmp (opername, "~~") == 0) {
                  appendStringInfo (&result, "(%s LIKE %s ESCAPE '\\')", left, right);
                } else if (strcmp (opername, "!~~") == 0) {
                  appendStringInfo (&result, "(%s NOT LIKE %s ESCAPE '\\')", left, right);
                } else if (strcmp (opername, "~~*") == 0) {
                  appendStringInfo (&result, "(UPPER(%s) LIKE UPPER(%s) ESCAPE '\\')", left, right);
                } else if (strcmp (opername, "!~~*") == 0) {
                  appendStringInfo (&result, "(UPPER(%s) NOT LIKE UPPER(%s) ESCAPE '\\')", left, right);
                } else if (strcmp (opername, "^") == 0) {
                  appendStringInfo (&result, "POWER(%s, %s)", left, right);
                } else if (strcmp (opername, "%") == 0) {
                  appendStringInfo (&result, "MOD(%s, %s)", left, right);
                } else if (strcmp (opername, "&") == 0) {
                  appendStringInfo (&result, "BITAND(%s, %s)", left, right);
                } else {
                  /* the other operators have the same name in DB2 */
                  appendStringInfo (&result, "(%s %s %s)", left, opername, right);
                }
                value = result.data;
              }
            } else {
              StringInfoData result;
              initStringInfo (&result);
              /* unary operator */
              if (strcmp (opername, "|/") == 0) {
                appendStringInfo (&result, "SQRT(%s)", left);
              } else if (strcmp (opername, "@") == 0) {
                appendStringInfo (&result, "ABS(%s)", left);
              } else {
                /* unary + or - */
                appendStringInfo (&result, "(%s%s)", opername, left);
              }
              value = result.data;
            }
          }
        } else {
          /* cannot translate this operator */
          db2Debug2("  cannot translate this opername: %s", opername);
        }
      }
    }
  }
  db2free (opername);
  return value;
}

char* deparseScalarArrayOpExpr (DB2Session* session, RelOptInfo* foreignrel, ScalarArrayOpExpr* expr, const DB2Table* db2Table, List** params) {
  char*              value     = NULL;
  char*              opername;
  Oid                leftargtype;
  Oid                schema;
  HeapTuple          tuple;

  db2Debug1("> %s::deparseExpr", __FILE__);
  tuple = SearchSysCache1 (OPEROID, ObjectIdGetDatum (expr->opno));
  if (!HeapTupleIsValid (tuple)) {
    elog (ERROR, "cache lookup failed for operator %u", expr->opno);
  }
  opername    = db2strdup(((Form_pg_operator) GETSTRUCT (tuple))->oprname.data);
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
    db2Debug2("  schema != PG_CATALOG_NAMESPACE");
  } else {
    /* don't try to push down anything but IN and NOT IN expressions */
    if ((strcmp (opername, "=") != 0 || !expr->useOr) && (strcmp (opername, "<>") != 0 || expr->useOr)) {
      db2Debug2("  don't try to push down anything but IN and NOT IN expressions");
    } else {
      if (!canHandleType (leftargtype)) {
        db2Debug2("  cannot Handle Type leftargtype (%d)", leftargtype);
      } else {
        char* left = deparseExpr (session, foreignrel, linitial (expr->args), db2Table, params);
        db2Debug2("  left: %s", left);
        if (left != NULL) {
          Expr*          rightexpr = NULL;
          bool           bResult   = true;
          StringInfoData result;
          /* begin to compose result */
          initStringInfo (&result);
          appendStringInfo (&result, "(%s %s (", left, expr->useOr ? "IN" : "NOT IN");
          /* the second (=last) argument can be Const, ArrayExpr or ArrayCoerceExpr */
          rightexpr = (Expr*)llast(expr->args);
          switch (rightexpr->type) {
            case T_Const: {
              /* the second (=last) argument is a Const of ArrayType */
              Const* constant = (Const*) rightexpr;
              /* using NULL in place of an array or value list is valid in DB2 and PostgreSQL */
              if (constant->constisnull) {
                appendStringInfo (&result, "NULL");
              } else {
                Datum         datum;
                bool          isNull;
                ArrayIterator iterator = array_create_iterator (DatumGetArrayTypeP (constant->constvalue), 0);
                bool          first_arg = true;

                /* loop through the array elements */
                while (array_iterate (iterator, &datum, &isNull)) {
                  char *c;
                  if (isNull) {
                    c = "NULL";
                  } else {
                    c = datumToString (datum, leftargtype);
                    db2Debug2("  c: %s",c);
                    if (c == NULL) {
                      array_free_iterator (iterator);
                      bResult = false;
                      break;
                    }
                  }
                  /* append the argument */
                  appendStringInfo (&result, "%s%s", first_arg ? "" : ", ", c);
                  first_arg = false;
                }
                array_free_iterator (iterator);
                db2Debug2("  first_arg: %s", first_arg ? "true":"false");
                if (first_arg) {
                  // don't push down empty arrays
                  // since the semantics for NOT x = ANY(<empty array>) differ
                  bResult = false;
                }
              }
            }
            break;
            case T_ArrayCoerceExpr: {
              /* the second (=last) argument is an ArrayCoerceExpr */
              ArrayCoerceExpr* arraycoerce = (ArrayCoerceExpr *) rightexpr;
              /* if the conversion requires more than binary coercion, don't push it down */
              if (arraycoerce->elemexpr && arraycoerce->elemexpr->type != T_RelabelType) {
                db2Debug2(" arraycoerce->elemexpr && arraycoerce->elemexpr->type != T_RelabelType");
                bResult = false;
                break;
              }
              /* the actual array is here */
              rightexpr = arraycoerce->arg;
            }
            /* fall through ! */
            case T_ArrayExpr: {
              /* the second (=last) argument is an ArrayExpr */
              ArrayExpr* array     = (ArrayExpr*) rightexpr;
              ListCell*  cell      = NULL;
              bool       first_arg = true;
              /* loop the array arguments */
              foreach (cell, array->elements) {
                /* convert the argument to a string */
                char* element = deparseExpr (session, foreignrel, (Expr *) lfirst (cell), db2Table, params);
                db2Debug2("  element: %s", element);
                if (element == NULL) {
                  /* if any element cannot be converted, give up */
                  bResult = false;
                  break;
                }
                /* append the argument */
                appendStringInfo (&result, "%s%s", first_arg ? "" : ", ", element);
                first_arg = false;
              }
              db2Debug2("  first_arg: %s", first_arg ? "true" : "false");
              if (first_arg) {
                /* don't push down empty arrays, since the semantics for NOT x = ANY(<empty array>) differ */
                bResult = false;
                break;
              }
            }
            break;
            default: {
              db2Debug2("  rightexpr->type(%d) default ",rightexpr->type);
              bResult = false;
            }
            break;
          }
          // only when there is a usable result otherwise keep value to null
          if (bResult) {
            /* two parentheses close the expression */
            appendStringInfo (&result, "))");
            value = result.data;
          }
        }
      }
    }
  }
  db2Debug1("< %s::deparseExpr: %s", __FILE__, value);
  return value;
}

char* deparseDistinctExpr      (DB2Session* session, RelOptInfo* foreignrel, DistinctExpr*      expr, const DB2Table* db2Table, List** params) {
  char*     value        = NULL;
  Oid       rightargtype = 0;
  HeapTuple tuple;

  db2Debug1("> %s::deparseDistinctExpr", __FILE__);
  tuple = SearchSysCache1 (OPEROID, ObjectIdGetDatum ((expr)->opno));
  if (!HeapTupleIsValid (tuple)) {
    elog (ERROR, "cache lookup failed for operator %u", (expr)->opno);
  }
  rightargtype = ((Form_pg_operator) GETSTRUCT (tuple))->oprright;
  ReleaseSysCache (tuple);
  if (!canHandleType (rightargtype)) {
    db2Debug2(" cannot Handle Type rightargtype (%d)",rightargtype);
  } else {
    char* left = deparseExpr (session, foreignrel, linitial ((expr)->args), db2Table, params);
    db2Debug2("  left: %s", left);
    if (left != NULL) {
      char* right = deparseExpr (session, foreignrel, lsecond ((expr)->args), db2Table, params);
      db2Debug2("  right: %s", right);
      if (right != NULL) {
        StringInfoData result;
        initStringInfo (&result);
        appendStringInfo (&result, "(%s IS DISTINCT FROM %s)", left, right);
        value = result.data;
      }
    }
  }
  db2Debug1("1 %s::deparseDistinctExpr: %s", __FILE__, value);
  return value;
}

char* deparseNullIfExpr        (DB2Session* session, RelOptInfo* foreignrel, NullIfExpr*        expr, const DB2Table* db2Table, List** params) {
  char*     value        = NULL;
  Oid       rightargtype = 0;
  HeapTuple tuple;

  db2Debug1("> %s::deparseNullIfExpr", __FILE__);
  tuple        = SearchSysCache1 (OPEROID, ObjectIdGetDatum ((expr)->opno));
  if (!HeapTupleIsValid (tuple)) {
    elog (ERROR, "cache lookup failed for operator %u", (expr)->opno);
  }
  rightargtype = ((Form_pg_operator) GETSTRUCT (tuple))->oprright;
  ReleaseSysCache (tuple);
  if (!canHandleType (rightargtype)) {
    db2Debug2("  cannot Handle Type rightargtype (%d)",rightargtype);
  } else {
    char* left = deparseExpr (session, foreignrel, linitial((expr)->args), db2Table, params);
    db2Debug2("  left: %s", left);
    if (left != NULL) {
      char* right = deparseExpr (session, foreignrel, lsecond((expr)->args), db2Table, params);
      db2Debug2("  right: %s", right);
      if (right != NULL) {
        StringInfoData result;
        initStringInfo (&result);
        appendStringInfo (&result, "NULLIF(%s, %s)", left, right);
        value = result.data;
      }
    }
  }
  db2Debug1("< %s::deparseNullIfExpr: %s", __FILE__, value);
  return value;
}

char* deparseBoolExpr          (DB2Session* session, RelOptInfo* foreignrel, BoolExpr*          expr, const DB2Table* db2Table, List** params) {
  ListCell* cell     = NULL;
  char*     arg      = NULL;
  db2Debug1("> %s::deparseBoolExpr", __FILE__);
  arg      = deparseExpr (session, foreignrel, linitial(expr->args), db2Table, params);
  if (arg != NULL) {
    StringInfoData result;
    bool           bBreak = false;
    initStringInfo (&result);
    appendStringInfo (&result, "(%s%s", expr->boolop == NOT_EXPR ? "NOT " : "", arg);
    do_each_cell(cell, expr->args, list_next(expr->args, list_head(expr->args))) { 
      arg = deparseExpr (session, foreignrel, (Expr*)lfirst(cell), db2Table, params);
      if (arg != NULL) {
        appendStringInfo (&result, " %s %s", expr->boolop == AND_EXPR ? "AND" : "OR", arg);
      } else {
        bBreak = true;
        break;
      }
    }
    if (!bBreak) {
      appendStringInfo (&result, ")");
      arg = result.data;
    } else {
      db2free(result.data);
      arg = NULL;
    }
  }
  db2Debug1("< %s::deparseBoolExpr: %s", __FILE__, arg);
  return arg;
}

char* deparseCaseExpr          (DB2Session* session, RelOptInfo* foreignrel, CaseExpr*          expr, const DB2Table* db2Table, List** params) {
  char*      value   = NULL;
  db2Debug1("> %s::deparseCaseExpr", __FILE__);
  if (!canHandleType (expr->casetype)) {
    db2Debug2("  cannot Handle Type caseexpr->casetype (%d)", expr->casetype);
  } else {
    StringInfoData result;
    bool           bBreak    = false;
    char*          arg       = NULL;
    ListCell*      cell      = NULL;

    initStringInfo   (&result);
    appendStringInfo (&result, "CASE");

    if (expr->arg != NULL) {
      /* for the form "CASE arg WHEN ...", add first expression */
      arg = deparseExpr (session, foreignrel, expr->arg, db2Table, params);
      db2Debug2("  CASE %s WHEN ...", arg);
      if (arg == NULL) {
        appendStringInfo (&result, " %s", arg);
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
          arg = deparseExpr (session, foreignrel, whenclause->expr, db2Table, params);
        } else {
          /* for CASE arg WHEN ..., use only the right branch of the equality */
          arg = deparseExpr (session, foreignrel, lsecond (((OpExpr*) whenclause->expr)->args), db2Table, params);
        }
        db2Debug2(" WHEN %s ", arg);
        if (arg != NULL) {
          appendStringInfo (&result, " WHEN %s", arg);
        } else {
          bBreak = true;
          break;
        }    /* THEN */
        arg = deparseExpr (session, foreignrel, whenclause->result, db2Table, params);
        db2Debug2(" THEN %s ", arg);
        if (arg != NULL) {
          appendStringInfo (&result, " THEN %s", arg);
        } else {
          bBreak = true;
          break;
        }
      }
      if (!bBreak) {
        /* append ELSE clause if appropriate */
        if (expr->defresult != NULL) {
          arg = deparseExpr (session, foreignrel, expr->defresult, db2Table, params);
          db2Debug2("  ELSE %s", arg);
          if (arg != NULL) {
            appendStringInfo (&result, " ELSE %s", arg);
          } else {
            bBreak = true;
          }
        }
        /* append END */
        appendStringInfo (&result, " END");
      }
    }
    if (!bBreak) {
      value = result.data;
    } else {
      // in case somwhere within the construct we encountered an expression not supported
      db2free(result.data);
      value = NULL;
    }
  }
  db2Debug1("< %s::deparseCaseExpr: %s", __FILE__, value);
  return value;
}

char* deparseCoalesceExpr      (DB2Session* session, RelOptInfo* foreignrel, CoalesceExpr*      expr, const DB2Table* db2Table, List** params) {
  char*          value        = NULL;
  db2Debug1("> %s::deparseCoalesceExpr", __FILE__);
  if (!canHandleType (expr->coalescetype)) {
    db2Debug2("  cannot Handle Type coalesceexpr->coalescetype (%d)", expr->coalescetype);
  } else {
    StringInfoData result;
    char*          arg       = NULL;
    bool           first_arg = true;
    ListCell*      cell      = NULL;
    initStringInfo   (&result);
    appendStringInfo (&result, "COALESCE(");
    foreach (cell, expr->args) {
      arg = deparseExpr (session, foreignrel, (Expr*)lfirst(cell), db2Table, params);
      db2Debug2("  arg: %s", arg);
      if (arg != NULL) {
        appendStringInfo(&result, ((first_arg) ? "%s" : ", %s"), arg);
        first_arg = false;
      } else {
        break;
      }
    }
    appendStringInfo (&result, ")");
    value = (arg == NULL) ? arg : result.data;
  }
  db2Debug1("< %s::deparseCoalesceExpr: %s", __FILE__, value);
  return value;
}

char* deparseFuncExpr          (DB2Session* session, RelOptInfo* foreignrel, FuncExpr*          expr, const DB2Table* db2Table, List** params) {
  char*     value = NULL;
  db2Debug1("> %s::deparseFuncExpr", __FILE__);
  if (!canHandleType (expr->funcresulttype)) {
    db2Debug2(" cannot handle funct->funcresulttype: %d",expr->funcresulttype);
  } else if (expr->funcformat == COERCE_IMPLICIT_CAST) {
      /* do nothing for implicit casts */
      db2Debug2(" COERCE_IMPLICIT_CAST == expr->funcformat(%d)",expr->funcformat);
      value = deparseExpr (session, foreignrel, linitial (expr->args), db2Table, params);
  } else {
    StringInfoData result;
    Oid       schema;
    char*     opername;
    char*     left;
    char*     right;
    char*     arg;
    bool      first_arg;
    HeapTuple tuple;
    /* get function name and schema */
    tuple = SearchSysCache1 (PROCOID, ObjectIdGetDatum (expr->funcid));
    if (!HeapTupleIsValid (tuple)) {
      elog (ERROR, "cache lookup failed for function %u", expr->funcid);
    }
    opername = db2strdup (((Form_pg_proc) GETSTRUCT (tuple))->proname.data);
    db2Debug2("  opername: %s",opername);
    schema = ((Form_pg_proc) GETSTRUCT (tuple))->pronamespace;
    db2Debug2("  schema: %d",schema);
    ReleaseSysCache (tuple);
    /* ignore functions in other than the pg_catalog schema */
    if (schema != PG_CATALOG_NAMESPACE) {
      db2Debug2("  T_FuncExpr: schema(%d) != PG_CATALOG_NAMESPACE", schema);
      db2Debug1("< %s::deparseExpr: NULL", __FILE__);
      return NULL;
    }
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
      ListCell* cell;
      initStringInfo (&result);
      if (strcmp (opername, "ceiling") == 0)
        appendStringInfo (&result, "CEIL(");
      else if (strcmp (opername, "char_length") == 0 || strcmp (opername, "character_length") == 0)
        appendStringInfo (&result, "LENGTH(");
      else if (strcmp (opername, "pow") == 0)
        appendStringInfo (&result, "POWER(");
      else if (strcmp (opername, "octet_length") == 0)
        appendStringInfo (&result, "LENGTHB(");
      else if (strcmp (opername, "position") == 0 || strcmp (opername, "strpos") == 0)
        appendStringInfo (&result, "INSTR(");
      else if (strcmp (opername, "substring") == 0)
        appendStringInfo (&result, "SUBSTR(");
      else
        appendStringInfo (&result, "%s(", opername);
      first_arg = true;
      foreach (cell, expr->args) {
        arg = deparseExpr (session, foreignrel, lfirst (cell), db2Table, params);
        if (arg == NULL) {
          db2free (result.data);
          db2Debug2("  T_FuncExpr: function %s that we cannot render for DB2", opername);
          db2free (opername);
          db2Debug1("< %s::deparseExpr: NULL", __FILE__);
          return NULL;
        }
        if (first_arg) {
          first_arg = false;
          appendStringInfo (&result, "%s", arg);
        } else {
          appendStringInfo (&result, ", %s", arg);
        }
        db2free(arg);
      }
      appendStringInfo (&result, ")");
    } else if (strcmp (opername, "date_part") == 0) {
      /* special case: EXTRACT */
      left = deparseExpr (session, foreignrel, linitial (expr->args), db2Table, params);
      if (left == NULL) {
        db2Debug2("  T_FuncExpr: function %s that we cannot render for DB2", opername);
        db2free (opername);
        db2Debug1("< %s::deparseExpr: NULL", __FILE__);
        return NULL;
      }
      /* can only handle these fields in DB2 */
      if (strcmp (left, "'year'")          == 0 || strcmp (left, "'month'")           == 0
      ||  strcmp (left, "'day'")           == 0 || strcmp (left, "'hour'")            == 0
      ||  strcmp (left, "'minute'")        == 0 || strcmp (left, "'second'")          == 0
      ||  strcmp (left, "'timezone_hour'") == 0 || strcmp (left, "'timezone_minute'") == 0) {
        /* remove final quote */
        left[strlen (left) - 1] = '\0';
        right = deparseExpr (session, foreignrel, lsecond (expr->args), db2Table, params);
        if (right == NULL) {
          db2Debug2("  T_FuncExpr: function %s that we cannot render for DB2", opername);
          db2free (opername);
          db2free (left);
          db2Debug1("< %s::deparseExpr: NULL", __FILE__);
          return NULL;
        }
        initStringInfo (&result);
        appendStringInfo (&result, "EXTRACT(%s FROM %s)", left + 1, right);
      } else {
        db2Debug2("  T_FuncExpr: function %s that we cannot render for DB2", opername);
        db2free (opername);
        db2free (left);
        db2Debug1("< %s::deparseExpr: NULL", __FILE__);
        return NULL;
      }
      db2free (left);
      db2free (right);
    } else if (strcmp (opername, "now") == 0 || strcmp (opername, "transaction_timestamp") == 0) {
      /* special case: current timestamp */
      initStringInfo (&result);
      appendStringInfo (&result, "(CAST (?/*:now*/ AS TIMESTAMP))");
    } else {
      /* function that we cannot render for DB2 */
      db2Debug2("  T_FuncExpr: function %s that we cannot render for DB2", opername);
      db2free (opername);
      db2Debug1("< %s::deparseExpr: NULL", __FILE__);
      return NULL;
    }
    db2free (opername);
    value = result.data;
  }
  db2Debug1("< %s::deparseFuncExpr: %s", __FILE__, value);
  return value;
}

char* deparseCoerceViaIOExpr   (CoerceViaIO* expr) {
  char*        value = NULL;

  db2Debug1("> %s::deparseCoerceViaIOExpr", __FILE__);
  /* We will only handle casts of 'now'.   */
  /* only casts to these types are handled */
  if (expr->resulttype != DATEOID && expr->resulttype != TIMESTAMPOID && expr->resulttype != TIMESTAMPTZOID) {
    db2Debug2("  only casts to DATEOID, TIMESTAMPOID and TIMESTAMPTZOID are handled");
  } else if (expr->arg->type != T_Const) {
  /* the argument must be a Const */
    db2Debug2("  T_CoerceViaIO: the argument must be a Const");
  } else {
    Const* constant = (Const *) expr->arg;
    if (constant->constisnull || (constant->consttype != CSTRINGOID && constant->consttype != TEXTOID)) {
    /* the argument must be a not-NULL text constant */
      db2Debug2("  T_CoerceViaIO: the argument must be a not-NULL text constant");
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
        db2Debug2("  value must be 'now'");
      } else {
        switch (expr->resulttype) {
          case DATEOID:
            value = "TRUNC(CAST (CAST(?/*:now*/ AS TIMESTAMP) AS DATE))";
          break;
          case TIMESTAMPOID:
            value = "(CAST (CAST (?/*:now*/ AS TIMESTAMP) AS TIMESTAMP))";
          break;
          case TIMESTAMPTZOID:
            value = "(CAST (?/*:now*/ AS TIMESTAMP))";
          break;
          case TIMEOID:
            value = "(CAST (CAST (?/*:now*/ AS TIME) AS TIME))";
          break;
          case TIMETZOID:
            value = "(CAST (?/*:now*/ AS TIME))";
          break;
        }
      }
    }
  }
  db2Debug1("< %s::deparseCoerceViaIOExpr: %s", __FILE__, value);
  return value;
}

#if PG_VERSION_NUM >= 100000
char* deparseSQLValueFuncExpr  (SQLValueFunction* expr) {
  char* value = NULL;
  db2Debug1("> %s::deparseSQLValueFuncExpr", __FILE__);
  switch (expr->op) {
    case SVFOP_CURRENT_DATE:
      value = "TRUNC(CAST (CAST(?/*:now*/ AS TIMESTAMP) AS DATE))";
    break;
    case SVFOP_CURRENT_TIMESTAMP:
      value = "(CAST (?/*:now*/ AS TIMESTAMP))";
    break;
    case SVFOP_LOCALTIMESTAMP:
      value = "(CAST (CAST (?/*:now*/ AS TIMESTAMP) AS TIMESTAMP))";
    break;
    case SVFOP_CURRENT_TIME:
      value = "(CAST (?/*:now*/ AS TIME))";
    break;
    case SVFOP_LOCALTIME:
      value = "(CAST (CAST (?/*:now*/ AS TIME) AS TIME))";
    break;
    default:
      /* don't push down other functions */
      db2Debug2("  op %d cannot be translated to DB2", expr->op);
      value = NULL;
    break;
  }
  db2Debug1("< %s::deparseSQLValueFuncExpr: %s", __FILE__, value);
  return value;
}
#endif

/** datumToString
 *   Convert a Datum to a string by calling the type output function.
 *   Returns the result or NULL if it cannot be converted to DB2 SQL.
 */
char* datumToString (Datum datum, Oid type) {
  StringInfoData result;
  regproc        typoutput;
  HeapTuple      tuple;
  char*          str;
  char*          p;
  db2Debug1("> %s::datumToString", __FILE__);
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
  db2Debug1("< %s::datumToString - returns: '%s'", __FILE__, result.data);
  return result.data;
}

/** guessNlsLang
 *   If nls_lang is not NULL, return "NLS_LANG=<nls_lang>".
 *   Otherwise, return a good guess for DB2's NLS_LANG.
 */
char* guessNlsLang (char *nls_lang) {
  char *server_encoding, *lc_messages, *language = "AMERICAN_AMERICA", *charset = NULL;
  StringInfoData buf;
  db2Debug1("> %s::guessNlsLang(nls_lang: %s)", __FILE__, nls_lang);
  initStringInfo (&buf);
  if (nls_lang == NULL) {
    server_encoding = db2strdup (GetConfigOption ("server_encoding", false, true));
    /* find an DB2 client character set that matches the database encoding */
    if (strcmp (server_encoding, "UTF8") == 0)
      charset = "AL32UTF8";
    else if (strcmp (server_encoding, "EUC_JP") == 0)
      charset = "JA16EUC";
    else if (strcmp (server_encoding, "EUC_JIS_2004") == 0)
      charset = "JA16SJIS";
    else if (strcmp (server_encoding, "EUC_TW") == 0)
      charset = "ZHT32EUC";
    else if (strcmp (server_encoding, "ISO_8859_5") == 0)
      charset = "CL8ISO8859P5";
    else if (strcmp (server_encoding, "ISO_8859_6") == 0)
      charset = "AR8ISO8859P6";
    else if (strcmp (server_encoding, "ISO_8859_7") == 0)
      charset = "EL8ISO8859P7";
    else if (strcmp (server_encoding, "ISO_8859_8") == 0)
      charset = "IW8ISO8859P8";
    else if (strcmp (server_encoding, "KOI8R") == 0)
      charset = "CL8KOI8R";
    else if (strcmp (server_encoding, "KOI8U") == 0)
      charset = "CL8KOI8U";
    else if (strcmp (server_encoding, "LATIN1") == 0)
      charset = "WE8ISO8859P1";
    else if (strcmp (server_encoding, "LATIN2") == 0)
      charset = "EE8ISO8859P2";
    else if (strcmp (server_encoding, "LATIN3") == 0)
      charset = "SE8ISO8859P3";
    else if (strcmp (server_encoding, "LATIN4") == 0)
      charset = "NEE8ISO8859P4";
    else if (strcmp (server_encoding, "LATIN5") == 0)
      charset = "WE8ISO8859P9";
    else if (strcmp (server_encoding, "LATIN6") == 0)
      charset = "NE8ISO8859P10";
    else if (strcmp (server_encoding, "LATIN7") == 0)
      charset = "BLT8ISO8859P13";
    else if (strcmp (server_encoding, "LATIN8") == 0)
      charset = "CEL8ISO8859P14";
    else if (strcmp (server_encoding, "LATIN9") == 0)
      charset = "WE8ISO8859P15";
    else if (strcmp (server_encoding, "WIN866") == 0)
      charset = "RU8PC866";
    else if (strcmp (server_encoding, "WIN1250") == 0)
      charset = "EE8MSWIN1250";
    else if (strcmp (server_encoding, "WIN1251") == 0)
      charset = "CL8MSWIN1251";
    else if (strcmp (server_encoding, "WIN1252") == 0)
      charset = "WE8MSWIN1252";
    else if (strcmp (server_encoding, "WIN1253") == 0)
      charset = "EL8MSWIN1253";
    else if (strcmp (server_encoding, "WIN1254") == 0)
      charset = "TR8MSWIN1254";
    else if (strcmp (server_encoding, "WIN1255") == 0)
      charset = "IW8MSWIN1255";
    else if (strcmp (server_encoding, "WIN1256") == 0)
      charset = "AR8MSWIN1256";
    else if (strcmp (server_encoding, "WIN1257") == 0)
      charset = "BLT8MSWIN1257";
    else if (strcmp (server_encoding, "WIN1258") == 0)
      charset = "VN8MSWIN1258";
    else {
      /* warn if we have to resort to 7-bit ASCII */
      charset = "US7ASCII";
      ereport (WARNING,(errcode (ERRCODE_WARNING)
                      ,errmsg ("no DB2 character set for database encoding \"%s\"", server_encoding)
                      ,errdetail ("All but ASCII characters will be lost.")
                      ,errhint ("You can set the option \"%s\" on the foreign data wrapper to force an DB2 character set.", OPT_NLS_LANG)
                      )
              );
    }
    db2free(server_encoding);
    lc_messages = db2strdup (GetConfigOption ("lc_messages", false, true));
    /* try to guess those for which there is a backend translation */
    if (strncmp (lc_messages, "de_", 3) == 0 || pg_strncasecmp (lc_messages, "german", 6) == 0)
      language = "GERMAN_GERMANY";
    if (strncmp (lc_messages, "es_", 3) == 0 || pg_strncasecmp (lc_messages, "spanish", 7) == 0)
      language = "SPANISH_SPAIN";
    if (strncmp (lc_messages, "fr_", 3) == 0 || pg_strncasecmp (lc_messages, "french", 6) == 0)
      language = "FRENCH_FRANCE";
    if (strncmp (lc_messages, "in_", 3) == 0 || pg_strncasecmp (lc_messages, "indonesian", 10) == 0)
      language = "INDONESIAN_INDONESIA";
    if (strncmp (lc_messages, "it_", 3) == 0 || pg_strncasecmp (lc_messages, "italian", 7) == 0)
      language = "ITALIAN_ITALY";
    if (strncmp (lc_messages, "ja_", 3) == 0 || pg_strncasecmp (lc_messages, "japanese", 8) == 0)
      language = "JAPANESE_JAPAN";
    if (strncmp (lc_messages, "pt_", 3) == 0 || pg_strncasecmp (lc_messages, "portuguese", 10) == 0)
      language = "BRAZILIAN PORTUGUESE_BRAZIL";
    if (strncmp (lc_messages, "ru_", 3) == 0 || pg_strncasecmp (lc_messages, "russian", 7) == 0)
      language = "RUSSIAN_RUSSIA";
    if (strncmp (lc_messages, "tr_", 3) == 0 || pg_strncasecmp (lc_messages, "turkish", 7) == 0)
      language = "TURKISH_TURKEY";
    if (strncmp (lc_messages, "zh_CN", 5) == 0 || pg_strncasecmp (lc_messages, "chinese-simplified", 18) == 0)
      language = "SIMPLIFIED CHINESE_CHINA";
    if (strncmp (lc_messages, "zh_TW", 5) == 0 || pg_strncasecmp (lc_messages, "chinese-traditional", 19) == 0)
      language = "TRADITIONAL CHINESE_TAIWAN";
    appendStringInfo (&buf, "NLS_LANG=%s.%s", language, charset);
    db2free(lc_messages);
  } else {
    appendStringInfo (&buf, "NLS_LANG=%s", nls_lang);
  }
  db2Debug1("< %s::guessNlsLang - returns: '%s'", __FILE__, buf.data);
  return buf.data;
}

/** deparseDate
 *   Render a PostgreSQL date so that DB2 can parse it.
 */
char* deparseDate (Datum datum) {
  struct pg_tm   datetime_tm;
  StringInfoData s;
  db2Debug1("> %s::deparseDate", __FILE__);
  if (DATE_NOT_FINITE (DatumGetDateADT (datum)))
    ereport (ERROR, (errcode (ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE), errmsg ("infinite date value cannot be stored in DB2")));

  /* get the parts */
  (void) j2date (DatumGetDateADT (datum) + POSTGRES_EPOCH_JDATE, &(datetime_tm.tm_year), &(datetime_tm.tm_mon), &(datetime_tm.tm_mday));

  if (datetime_tm.tm_year < 0)
    ereport (ERROR, (errcode (ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE), errmsg ("BC date value cannot be stored in DB2")));

  initStringInfo (&s);
  appendStringInfo (&s, "%04d-%02d-%02d 00:00:00", datetime_tm.tm_year > 0 ? datetime_tm.tm_year : -datetime_tm.tm_year + 1, datetime_tm.tm_mon, datetime_tm.tm_mday);
  db2Debug1("< %s::deparseDate - returns: '%s'", __FILE__, s.data);
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
  db2Debug1("> %s::deparseTimestamp",__FILE__);
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
  db2Debug1("< %s::deparseTimestamp - returns: '%s'", __FILE__, s.data);
  return s.data;
}

/** deparsedeparseInterval
 *   Render a PostgreSQL timestamp so that DB2 can parse it.
 */
char* deparseInterval (Datum datum) {
  #if PG_VERSION_NUM >= 150000
  struct pg_itm tm;
  #else
  struct pg_tm tm;
  #endif
  fsec_t         fsec=0;
  StringInfoData s;
  char*          sign;
  int            idx = 0;

  db2Debug1("> %s::deparseInterval",__FILE__);
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
  db2Debug1("< %s::deparseInterval - returns: '%s'",__FILE__,s.data);
  return s.data;
}

/** convertTuple
 *   Convert a result row from DB2 stored in db2Table
 *   into arrays of values and null indicators.
 */
void convertTuple (DB2FdwState* fdw_state, Datum* values, bool* nulls) {
  char*                tmp_value = NULL;
  char*                value     = NULL;
  long                 value_len = 0;
  int                  j, 
                       index     = -1;
//  ErrorContextCallback errcb;
  Oid                  pgtype;

  db2Debug1("> %s::convertTuple",__FILE__);
  /* initialize error context callback, install it only during conversions */
//  errcb.callback = errorContextCallback;
//  errcb.arg = (void *) fdw_state;

  /* assign result values */
  for (j = 0; j < fdw_state->db2Table->npgcols; ++j) {
    short db2Type;
    db2Debug2("  start processing column %d of %d",j + 1, fdw_state->db2Table->npgcols);
    db2Debug2("  index: %d",index);
    /* for dropped columns, insert a NULL */
    if ((index + 1 < fdw_state->db2Table->ncols) && (fdw_state->db2Table->cols[index + 1]->pgattnum > j + 1)) {
      nulls[j] = true;
      values[j] = PointerGetDatum (NULL);
      continue;
    } else {
      ++index;
    }
    db2Debug2("  index: %d",index);
    /*
     * Columns exceeding the length of the DB2 table will be NULL,
     * as well as columns that are not used in the query.
     * Geometry columns are NULL if the value is NULL,
     * for all other types use the NULL indicator.
     */
    if (index >= fdw_state->db2Table->ncols || fdw_state->db2Table->cols[index]->used == 0 || fdw_state->db2Table->cols[index]->val_null == -1) {
      nulls[j] = true;
      values[j] = PointerGetDatum (NULL);
      continue;
    }

    /* from here on, we can assume columns to be NOT NULL */
    nulls[j] = false;
    pgtype = fdw_state->db2Table->cols[index]->pgtype;

    /* get the data and its length */
    switch(c2dbType(fdw_state->db2Table->cols[index]->colType)) {
      case DB2_BLOB:
      case DB2_CLOB: {
        db2Debug3("  DB2_BLOB or DB2CLOB");
        /* for LOBs, get the actual LOB contents (allocated), truncated if desired */
        /* the column index is 1 based, whereas index id 0 based, so always add 1 to index when calling db2GetLob, since it does a column based access*/
        db2GetLob (fdw_state->session, fdw_state->db2Table->cols[index], index+1, &value, &value_len);
      }
      break;
      case DB2_LONGVARBINARY: {
        db2Debug3("  DB2_LONGBINARY datatypes");
        /* for LONG and LONG RAW, the first 4 bytes contain the length */
        value_len = *((int32 *) fdw_state->db2Table->cols[index]->val);
        /* the rest is the actual data */
        value = fdw_state->db2Table->cols[index]->val;
        /* terminating zero byte (needed for LONGs) */
        value[value_len] = '\0';
      }
      break;
      case DB2_FLOAT:
      case DB2_DECIMAL:
      case DB2_SMALLINT:
      case DB2_INTEGER:
      case DB2_REAL:
      case DB2_DECFLOAT:
      case DB2_DOUBLE: {
        db2Debug3("  DB2_FLOAT, DECIMAL, SMALLINT, INTEGER, REAL, DECFLOAT, DOUBLE");
        value     = fdw_state->db2Table->cols[index]->val;
        value_len = fdw_state->db2Table->cols[index]->val_len;
        value_len = (value_len == 0) ? strlen(value) : value_len;
        tmp_value = value;
        if((tmp_value = strchr(value,','))!=NULL) {
          *tmp_value = '.';
        }
      }
      break;
      default: {
        db2Debug3("  shoud be string based values");
        /* for other data types, db2Table contains the results */
        value     = fdw_state->db2Table->cols[index]->val;
        value_len = fdw_state->db2Table->cols[index]->val_len;
        value_len = (value_len == 0) ? strlen(value) : value_len;
      }
      break;
    }
    db2Debug2("  value    : '%x'", value);
    if (value != NULL) {
      db2Debug2("  value    : '%s'", value);
    }
    db2Debug2("  value_len: %ld" , value_len);
    db2Debug2("  fdw_state->db2Table->cols[%d]->val_null : %d",index,fdw_state->db2Table->cols[index]->val_len );
    db2Debug2("  fdw_state->db2Table->cols[%d]->val_null : %d",index,fdw_state->db2Table->cols[index]->val_null);
    db2Debug2("  fdw_state->db2Table->cols[%d]->pgname   : %s",index,fdw_state->db2Table->cols[index]->pgname  );
    db2Debug2("  fdw_state->db2Table->cols[%d]->pgattnum : %d",index,fdw_state->db2Table->cols[index]->pgattnum);
    db2Debug2("  fdw_state->db2Table->cols[%d]->pgtype   : %d",index,fdw_state->db2Table->cols[index]->pgtype  );
    db2Debug2("  fdw_state->db2Table->cols[%d]->pgtypemod: %d",index,fdw_state->db2Table->cols[index]->pgtypmod);
    /* fill the TupleSlot with the data (after conversion if necessary) */
    if (pgtype == BYTEAOID) {
      /* binary columns are not converted */
      bytea* result = (bytea*) db2alloc ("bytea", value_len + VARHDRSZ);
      memcpy (VARDATA (result), value, value_len);
      SET_VARSIZE (result, value_len + VARHDRSZ);

      values[j] = PointerGetDatum (result);
    } else {
      regproc   typinput;
      HeapTuple tuple;
      Datum     dat;
      db2Debug2("  pgtype: %d",pgtype);
      /* find the appropriate conversion function */
      tuple = SearchSysCache1 (TYPEOID, ObjectIdGetDatum (pgtype));
      if (!HeapTupleIsValid (tuple)) {
        elog (ERROR, "cache lookup failed for type %u", pgtype);
      }
      typinput = ((Form_pg_type) GETSTRUCT (tuple))->typinput;
      ReleaseSysCache (tuple);
      db2Debug3("  CStringGetDatum");
      dat = CStringGetDatum (value);
      /* install error context callback */
//      db2Debug3("  error_context_stack");
//      db2Debug2("  errcb.previous: %x",errcb.previous);
//      errcb.previous         = error_context_stack;
//      db2Debug2("  errcb.previous: %x",errcb.previous);
//      db2Debug2("  &errcb: %x", &errcb);
//      error_context_stack    = &errcb;
      db2Debug2("  index: %d", index);
      fdw_state->columnindex = index;

      /* for string types, check that the data are in the database encoding */
      if (pgtype == BPCHAROID || pgtype == VARCHAROID || pgtype == TEXTOID) {
        db2Debug3("  pg_verify_mbstr");
        (void) pg_verify_mbstr (GetDatabaseEncoding (), value, value_len, fdw_state->db2Table->cols[index]->noencerr == NO_ENC_ERR_TRUE);
      }
      /* call the type input function */
      switch (pgtype) {
        case BPCHAROID:
        case VARCHAROID:
        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
        case TIMEOID:
        case TIMETZOID:
        case INTERVALOID:
        case NUMERICOID:
          db2Debug3("  Calling OidFunctionCall3");
          /* these functions require the type modifier */
          values[j] = OidFunctionCall3 (typinput, dat, ObjectIdGetDatum (InvalidOid), Int32GetDatum (fdw_state->db2Table->cols[index]->pgtypmod));
          break;
        default:
          db2Debug3("  Calling OidFunctionCall1");
          /* the others don't */
          values[j] = OidFunctionCall1 (typinput, dat);
      }
      /* uninstall error context callback */
//      error_context_stack = errcb.previous;
    }

    /* release the data buffer for LOBs */
    db2Type = c2dbType(fdw_state->db2Table->cols[index]->colType);
    if (db2Type == DB2_BLOB || db2Type == DB2_CLOB) {
      if (value != NULL) {
        db2free (value);
      } else {
        db2Debug2("  not freeing value, since it is null");
      }
    }
  }
  db2Debug1("< %s::convertTuple",__FILE__);
}

/** errorContextCallback
 *   Provides the context for an error message during a type input conversion.
 *   The argument must be a pointer to a DB2FdwState.
 */
void errorContextCallback (void* arg) {
  DB2FdwState *fdw_state = (DB2FdwState*) arg;
  db2Debug1("> %s::errorContextCallback",__FILE__);
  errcontext ( "converting column \"%s\" for foreign table scan of \"%s\", row %lu"
             , quote_identifier (fdw_state->db2Table->cols[fdw_state->columnindex]->pgname)
             , quote_identifier (fdw_state->db2Table->pgname)
             , fdw_state->rowcount
            );
  db2Debug1("< %s::errorContextCallback",__FILE__);
}

/** exitHook
 *   Close all DB2 connections on process exit.
 */
void exitHook (int code, Datum arg) {
  db2Debug1("> %s::exitHook",__FILE__);
  db2Shutdown ();
  db2Debug1("< %s::exitHook",__FILE__);
}
