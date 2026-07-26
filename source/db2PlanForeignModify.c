#include <postgres.h>
#include <nodes/makefuncs.h>
#include <parser/parse_relation.h>
#include <parser/parsetree.h>
#include <optimizer/optimizer.h>
#include <access/heapam.h>
#include "db2_fdw.h"
#include "DB2FdwState.h"
#include "DB2Column.h"

/** external prototypes */
extern DB2FdwState* db2GetFdwState            (Oid foreigntableid, double* sample_percent, bool describe);
extern short        c2dbType                  (short fcType);
extern void         appendAsType              (StringInfoData* dest, Oid type);
extern List*        serializePlanData         (DB2FdwState* fdwState);

/** local prototypes */
       List*        db2PlanForeignModify(PlannerInfo* root, ModifyTable* plan, Index resultRelation, int subplan_index);
static DB2FdwState* copyPlanData        (DB2FdwState* orig);
static ParamDesc*   reverseParamList    (ParamDesc* head);
       void         addParam            (ParamDesc** paramList, DB2Column* db2col, int colnum, int txts);
       void         checkDataType       (short db2type, int scale, Oid pgtype, const char* tablename, const char* colname);

/* db2PlanForeignModify
 * Construct an DB2FdwState or copy it from the foreign scan plan.
 * Construct the DB2 DML statement and a list of necessary parameters.
 * Return the serialized DB2FdwState.
 */
List* db2PlanForeignModify (PlannerInfo* root, ModifyTable* plan, Index resultRelation, int subplan_index) {
  CmdType           operation     = plan->operation;
  RangeTblEntry*    rte           = planner_rt_fetch (resultRelation, root);
  Relation          rel           = NULL;
  StringInfoData    sql;
  List*             targetAttrs   = NIL;
  List*             returningList = NIL;
  DB2FdwState*      fdwState      = NULL;
  int               attnum        = 0;
  int               i             = 0;
  ListCell*         cell          = NULL;
  bool              has_trigger   = false, firstcol;
  ParamDesc*        param         = NULL;
  TupleDesc         tupdesc;
  Bitmapset*        updated_cols  = NULL;
  AttrNumber        col;
  int               col_idx       = -1;
  List*             result        = NIL;
  #if PG_VERSION_NUM >= 160000
  RTEPermissionInfo* perminfo     = NULL;
  #endif  /* PG_VERSION_NUM >= 160000 */

  db2Entry1();
  /* Get the updated columns and the user for permission checks.
   * We put that here at the beginning, since the way to do that changed considerably over the different PostgreSQL versions.
   */
  #if PG_VERSION_NUM >= 160000
  perminfo     = getRTEPermissionInfo(root->parse->rteperminfos, rte);
  updated_cols = bms_copy(perminfo->updatedCols);
  #else
  updated_cols = bms_copy(rte->updatedCols);
  #endif  /* PG_VERSION_NUM >= 160000 */

  /* we don't support INSERT ... ON CONFLICT */
  if (plan->onConflictAction != ONCONFLICT_NONE)
    ereport(ERROR, (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION), errmsg("INSERT with ON CONFLICT clause is not supported")));

  /* check if the foreign table is scanned and we already planned that scan */
  if (resultRelation < root->simple_rel_array_size 
  &&  root->simple_rel_array[resultRelation]              != NULL 
  &&  root->simple_rel_array[resultRelation]->fdw_private != NULL) {
    /* if yes, copy the foreign table information from the associated RelOptInfo */
    fdwState = copyPlanData((DB2FdwState*)(root->simple_rel_array[resultRelation]->fdw_private));
  } else {
    /* If no, we have to construct the foreign table data ourselves.
     * To match what ExecCheckRTEPerms does, pass the user whose user mapping should be used (if invalid, the current user is used).
     */
    fdwState = db2GetFdwState(rte->relid, NULL, true);
  }
  initStringInfo(&sql);

  /* Core code already has some lock on each rel being planned, so we can use NoLock here. */
  rel = table_open(rte->relid, NoLock);

  /* figure out which attributes are affected and if there is a trigger */
  switch (operation) {
    case CMD_INSERT:
      /*
       * In an INSERT, we transmit all columns that are defined in the foreign
       * table.  In an UPDATE, we transmit only columns that were explicitly
       * targets of the UPDATE, so as to avoid unnecessary data transmission.
       * (We can't do that for INSERT since we would miss sending default values
       * for columns not listed in the source statement.)
       */
      tupdesc = RelationGetDescr (rel);
      for (attnum = 1; attnum <= tupdesc->natts; attnum++) {
        Form_pg_attribute attr = TupleDescAttr (tupdesc, attnum - 1);
        if (!attr->attisdropped)
          targetAttrs = lappend_int (targetAttrs, attnum);
      }
      /* is there a row level AFTER trigger? */
      has_trigger = rel->trigdesc && rel->trigdesc->trig_insert_after_row;
  
    break;
    case CMD_UPDATE:
      while ((col_idx = bms_next_member (updated_cols,col_idx)) >= 0) {
        col = col_idx + FirstLowInvalidHeapAttributeNumber;
        if (col <= InvalidAttrNumber)	/* shouldn't happen */
          elog (ERROR, "system-column update is not supported");
        targetAttrs = lappend_int (targetAttrs, col);
      }
      /* is there a row level AFTER trigger? */
      has_trigger = rel->trigdesc && rel->trigdesc->trig_update_after_row;
    break;
    case CMD_DELETE:
      /* is there a row level AFTER trigger? */
      has_trigger = rel->trigdesc && rel->trigdesc->trig_delete_after_row;
    break;
    default:
      elog (ERROR, "unexpected operation: %d", (int) operation);
    break;
  }
  table_close (rel, NoLock);
  /* mark all attributes for which we need a RETURNING clause */
  if (has_trigger) {
    /* all attributes are needed for the RETURNING clause */
    for (i = 0; i < fdwState->db2Table->ncols; ++i) {
      if (fdwState->db2Table->cols[i]->pgname != NULL) {
        /* throw an error if it is a LONG or LONG RAW column */
        short dbType = c2dbType(fdwState->db2Table->cols[i]->colType);
        if (dbType == DB2_BIGINT) {
          ereport ( ERROR
                  , ( errcode (ERRCODE_FDW_INVALID_DATA_TYPE)
                    , errmsg ("columns with DB2 type LONG or LONG RAW cannot be used in RETURNING clause")
                    , errdetail ("Column \"%s\" of foreign table \"%s\" is of DB2 type LONG%s."
                                , fdwState->db2Table->cols[i]->pgname
                                , fdwState->db2Table->pgname
                                , dbType == DB2_BIGINT ? "" : " RAW"
                                )
                    )
                  );
        }
        fdwState->db2Table->cols[i]->used = 1;
      }
    }
  } else {
    Bitmapset *attrs_used = NULL;
    /* extract the relevant RETURNING list if any */
    if (plan->returningLists)
      returningList = (List *) list_nth (plan->returningLists, subplan_index);
    if (returningList != NIL) {
      /* get all the attributes mentioned there */
      pull_varattnos ((Node *) returningList, resultRelation, &attrs_used);
      /* mark the corresponding columns as used */
      for (i = 0; i < fdwState->db2Table->ncols; ++i) {
        /* ignore columns that are not in the PostgreSQL table */
        if (fdwState->db2Table->cols[i]->pgname == NULL)
          continue;
        if (bms_is_member (fdwState->db2Table->cols[i]->pgattnum - FirstLowInvalidHeapAttributeNumber, attrs_used)) {
          /* throw an error if it is a LONG or LONG RAW column */
          fdwState->db2Table->cols[i]->used = 1;
        }
      }
    }
  }
  /* construct the SQL command string */
  switch (operation) {
    case CMD_INSERT:
      appendStringInfo (&sql, "INSERT INTO %s (", fdwState->db2Table->name);
      firstcol = true;
      for (i = 0; i < fdwState->db2Table->ncols; ++i) {
        /* don't add columns beyond the end of the PostgreSQL table */
        if (fdwState->db2Table->cols[i]->pgname == NULL)
          continue;
        if (firstcol)
          firstcol = false;
        else
          appendStringInfo (&sql, ", ");
        appendStringInfo (&sql, "%s", fdwState->db2Table->cols[i]->colName);
      }
      appendStringInfo (&sql, ") VALUES (");
      firstcol = true;
      for (i = 0; i < fdwState->db2Table->ncols; ++i) {
        /* don't add columns beyond the end of the PostgreSQL table */
        if (fdwState->db2Table->cols[i]->pgname == NULL)
          continue;
        /* check that the data types can be converted */
        checkDataType (fdwState->db2Table->cols[i]->colType, fdwState->db2Table->cols[i]->colScale, fdwState->db2Table->cols[i]->pgtype, fdwState->db2Table->pgname, fdwState->db2Table->cols[i]->pgname);
        /* add a parameter description for the column */
        addParam (&fdwState->paramList, fdwState->db2Table->cols[i], i, 0);
        /* add parameter name */
        if (firstcol)
          firstcol = false;
        else
          appendStringInfo (&sql, ", ");
        appendAsType (&sql, fdwState->db2Table->cols[i]->pgtype);
      }
      appendStringInfo (&sql, ")");
    break;
    case CMD_UPDATE:
      appendStringInfo (&sql, "UPDATE %s SET ", fdwState->db2Table->name);
      firstcol = true;
      i = 0;
      foreach (cell, targetAttrs) {
        /* find the corresponding db2Table entry */
        while (i < fdwState->db2Table->ncols && fdwState->db2Table->cols[i]->pgattnum < lfirst_int (cell))
          ++i;
        if (i == fdwState->db2Table->ncols)
          break;
        /* ignore columns that don't occur in the foreign table */
        if (fdwState->db2Table->cols[i]->pgtype == 0)
          continue;
        /* check that the data types can be converted */
        checkDataType (fdwState->db2Table->cols[i]->colType, fdwState->db2Table->cols[i]->colScale, fdwState->db2Table->cols[i]->pgtype, fdwState->db2Table->pgname, fdwState->db2Table->cols[i]->pgname);
        /* add a parameter description for the column */
        addParam (&fdwState->paramList, fdwState->db2Table->cols[i], i, 0);
        /* add the parameter name to the query */
        if (firstcol)
          firstcol = false;
        else
          appendStringInfo (&sql, ", ");
        appendStringInfo (&sql, "%s = ", fdwState->db2Table->cols[i]->colName);
        appendAsType (&sql, fdwState->db2Table->cols[i]->pgtype);
      }
      db2Debug2("sql: '%s'",sql.data);
      /* throw a meaningful error if nothing is updated */
      if (firstcol)
        ereport (ERROR
                , (errcode (ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION)
                           , errmsg ("no DB2 column modified by UPDATE")
                           , errdetail ("The UPDATE statement only changes colums that do not exist in the DB2 table.")
                  )
                );
    break;
    case CMD_DELETE:
      appendStringInfo (&sql, "DELETE FROM %s", fdwState->db2Table->name);
      break;
    default:
      elog (ERROR, "unexpected operation: %d", (int) operation);
    break;
  }
  if (operation == CMD_UPDATE || operation == CMD_DELETE) {
    /* add WHERE clause with the primary key columns */
    firstcol = true;
    for (i = 0; i < fdwState->db2Table->ncols; ++i) {
      if (fdwState->db2Table->cols[i]->pkey) {
        /* only set the flag here because we only retrieve the old key values in update or delete cases, later in setModifyParms*/
        fdwState->db2Table->cols[i]->colPrimKeyPart = 1;
        /* add a parameter description */
        addParam (&fdwState->paramList, fdwState->db2Table->cols[i], i, 0);
        /* add column and parameter name to query */
        if (firstcol) {
          appendStringInfo (&sql, " WHERE");
          firstcol = false;
        } else {
          appendStringInfo (&sql, " AND");
        }
        appendStringInfo (&sql, " %s = ", fdwState->db2Table->cols[i]->colName);
        appendAsType (&sql, fdwState->db2Table->cols[i]->pgtype);
      }
    }
  }
  /* add RETURNING clause if appropriate */
  firstcol = true;
  for (i = 0; i < fdwState->db2Table->ncols; ++i) {
    if (fdwState->db2Table->cols[i]->used) {
      if (firstcol) {
        firstcol = false;
        appendStringInfo (&sql, " RETURNING ");
      } else {
        appendStringInfo (&sql, ", ");
      }
      appendStringInfo (&sql, "%s", fdwState->db2Table->cols[i]->colName);
    }
  }
    /* add the parameters for the RETURNING clause */
  firstcol = true;
  for (i = 0; i < fdwState->db2Table->ncols; ++i) {
    if (fdwState->db2Table->cols[i]->used) {
      /* check that the data types can be converted */
      checkDataType (fdwState->db2Table->cols[i]->colType, fdwState->db2Table->cols[i]->colScale, fdwState->db2Table->cols[i]->pgtype, fdwState->db2Table->pgname, fdwState->db2Table->cols[i]->pgname);

      /* create a new entry in the parameter list */
      param = (ParamDesc *) db2alloc(sizeof (ParamDesc),"param");
      param->type         = fdwState->db2Table->cols[i]->pgtype;
      param->bindType     = BIND_OUTPUT;
      param->value        = NULL;
      param->node         = NULL;
      param->colnum       = i;
      param->next         = fdwState->paramList;
      fdwState->paramList = param;

      if (firstcol) {
        firstcol = false;
        appendStringInfo (&sql, " INTO ");
      } else {
        appendStringInfo (&sql, ", ");
      }
      appendStringInfo (&sql, "?");
    }
  }

  /*
   * addParam() and the output-param builder above currently prepend to
   * fdwState->paramList.
   *
   * db2ExecuteQuery() binds parameters in list traversal order (1..N). If we
   * leave the list in prepended order, the bound parameter values won't match
   * the order of '?' placeholders in the generated SQL, leading to wrong DML
   * execution and hard-to-debug runtime failures.
   */
  fdwState->paramList = reverseParamList(fdwState->paramList);

  fdwState->query = sql.data;
  db2Debug2("fdwState->query: '%s'", fdwState->query);
  /* return a serialized form of the plan state */
  result = serializePlanData (fdwState);
  db2Exit1(": %x", result);
  return result;
}

static ParamDesc* reverseParamList(ParamDesc* head) {
  ParamDesc* prev = NULL;
  ParamDesc* cur  = head;

  while (cur) {
    ParamDesc* next = cur->next;
    cur->next = prev;
    prev = cur;
    cur = next;
  }

  return prev;
}

/** copyPlanData
 *   Create a deep copy of the argument, copy only those fields needed for planning.
 */
static DB2FdwState* copyPlanData (DB2FdwState* orig) {
  int          i    = 0;
  DB2FdwState* copy = NULL;

  db2Entry4();
  copy                    = db2alloc(sizeof (DB2FdwState), "DB2FdwState* copy");
  copy->dbserver          = db2strdup(orig->dbserver, "copy->dbserver");
  copy->user              = db2strdup(orig->user, "copy->user");
  copy->password          = db2strdup(orig->password, "copy->password");
  copy->session           = NULL;
  copy->query             = NULL;
  copy->paramList         = NULL;
  copy->db2Table          = (DB2Table*) db2alloc( sizeof (DB2Table),"copy->db2Table");
  copy->db2Table->name    = db2strdup(orig->db2Table->name,"copy->db2Table->name");
  copy->db2Table->pgname  = db2strdup(orig->db2Table->pgname,"copy->db2Table->pgname");
  copy->db2Table->ncols   = orig->db2Table->ncols;
  copy->db2Table->npgcols = orig->db2Table->npgcols;
  copy->db2Table->cols    = (DB2Column**) db2alloc(sizeof (DB2Column*) * orig->db2Table->ncols,"copy->db2Table->cols(%d)",orig->db2Table->ncols);
  for (i = 0; i < orig->db2Table->ncols; ++i) {
    copy->db2Table->cols[i]                 = (DB2Column*) db2alloc( sizeof (DB2Column),"copy->db2Table->cols[%d]",i);
    copy->db2Table->cols[i]->colName        = db2strdup(orig->db2Table->cols[i]->colName,"copy->db2Table->cols[%d]->colName",i);
    copy->db2Table->cols[i]->colType        = orig->db2Table->cols[i]->colType;
    copy->db2Table->cols[i]->colSize        = orig->db2Table->cols[i]->colSize;
    copy->db2Table->cols[i]->colScale       = orig->db2Table->cols[i]->colScale;
    copy->db2Table->cols[i]->colNulls       = orig->db2Table->cols[i]->colNulls;
    copy->db2Table->cols[i]->colChars       = orig->db2Table->cols[i]->colChars;
    copy->db2Table->cols[i]->colBytes       = orig->db2Table->cols[i]->colBytes;
    copy->db2Table->cols[i]->colPrimKeyPart = orig->db2Table->cols[i]->colPrimKeyPart;
    copy->db2Table->cols[i]->colCodepage    = orig->db2Table->cols[i]->colCodepage;
    copy->db2Table->cols[i]->pgname         = db2strdup(orig->db2Table->cols[i]->pgname,"copy->db2Table->cols[%d]->pgname",i);
    copy->db2Table->cols[i]->pgattnum       = orig->db2Table->cols[i]->pgattnum;
    copy->db2Table->cols[i]->pgtype         = orig->db2Table->cols[i]->pgtype;
    copy->db2Table->cols[i]->pgtypmod       = orig->db2Table->cols[i]->pgtypmod;
    copy->db2Table->cols[i]->used           = 0;
    copy->db2Table->cols[i]->pkey           = orig->db2Table->cols[i]->pkey;
    copy->db2Table->cols[i]->val_size       = orig->db2Table->cols[i]->val_size;
  }
  copy->startup_cost = 0.0;
  copy->total_cost   = 0.0;
  copy->rowcount     = 0;
  copy->temp_cxt     = NULL;
  copy->order_clause = NULL;
  db2Exit4(": %x", copy);
  return copy;
}

/* addParam
 * Creates a new ParamDesc with the given values and adds it to the list.
 * A deep copy of the parameter is created.
 */
void addParam (ParamDesc **paramList, DB2Column* db2col, int colnum, int txts) {
  ParamDesc *param;

  db2Entry1();
  db2Debug2("pgtype: %d",db2col->pgtype);
  db2Debug2("colType: %d",db2col->colType);
  db2Debug2("colnum: %d",colnum);
  db2Debug2("txts: %d",txts);
  param       = db2alloc(sizeof (ParamDesc), "param");
  param->colName  = db2strdup(db2col->colName,"param->colName");
  db2Debug2("param->colName: '%s'",param->colName);
  param->colType  = db2col->colType;
  db2Debug2("param->colType: '%d'",param->colType);
  param->colSize  = db2col->colSize;
  db2Debug2("param->colSize: '%d'",param->colSize);
  param->type     = db2col->pgtype;
  switch (c2dbType(db2col->colType)) {
    case DB2_INTEGER:
    case DB2_NUMERIC:
    case DB2_BIGINT:
    case DB2_SMALLINT:
    case DB2_FLOAT:
    case DB2_REAL:
    case DB2_DOUBLE:
      param->bindType = BIND_NUMBER;
    break;
    case DB2_CLOB:
      param->bindType = BIND_LONG;
    break;
    case DB2_BLOB:
      param->bindType = BIND_LONGRAW;
    break;
    default:
      param->bindType = BIND_STRING;
  }
  db2Debug2("param->bindType: '%d'",param->bindType);
  param->value    = NULL;
  db2Debug2("param->value: %x",param->value);
  param->val_size = db2col->val_size;
  db2Debug2("param->val_size: %d",param->val_size);
  param->node     = NULL;
  db2Debug2("param->node: %x",param->node);
  param->colnum   = colnum;
  db2Debug2("param->colnum: %d",param->colnum);
  param->txts     = txts;
  db2Debug2("param->txts: %d",param->txts);
  param->next     = *paramList;
  *paramList      = param;
  db2Exit1();
}

/* checkDataType
 * Check that the DB2 data type of a column can be converted to the PostgreSQL data type, raise an error if not.
 */
void checkDataType (short sqltype, int scale, Oid pgtype, const char *tablename, const char *colname) {
  short db2type = c2dbType(sqltype);
  db2Entry4();
  db2Debug4("checkDataType: %s.%s of sqltype: %d, db2type: %d, pgtype: %d",tablename,colname,sqltype, db2type, pgtype);
  /* the binary DB2 types can be converted to bytea */
  if (db2type == DB2_BLOB && pgtype == BYTEAOID) {
    db2Debug5("DB2_BLOB can be converted into BYTEAOID");
  } else if (db2type == DB2_XML && pgtype == XMLOID) {
    db2Debug5("DB2_XML can be converted into XMLOID");
  } else if (db2type != DB2_UNKNOWN_TYPE && db2type != DB2_BLOB && (pgtype == TEXTOID || pgtype == VARCHAROID || pgtype == BPCHAROID)) {
    db2Debug5("DB2_UNKNONW && not DB2_BLOB can be converted into TEXTOID, VARCHAROID, BPCHAROID");
  } else if ((db2type == DB2_INTEGER || db2type == DB2_SMALLINT || db2type == DB2_BIGINT || db2type == DB2_FLOAT || db2type == DB2_DOUBLE || db2type == DB2_REAL || db2type == DB2_DECIMAL || db2type == DB2_DECFLOAT) && (pgtype == NUMERICOID || pgtype == FLOAT4OID || pgtype == FLOAT8OID)) {
    db2Debug5("DB2_INTEGER,SMALLINT,BIGINT,FLOAT,DOUBLE,REAL,DECIMAL,DECFLOAT can be converted into NUMERICOID,FLOAT4OID,FLOAT8OID");
  } else if ((db2type == DB2_INTEGER || db2type == DB2_SMALLINT || db2type == DB2_BIGINT || db2type == DB2_BOOLEAN) && scale <= 0 && (pgtype == INT2OID || pgtype == INT4OID || pgtype == INT8OID || pgtype == BOOLOID)) {
    db2Debug5("DB2_INTEGER,SMALLINT,BIGINT,BOOLEAN can be converted into INT2OID, INT42OID, INT8OID,BOOLOID");
  } else if ((db2type == DB2_TYPE_DATE || db2type == DB2_TYPE_TIME || db2type == DB2_TYPE_TIMESTAMP || db2type == DB2_TYPE_TIMESTAMP_WITH_TIMEZONE) && (pgtype == DATEOID || pgtype == TIMESTAMPOID || pgtype == TIMESTAMPTZOID || pgtype == TIMEOID || pgtype == TIMETZOID)) {
    db2Debug5("DB2_TYPE_DATE,TIME,TIMESTAMP,TIMESTAMP_WITH_TIMEZONE can be converted into DATEOID,TIMESTAMPOID,TIMESTAMPTZOID,TIMEOID,TIMETZOID");
  } else if ((db2type == DB2_VARCHAR || db2type == DB2_CLOB) && pgtype == JSONOID) {
    db2Debug5("DB2_VARCHAR or DB2_CLOB can be converted into JSONOID");
  } else {
    /* nok - report an error */
    ereport ( ERROR
            , ( errcode (ERRCODE_FDW_INVALID_DATA_TYPE)
              , errmsg ( "column \"%s\" of type \"%d\" of foreign DB2 table \"%s\" cannot be converted to \"%d\" "
                       , colname
                       , db2type
                       , tablename
                       , pgtype)
              )
            );
  }
  db2Exit4();
}
