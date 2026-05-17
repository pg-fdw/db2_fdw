#include <postgres.h>
#include <utils/builtins.h>
#include <nodes/makefuncs.h>
#include "db2_fdw.h"
#include "DB2FdwState.h"

/** external prototypes */
extern char*        c2name              (short fcType);

/** local prototypes */
       DB2FdwState* deserializePlanData (List* list);
       List*        serializePlanData   (DB2FdwState* fdwState);

static char*        deserializeString   (Const* constant);
static long         deserializeLong     (Const* constant);
static Const*       serializeString     (const char* s);
static Const*       serializeLong       (long i);

/** deserializePlanData
 *   Extract the data structures from a List created by serializePlanData.
 */
DB2FdwState* deserializePlanData (List* list) {
  DB2FdwState* state  = db2alloc (sizeof(DB2FdwState),"DB2FdwState");
  int          idx    = 0; 
  int          i      = 0; 
  int          len    = 0;
  ParamDesc*   param  = NULL;

  db2Entry1();
  /* session will be set upon connect */
  state->session           = NULL;
  /* these fields are not needed during execution */
  state->startup_cost      = 0;
  state->total_cost        = 0;
  /* these are not serialized */
  state->rowcount          = 0;
  state->params            = NULL;
  state->temp_cxt          = NULL;
  state->order_clause      = NULL;

  state->retrieved_attr    = (List *) list_nth(list, idx++);
  /* dbserver */
  state->dbserver          = deserializeString(list_nth(list, idx++));
  /* user */
  state->user              = deserializeString(list_nth(list, idx++));
  /* password */
  state->password          = deserializeString(list_nth(list, idx++));
  /* jwt-token */
  state->jwt_token         = deserializeString(list_nth(list, idx++));
  /* nls_lang */
  state->nls_lang          = deserializeString(list_nth(list, idx++));
  /* query */
  state->query             = deserializeString(list_nth(list, idx++));
  /* DB2 prefetch count */
  state->prefetch          = (unsigned long) DatumGetInt32 (((Const*)list_nth(list, idx++))->constvalue);
  /* DB2 fetch_size */
  state->fetch_size        = (unsigned long) DatumGetInt32 (((Const*)list_nth(list, idx++))->constvalue);
  /* relation_name */
  state->relation_name     = deserializeString(list_nth(list, idx++));
  /* table data */
  state->db2Table          = (DB2Table*) db2alloc (sizeof (struct db2Table),"state->db2Table");
  state->db2Table->name    = deserializeString(list_nth(list, idx++));
  state->db2Table->pgname  = deserializeString(list_nth(list, idx++));
  state->db2Table->batchsz = (int) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
  state->db2Table->ncols   = (int) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
  state->db2Table->npgcols = (int) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
  state->db2Table->cols    = (DB2Column**) db2alloc (sizeof (DB2Column*) * state->db2Table->ncols,"state->db2Table->cols");

  /* loop columns */
  for (i = 0; i < state->db2Table->ncols; ++i) {
    state->db2Table->cols[i]           = (DB2Column *) db2alloc (sizeof (DB2Column), "state->db2Table->cols[i]");
    state->db2Table->cols[i]->colName  = deserializeString(list_nth(list, idx++));
    db2Debug3("deserialize col[%d].colName: %s"  ,i, state->db2Table->cols[i]->colName);
    state->db2Table->cols[i]->colType  = (short) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
    db2Debug3("deserialize col[%d].colType: %d"  ,i, state->db2Table->cols[i]->colType);
    state->db2Table->cols[i]->colSize  = (size_t) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
    db2Debug3("deserialize col[%d].colSize: %d"  ,i, state->db2Table->cols[i]->colSize);
    state->db2Table->cols[i]->colScale = (short) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
    db2Debug3("deserialize col[%d].colScale: %d"  ,i, state->db2Table->cols[i]->colScale);
    state->db2Table->cols[i]->colNulls = (short) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
    db2Debug3("deserialize col[%d].colNulls: %d"  ,i, state->db2Table->cols[i]->colNulls);
    state->db2Table->cols[i]->colChars = (size_t) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
    db2Debug3("deserialize col[%d].colChars: %d"  ,i, state->db2Table->cols[i]->colChars);
    state->db2Table->cols[i]->colBytes = (size_t) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
    db2Debug3("deserialize col[%d].colBytes: %d"  ,i, state->db2Table->cols[i]->colBytes);
    state->db2Table->cols[i]->colPrimKeyPart = (size_t) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
    db2Debug3("deserialize col[%d].colPrimKeyPart: %d"  ,i, state->db2Table->cols[i]->colPrimKeyPart);
    state->db2Table->cols[i]->colCodepage = (size_t) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
    db2Debug3("deserialize col[%d].colCodepage: %d"  ,i, state->db2Table->cols[i]->colCodepage);
    state->db2Table->cols[i]->pgname   = deserializeString(list_nth(list, idx++));
    db2Debug3("deserialize col[%d].pgname: %s"  ,i, state->db2Table->cols[i]->pgname);
    state->db2Table->cols[i]->pgattnum = (int) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
    db2Debug3("deserialize col[%d].pgattnum: %d"  ,i, state->db2Table->cols[i]->pgattnum);
    state->db2Table->cols[i]->pgtype   = DatumGetObjectId(((Const*)list_nth(list, idx++))->constvalue);
    db2Debug3("deserialize col[%d].pgtype: %d"  ,i, state->db2Table->cols[i]->pgtype);
    state->db2Table->cols[i]->pgtypmod = (int) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
    db2Debug3("deserialize col[%d].pgtypmod: %d"  ,i, state->db2Table->cols[i]->pgtypmod);
    state->db2Table->cols[i]->used     = (int) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
    db2Debug3("deserialize col[%d].used: %d"  ,i, state->db2Table->cols[i]->used);
    state->db2Table->cols[i]->pkey     = (int) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
    db2Debug3("deserialize col[%d].pkey: %d"  ,i, state->db2Table->cols[i]->pkey);
    state->db2Table->cols[i]->val_size = deserializeLong(list_nth(list, idx++));
    db2Debug3("deserialize col[%d].val_size: %ld"  ,i, state->db2Table->cols[i]->val_size);
    state->db2Table->cols[i]->noencerr = deserializeLong(list_nth(list, idx++));
    db2Debug3("deserialize col[%d].noencerr: %ld"  ,i, state->db2Table->cols[i]->noencerr);
  }

  /* length of parameter list */
  len  = (int) DatumGetInt32 (((Const*)list_nth(list, idx++))->constvalue);

  /* parameter table entries */
  state->paramList = NULL;
  for (i = 0; i < len; ++i) {
    param            = (ParamDesc*) db2alloc (sizeof (ParamDesc),"state->parmList->next");
    param->colName         = deserializeString(list_nth(list, idx++));
    db2Debug3("deserialize param[%d].colName: %s"  ,i, param->colName);
    param->colType        = (short) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
    db2Debug3("deserialize param[%d].colType: %d"  ,i, param->colType);
    param->colSize        = (size_t) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
    db2Debug3("deserialize param[%d].colSize: %d"  ,i, param->colSize);
    param->type      = DatumGetObjectId(((Const*)list_nth(list, idx++))->constvalue);
    db2Debug3("deserialize param[%d].type: %d"  ,i, param->type);
    param->bindType  = (db2BindType) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
    db2Debug3("deserialize param[%d].bindType: %d"  ,i, param->bindType);
    if (param->bindType == BIND_OUTPUT)
      param->value   = (void *) 42;	/* something != NULL */
    else
      param->value   = NULL;
    db2Debug3("deserialize param[%d].value: %x"  ,i, param->value);
    param->val_size  = deserializeLong(list_nth(list, idx++));
    db2Debug3("deserialize param[%d].val_size: %ld"  ,i, param->val_size);
    param->node      = NULL;
    db2Debug3("deserialize param[%d].node: %x"  ,i, param->node);
    param->colnum    = (int) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
    db2Debug3("deserialize param[%d].colnum: %d"  ,i, param->colnum);
    param->txts      = (int) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
    db2Debug3("deserialize param[%d].txts: %d"  ,i, param->txts);
    param->next      = state->paramList;
    state->paramList = param;
  }

    /* length of parameter list */
  len  = (int) DatumGetInt32 (((Const*)list_nth(list, idx++))->constvalue);
  /* parameter table entries */
  state->resultList = NULL;
  for (i = 0; i < len; ++i) {
    DB2ResultColumn* res =  (DB2ResultColumn *) db2alloc (sizeof (DB2ResultColumn),"state->resultList->next");
    res->colName        = deserializeString(list_nth(list, idx++));
    db2Debug3("deserialize res[%d].colName: %s"  ,i, res->colName);
    res->colType        = (short) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
    db2Debug3("deserialize res[%d].colType: %d"  ,i, res->colType);
    res->colSize        = (size_t) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
    db2Debug3("deserialize res[%d].colSize: %d"  ,i, res->colSize);
    res->colScale       = (short) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
    db2Debug3("deserialize res[%d].colScale: %d"  ,i, res->colScale);
    res->colNulls       = (short) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
    db2Debug3("deserialize res[%d].colNulls: %d"  ,i, res->colNulls);
    res->colChars       = (size_t) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
    db2Debug3("deserialize res[%d].colChars: %d"  ,i, res->colChars);
    res->colBytes       = (size_t) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
    db2Debug3("deserialize res[%d].colBytes: %d"  ,i, res->colBytes);
    res->colPrimKeyPart = (size_t) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
    db2Debug3("deserialize res[%d].colPrimKeyPart: %d"  ,i, res->colPrimKeyPart);
    res->colCodepage    = (size_t) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
    db2Debug3("deserialize res[%d].colCodepage: %d"  ,i, res->colCodepage);
    res->pgname         = deserializeString(list_nth(list, idx++));
    db2Debug3("deserialize res[%d].pgname: %s"  ,i, res->pgname);
    res->pgattnum       = (int) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
    db2Debug3("deserialize res[%d].pgattnum: %d"  ,i, res->pgattnum);
    res->pgtype         = DatumGetObjectId(((Const*)list_nth(list, idx++))->constvalue);
    db2Debug3("deserialize res[%d].pgtype: %d"  ,i, res->pgtype);
    res->pgtypmod       = (int) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
    db2Debug3("deserialize res[%d].pgtypmod: %d"  ,i, res->pgtypmod);
    res->pkey           = (int) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
    db2Debug3("deserialize res[%d].pkey: %d"  ,i, res->pkey);
    res->val_size       = deserializeLong(list_nth(list, idx++));
    db2Debug3("deserialize res[%d].val_size: %ld"  ,i, res->val_size);
    res->noencerr       = deserializeLong(list_nth(list, idx++));
    db2Debug3("deserialize res[%d].noencerr: %ld"  ,i, res->noencerr);
    res->resnum         = (int) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
    db2Debug3("deserialize res[%d].resnum: %d"  ,i, res->resnum);
    res->val            = (char*) db2alloc (MIN(res->val_size + 1, 1073741823), "res->val");
    res->val_len        = 0;
    res->val_null       = 1;
    res->next           = state->resultList;
    state->resultList   = res;
  }
  db2Exit1(": %x", state);
  return state;
}

/** deserializeString
 *   Extracts a string from a Const, returns a deep copy.
 */
static char* deserializeString (Const* constant) {
  char* result = NULL;
  db2Entry5();
  if (!constant->constisnull)
    result = text_to_cstring (DatumGetTextP (constant->constvalue));
  db2Exit5(": '%s'", result);
  return result;
}

/** deserializeLong
 *   Extracts a long integer from a Const.
 */
static long deserializeLong (Const* constant) {
  long result = 0L;
  db2Entry5();
  result =  (sizeof (long) <= 4) ? (long) DatumGetInt32 (constant->constvalue)
                                 : (long) DatumGetInt64 (constant->constvalue);
  db2Exit5(": %ld", result);
  return result;
}

/** serializePlanData
 *   Create a List representation of plan data that copyObject can copy.
 *   This List can be parsed by deserializePlanData.
 */
List* serializePlanData (DB2FdwState* fdwState) {
  List*             result    = NIL;
  int               idxCol    = 0;
  int               lenParam  = 0;
  ParamDesc*        param     = NULL;
  DB2ResultColumn*  rcol      = NULL;

  db2Entry1();
  result = list_make1(fdwState->retrieved_attr);
  /* dbserver */
  result = lappend (result, serializeString (fdwState->dbserver));
  /* user name */
  result = lappend (result, serializeString (fdwState->user));
  /* password */
  result = lappend (result, serializeString (fdwState->password));
  /* jwt_token */
  result = lappend (result, serializeString (fdwState->jwt_token));
  /* nls_lang */
  result = lappend (result, serializeString (fdwState->nls_lang));
  /* query */
  result = lappend (result, serializeString (fdwState->query));
  /* DB2 prefetch count */
  result = lappend (result, serializeLong (fdwState->prefetch));
  /* DB2 fetchsize count */
  result = lappend (result, serializeLong (fdwState->fetch_size));
  /* relation_name */
  result = lappend (result, serializeString (fdwState->relation_name));
  /* DB2 table name */
  result = lappend (result, serializeString (fdwState->db2Table->name));
  /* PostgreSQL table name */
  result = lappend (result, serializeString (fdwState->db2Table->pgname));
  /* batch size in DB2 table */
  result = lappend (result, serializeInt (fdwState->db2Table->batchsz));
  /* number of columns in DB2 table */
  result = lappend (result, serializeInt (fdwState->db2Table->ncols));
  /* number of columns in PostgreSQL table */
  result = lappend (result, serializeInt (fdwState->db2Table->npgcols));
  /* column data */
  for (idxCol = 0; idxCol < fdwState->db2Table->ncols; ++idxCol) {
    result = lappend (result, serializeString (fdwState->db2Table->cols[idxCol]->colName));
    db2Debug3("serialize col[%d].colName: %s"  ,idxCol, fdwState->db2Table->cols[idxCol]->colName);
    result = lappend (result, serializeInt    (fdwState->db2Table->cols[idxCol]->colType));
    db2Debug3("serialize col[%d].colType: %d"  ,idxCol, fdwState->db2Table->cols[idxCol]->colType);
    result = lappend (result, serializeInt    (fdwState->db2Table->cols[idxCol]->colSize));
    db2Debug3("serialize col[%d].colSize: %d"  ,idxCol, fdwState->db2Table->cols[idxCol]->colSize);
    result = lappend (result, serializeInt    (fdwState->db2Table->cols[idxCol]->colScale));
    db2Debug3("serialize col[%d].colScale: %d"  ,idxCol, fdwState->db2Table->cols[idxCol]->colScale);
    result = lappend (result, serializeInt    (fdwState->db2Table->cols[idxCol]->colNulls));
    db2Debug3("serialize col[%d].colNulls: %d"  ,idxCol, fdwState->db2Table->cols[idxCol]->colNulls);
    result = lappend (result, serializeInt    (fdwState->db2Table->cols[idxCol]->colChars));
    db2Debug3("serialize col[%d].colChars: %d"  ,idxCol, fdwState->db2Table->cols[idxCol]->colChars);
    result = lappend (result, serializeInt    (fdwState->db2Table->cols[idxCol]->colBytes));
    db2Debug3("serialize col[%d].colBytes: %d"  ,idxCol, fdwState->db2Table->cols[idxCol]->colBytes);
    result = lappend (result, serializeInt    (fdwState->db2Table->cols[idxCol]->colPrimKeyPart));
    db2Debug3("serialize col[%d].colPrimKeyPart: %d"  ,idxCol, fdwState->db2Table->cols[idxCol]->colPrimKeyPart);
    result = lappend (result, serializeInt    (fdwState->db2Table->cols[idxCol]->colCodepage));
    db2Debug3("serialize col[%d].colCodepage: %d"  ,idxCol, fdwState->db2Table->cols[idxCol]->colCodepage);
    result = lappend (result, serializeString (fdwState->db2Table->cols[idxCol]->pgname));
    db2Debug3("serialize col[%d].pgname: %s"  ,idxCol, fdwState->db2Table->cols[idxCol]->pgname);
    result = lappend (result, serializeInt    (fdwState->db2Table->cols[idxCol]->pgattnum));
    db2Debug3("serialize col[%d].pgattnum: %d"  ,idxCol, fdwState->db2Table->cols[idxCol]->pgattnum);
    result = lappend (result, serializeOid    (fdwState->db2Table->cols[idxCol]->pgtype));
    db2Debug3("serialize col[%d].pgtype: %d"  ,idxCol, fdwState->db2Table->cols[idxCol]->pgtype);
    result = lappend (result, serializeInt    (fdwState->db2Table->cols[idxCol]->pgtypmod));
    db2Debug3("serialize col[%d].pgtypmod: %d"  ,idxCol, fdwState->db2Table->cols[idxCol]->pgtypmod);
    result = lappend (result, serializeInt    (fdwState->db2Table->cols[idxCol]->used));
    db2Debug3("serialize col[%d].used: %d"  ,idxCol, fdwState->db2Table->cols[idxCol]->used);
    result = lappend (result, serializeInt    (fdwState->db2Table->cols[idxCol]->pkey));
    db2Debug3("serialize col[%d].pkey: %d"  ,idxCol, fdwState->db2Table->cols[idxCol]->pkey);
    result = lappend (result, serializeLong   (fdwState->db2Table->cols[idxCol]->val_size));
    db2Debug3("serialize col[%d].val_size: %ld"  ,idxCol, fdwState->db2Table->cols[idxCol]->val_size);
    result = lappend (result, serializeInt    (fdwState->db2Table->cols[idxCol]->noencerr));
    db2Debug3("serialize col[%d].noencerr: %d"  ,idxCol, fdwState->db2Table->cols[idxCol]->noencerr);
    /* don't serialize val, val_len, val_null and varno */
  }

  /* find length of parameter list */
  for (param = fdwState->paramList; param; param = param->next) {
    ++lenParam;
  }
  /* serialize length */
  result = lappend (result, serializeInt (lenParam));
  db2Debug3("serialize paramList.length: %d", lenParam);
  /* parameter list entries */
  for (param = fdwState->paramList; param; param = param->next) {
    result = lappend (result, serializeString (param->colName));
    db2Debug3("serialize param.colName: %s"  , param->colName);
    result = lappend (result, serializeInt    (param->colType));
    db2Debug3("serialize param.colType: %d"  , param->colType);
    result = lappend (result, serializeInt    (param->colSize));
    db2Debug3("serialize param.colSize: %d"  , param->colSize);
    result = lappend (result, serializeOid (param->type));
    db2Debug3("serialize param.type: %d"  , param->type);
    result = lappend (result, serializeInt ((int) param->bindType));
    db2Debug3("serialize param.bindType: %d"  , param->bindType);
    result = lappend (result, serializeLong   (param->val_size));
    db2Debug3("serialize param.val_size: %ld"  , param->val_size);
    result = lappend (result, serializeInt ((int) param->colnum));
    db2Debug3("serialize param.colnum: %d"  , param->colnum);
    result = lappend (result, serializeInt ((int) param->txts));
    db2Debug3("serialize param.txts: %d"  , param->txts);
  }

  /* find length of result list */
  lenParam = 0;
  for (rcol = fdwState->resultList; rcol; rcol = rcol->next) {
    ++lenParam;
  }
  /* serialize length */
  result = lappend (result, serializeInt (lenParam));
  db2Debug3("serialize resultList.length: %d", lenParam);
  /* parameter list entries */
  for (rcol = fdwState->resultList; rcol; rcol = rcol->next) {
    result = lappend (result, serializeString (rcol->colName));
    db2Debug3("serialize res.colName: %s"  , rcol->colName);
    result = lappend (result, serializeInt    (rcol->colType));
    db2Debug3("serialize res.colType: %d"  , rcol->colType);
    result = lappend (result, serializeInt    (rcol->colSize));
    db2Debug3("serialize res.colSize: %d"  , rcol->colSize);
    result = lappend (result, serializeInt    (rcol->colScale));
    db2Debug3("serialize res.colScale: %d" , rcol->colScale);
    result = lappend (result, serializeInt    (rcol->colNulls));
    db2Debug3("serialize res.colNulls: %d", rcol->colNulls);
    result = lappend (result, serializeInt    (rcol->colChars));
    db2Debug3("serialize res.colChars: %d" , rcol->colChars);
    result = lappend (result, serializeInt    (rcol->colBytes));
    db2Debug3("serialize res.colBytes: %d" , rcol->colBytes);
    result = lappend (result, serializeInt    (rcol->colPrimKeyPart));
    db2Debug3("serialize res.colPrimKeyPart: %d", rcol->colPrimKeyPart);
    result = lappend (result, serializeInt    (rcol->colCodepage));
    db2Debug3("serialize res.codepage: %d" , rcol->colCodepage);
    result = lappend (result, serializeString (rcol->pgname));
    db2Debug3("serialize res.pgname: %s"   , rcol->pgname);
    result = lappend (result, serializeInt    (rcol->pgattnum));
    db2Debug3("serialize res.pgattnum: %d" , rcol->pgattnum);
    result = lappend (result, serializeOid    (rcol->pgtype));
    db2Debug3("serialize res.pgtype: %d"   , rcol->pgtype);
    result = lappend (result, serializeInt    (rcol->pgtypmod));
    db2Debug3("serialize res.pgtypmod: %d" , rcol->pgtypmod);
    result = lappend (result, serializeInt    (rcol->pkey));
    db2Debug3("serialize res.pkey: %d"      , rcol->pkey);
    result = lappend (result, serializeLong   (rcol->val_size));
    db2Debug3("serialize res.val_size: %ld", rcol->val_size);
    result = lappend (result, serializeInt    (rcol->noencerr));
    db2Debug3("serialize res.noencerr: %d" , rcol->noencerr);
    result = lappend (result, serializeInt    (rcol->resnum));        // the last result is the first in the list
    db2Debug3("serialize res.resnum: %d"   , rcol->resnum);
    lenParam--;
  }

  /* don't serialize params, startup_cost, total_cost, rowcount, temp_cxt, order_clause and where_clause */
  db2Exit1(": %x",result);
  return result;
}

/** serializeString
 *   Create a Const that contains the string.
 */
static Const* serializeString (const char* s) {
  Const* result = NULL;
  db2Entry5();
  result = (s == NULL) ? makeNullConst (TEXTOID, -1, InvalidOid) 
                       : makeConst (TEXTOID, -1, InvalidOid, -1, PointerGetDatum (cstring_to_text (s)), false, false);
  db2Exit5(": %x",result);
  return result;
}

/** serializeLong
 *   Create a Const that contains the long integer.
 */
static Const* serializeLong (long i) {
  Const* result = NULL;
  db2Entry5();
  if (sizeof (long) <= 4)
    result = makeConst (INT4OID, -1, InvalidOid, 4, Int32GetDatum ((int32) i), false, true);
  else
    result = makeConst (INT4OID, -1, InvalidOid, 8, Int64GetDatum ((int64) i), false,
#ifdef USE_FLOAT8_BYVAL
      true
#else
      false
#endif /* USE_FLOAT8_BYVAL */
      );
  db2Exit5(": %x",result);
  return result;
}
