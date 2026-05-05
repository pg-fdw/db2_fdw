#include <postgres.h>
#include <commands/explain.h>
#include <commands/vacuum.h>
#include <utils/builtins.h>
#include <utils/syscache.h>
#include <nodes/pathnodes.h>
#include <optimizer/optimizer.h>
#include <access/heapam.h>
#include "db2_fdw.h"
#include "DB2FdwState.h"

/** external variables */

/** external prototypes */
extern void         db2PrepareQuery            (DB2Session* session, const char* query, DB2Table* db2Table, unsigned long prefetch);
extern void         db2Debug1                  (const char* message, ...);
extern void         db2Debug2                  (const char* message, ...);
extern void*        db2alloc                   (const char* type, size_t size);
extern char*        c2name                     (short fcType);
extern void         db2BeginForeignModifyCommon(ModifyTableState* mtstate, ResultRelInfo* rinfo, DB2FdwState* fdw_state, Plan* subplan);

/** local prototypes */
void         db2BeginForeignModify(ModifyTableState* mtstate, ResultRelInfo* rinfo, List* fdw_private, int subplan_index, int eflags);
DB2FdwState* deserializePlanData  (List* list);
char*        deserializeString    (Const* constant);
long         deserializeLong      (Const* constant);

/** db2BeginForeignModify
 *   Prepare everything for the DML query:
 *   The SQL statement is prepared, the type output functions for
 *   the parameters are fetched, and the column numbers of the
 *   resjunk attributes are stored in the "pkey" field.
 */
void db2BeginForeignModify (ModifyTableState * mtstate, ResultRelInfo * rinfo, List * fdw_private, int subplan_index, int eflags) {
  DB2FdwState* fdw_state = deserializePlanData (fdw_private);
  Plan        *subplan   = NULL;

  db2Debug1("> db2BeginForeignModify");
  db2Debug2("  relid: %d", RelationGetRelid (rinfo->ri_RelationDesc));
  #if PG_VERSION_NUM < 140000
  subplan   = mtstate->mt_plans[subplan_index]->plan;
  #else
  subplan   = outerPlanState(mtstate)->plan;
  #endif

  db2BeginForeignModifyCommon(mtstate, rinfo, fdw_state, subplan);
}

/** deserializePlanData
 *   Extract the data structures from a List created by serializePlanData.
 */
DB2FdwState* deserializePlanData (List* list) {
  DB2FdwState* state = db2alloc ("DB2FdwState", sizeof (DB2FdwState));
  ListCell*    cell  = list_head (list);
  int          i, 
               len;
  ParamDesc*   param;

  db2Debug1("> deserializePlanData");
  /* session will be set upon connect */
  state->session      = NULL;
  /* these fields are not needed during execution */
  state->startup_cost = 0;
  state->total_cost   = 0;
  /* these are not serialized */
  state->rowcount     = 0;
  state->columnindex  = 0;
  state->params       = NULL;
  state->temp_cxt     = NULL;
  state->order_clause = NULL;

  /* dbserver */
  state->dbserver = deserializeString (lfirst (cell));
  cell = list_next (list,cell);

  /* user */
  state->user = deserializeString (lfirst (cell));
  cell = list_next (list,cell);

  /* password */
  state->password = deserializeString (lfirst (cell));
  cell = list_next (list,cell);

  /* jwt_token */
  state->jwt_token = deserializeString (lfirst (cell));
  cell = list_next (list,cell);

  /* nls_lang */
  state->nls_lang = deserializeString (lfirst (cell));
  cell = list_next (list,cell);

  /* query */
  state->query = deserializeString (lfirst (cell));
  cell = list_next (list,cell);

  /* DB2 prefetch count */
  state->prefetch = (unsigned long) DatumGetInt32 (((Const *) lfirst (cell))->constvalue);
  cell = list_next (list,cell);

  /* table data */
  state->db2Table = (DB2Table*) db2alloc ("state->db2Table", sizeof (struct db2Table));
  state->db2Table->name = deserializeString (lfirst (cell));
  db2Debug2("  state->db2Table->name: '%s'",state->db2Table->name);
  cell = list_next (list,cell);
  state->db2Table->pgname = deserializeString (lfirst (cell));
  db2Debug2("  state->db2Table->pgname: '%s'",state->db2Table->pgname);
  cell = list_next (list,cell);
  state->db2Table->batchsz = (int) DatumGetInt32 (((Const*) lfirst (cell))->constvalue);
  db2Debug2("  state->db2Table->batchsz: %d",state->db2Table->batchsz);
  cell = list_next (list,cell);
  state->db2Table->ncols = (int) DatumGetInt32 (((Const*) lfirst (cell))->constvalue);
  db2Debug2("  state->db2Table->ncols: %d",state->db2Table->ncols);
  cell = list_next (list,cell);
  state->db2Table->npgcols = (int) DatumGetInt32 (((Const*) lfirst (cell))->constvalue);
  db2Debug2("  state->db2Table->npgcols: %d",state->db2Table->npgcols);
  cell = list_next (list,cell);
  state->db2Table->cols = (DB2Column**) db2alloc ("state->db2Table->cols", sizeof (DB2Column*) * state->db2Table->ncols);

  /* loop columns */
  for (i = 0; i < state->db2Table->ncols; ++i) {
    state->db2Table->cols[i]           = (DB2Column *) db2alloc ("state->db2Table->cols[i]", sizeof (DB2Column));
    state->db2Table->cols[i]->colName  = deserializeString (lfirst (cell));
    db2Debug2("  state->db2Table->cols[%d]->colName: '%s'",i,state->db2Table->cols[i]->colName);
    cell = list_next (list,cell);
    state->db2Table->cols[i]->colType  = (short) DatumGetInt32 (((Const*) lfirst (cell))->constvalue);
    db2Debug2("  state->db2Table->cols[%d]->colType: %d (%s)",i,state->db2Table->cols[i]->colType,c2name(state->db2Table->cols[i]->colType));
    cell = list_next (list,cell);
    state->db2Table->cols[i]->colSize  = (size_t) DatumGetInt32 (((Const*) lfirst (cell))->constvalue);
    db2Debug2("  state->db2Table->cols[%d]->colSize: %lld",i,state->db2Table->cols[i]->colSize);
    cell = list_next (list,cell);
    state->db2Table->cols[i]->colScale = (short) DatumGetInt32 (((Const*) lfirst (cell))->constvalue);
    db2Debug2("  state->db2Table->cols[%d]->colScale: %d",i,state->db2Table->cols[i]->colScale);
    cell = list_next (list,cell);
    state->db2Table->cols[i]->colNulls = (short) DatumGetInt32 (((Const*) lfirst (cell))->constvalue);
    db2Debug2("  state->db2Table->cols[%d]->colNulls: %d",i,state->db2Table->cols[i]->colNulls);
    cell = list_next (list,cell);
    state->db2Table->cols[i]->colChars = (size_t) DatumGetInt32 (((Const*) lfirst (cell))->constvalue);
    db2Debug2("  state->db2Table->cols[%lld]->colChars: %lld",i,state->db2Table->cols[i]->colChars);
    cell = list_next (list,cell);
    state->db2Table->cols[i]->colBytes = (size_t) DatumGetInt32 (((Const*) lfirst (cell))->constvalue);
    db2Debug2("  state->db2Table->cols[%lld]->colBytes: %lld",i,state->db2Table->cols[i]->colBytes);
    cell = list_next (list,cell);
    state->db2Table->cols[i]->colPrimKeyPart = (size_t) DatumGetInt32 (((Const*) lfirst (cell))->constvalue);
    db2Debug2("  state->db2Table->cols[%lld]->colPrimKeyPart: %lld",i,state->db2Table->cols[i]->colPrimKeyPart);
    cell = list_next (list,cell);
    state->db2Table->cols[i]->colCodepage = (size_t) DatumGetInt32 (((Const*) lfirst (cell))->constvalue);
    db2Debug2("  state->db2Table->cols[%lld]->colCodepaget: %lld",i,state->db2Table->cols[i]->colCodepage);
    cell = list_next (list,cell);
    state->db2Table->cols[i]->pgname   = deserializeString (lfirst (cell));
    db2Debug2("  state->db2Table->cols[%d]->pgname: '%s'",i,state->db2Table->cols[i]->pgname);
    cell = list_next (list,cell);
    state->db2Table->cols[i]->pgattnum = (int) DatumGetInt32 (((Const*) lfirst (cell))->constvalue);
    db2Debug2("  state->db2Table->cols[%d]->pgattnum: %d",i,state->db2Table->cols[i]->pgattnum);
    cell = list_next (list,cell);
    state->db2Table->cols[i]->pgtype   = DatumGetObjectId (((Const*) lfirst (cell))->constvalue);
    db2Debug2("  state->db2Table->cols[%d]->pgtype: %d",i,state->db2Table->cols[i]->pgtype);
    cell = list_next (list,cell);
    state->db2Table->cols[i]->pgtypmod = (int) DatumGetInt32 (((Const*) lfirst (cell))->constvalue);
    db2Debug2("  state->db2Table->cols[%d]->pgtypmod: %d",i,state->db2Table->cols[i]->pgtypmod);
    cell = list_next (list,cell);
    state->db2Table->cols[i]->used     = (int) DatumGetInt32 (((Const*) lfirst (cell))->constvalue);
    db2Debug2("  state->db2Table->cols[%d]->used: %d",i,state->db2Table->cols[i]->used);
    cell = list_next (list,cell);
    state->db2Table->cols[i]->pkey     = (int) DatumGetInt32 (((Const*) lfirst (cell))->constvalue);
    db2Debug2("  state->db2Table->cols[%d]->pkey: %d",i,state->db2Table->cols[i]->pkey);
    cell = list_next (list,cell);
    state->db2Table->cols[i]->val_size = deserializeLong (lfirst (cell));
    db2Debug2("  state->db2Table->cols[%d]->val_size: %ld",i,state->db2Table->cols[i]->val_size);
    cell = list_next (list,cell);
    state->db2Table->cols[i]->noencerr = deserializeLong (lfirst (cell));
    db2Debug2("  state->db2Table->cols[%d]->noencerr: %d",i,state->db2Table->cols[i]->noencerr);
    cell = list_next (list,cell);
    /* allocate memory for the result value only when the column is used in query */
    state->db2Table->cols[i]->val      = (state->db2Table->cols[i]->used == 1) ? (char*) db2alloc ("state->db2Table->cols[i]->val", MIN(state->db2Table->cols[i]->val_size, 1073741823)) : NULL;
    db2Debug2("  state->db2Table->cols[%d]->val: %x",i,state->db2Table->cols[i]->val);
    state->db2Table->cols[i]->val_len  = 0;
    db2Debug2("  state->db2Table->cols[%d]->val_len: %d",i,state->db2Table->cols[i]->val_len);
    state->db2Table->cols[i]->val_null = 1;
    db2Debug2("  state->db2Table->cols[%d]->val_null: %d",i,state->db2Table->cols[i]->val_null);
  }

  /* length of parameter list */
  len  = (int) DatumGetInt32 (((Const*) lfirst (cell))->constvalue);
  cell = list_next (list,cell);

  /* parameter table entries */
  state->paramList = NULL;
  for (i = 0; i < len; ++i) {
    param            = (ParamDesc*) db2alloc ("state->parmList->next", sizeof (ParamDesc));
    param->type      = DatumGetObjectId (((Const *) lfirst (cell))->constvalue);
    cell             = list_next (list,cell);
    param->bindType  = (db2BindType) DatumGetInt32 (((Const *) lfirst (cell))->constvalue);
    cell             = list_next (list,cell);
    if (param->bindType == BIND_OUTPUT)
      param->value   = (void *) 42;	/* something != NULL */
    else
      param->value   = NULL;
    param->node      = NULL;
    param->colnum    = (int) DatumGetInt32 (((Const *) lfirst (cell))->constvalue);
    cell             = list_next (list,cell);
    param->txts      = (int) DatumGetInt32 (((Const *) lfirst (cell))->constvalue);
    cell             = list_next (list,cell);
    param->next      = state->paramList;
    state->paramList = param;
  }

  db2Debug1("< deserializePlanData - returns: %x", state);
  return state;
}

/** deserializeString
 *   Extracts a string from a Const, returns a deep copy.
 */
char* deserializeString (Const* constant) {
  char* result = NULL;
  db2Debug1("> deserializeString");
  if (constant->constisnull)
    result = NULL;
  else
    result = text_to_cstring (DatumGetTextP (constant->constvalue));
  db2Debug1("< deserializeString: '%s'", result);
  return result;
}

/** deserializeLong
 *   Extracts a long integer from a Const.
 */
long deserializeLong (Const* constant) {
  long result = 0L;
  db2Debug1("> deserializeLong");
  result =  (sizeof (long) <= 4) ? (long) DatumGetInt32 (constant->constvalue)
                                 : (long) DatumGetInt64 (constant->constvalue);
  db2Debug1("< deserializeLong - returns: %ld", result);
  return result;
}
