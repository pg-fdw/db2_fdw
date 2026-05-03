#include <postgres.h>
#include <commands/explain.h>
#include <nodes/pathnodes.h>
#include <optimizer/optimizer.h>
#include <access/heapam.h>
#include "db2_fdw.h"
#include "DB2FdwState.h"

/** external variables */
extern bool     dml_in_transaction;
extern regproc* output_funcs;

/** external prototypes */
extern int             db2ExecuteQuery           (DB2Session* session, const DB2Table* db2Table, ParamDesc* paramList);
extern void            db2Debug1                 (const char* message, ...);
extern void            db2Debug2                 (const char* message, ...);
extern void            convertTuple              (DB2FdwState* fdw_state, Datum* values, bool* nulls) ;
extern char*           deparseDate               (Datum datum);
extern char*           deparseTimestamp          (Datum datum, bool hasTimezone);
extern void*           db2alloc                  (const char* type, size_t size);

/** local prototypes */
TupleTableSlot* db2ExecForeignDelete (EState* estate, ResultRelInfo* rinfo, TupleTableSlot* slot, TupleTableSlot* planSlot);
void            setModifyParameters       (ParamDesc* paramList, TupleTableSlot* newslot, TupleTableSlot* oldslot, DB2Table* db2Table, DB2Session* session);

/** db2ExecForeignDelete
 *   Set the parameter values from the slots and execute the DELETE statement.
 *   Returns a slot with the results from the RETRUNING clause.
 */
TupleTableSlot* db2ExecForeignDelete (EState* estate, ResultRelInfo* rinfo, TupleTableSlot* slot, TupleTableSlot* planSlot) {
  DB2FdwState*  fdw_state = (DB2FdwState*) rinfo->ri_FdwState;
  int           rows;
  MemoryContext oldcontext;

  db2Debug1("> db2ExecForeignDelete");
  db2Debug2("  relid: %d", RelationGetRelid (rinfo->ri_RelationDesc));

  ++fdw_state->rowcount;
  dml_in_transaction = true;

  MemoryContextReset (fdw_state->temp_cxt);
  oldcontext = MemoryContextSwitchTo (fdw_state->temp_cxt);

  /* extract the values from the slot and store them in the parameters */
  setModifyParameters (fdw_state->paramList, slot, planSlot, fdw_state->db2Table, fdw_state->session);

  /* execute the DELETE statement and store RETURNING values in db2Table's columns */
  rows = db2ExecuteQuery (fdw_state->session, fdw_state->db2Table, fdw_state->paramList);

  if (rows != 1)
    ereport ( ERROR
            , ( errcode (ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION)
              , errmsg ("DELETE on DB2 table removed %d rows instead of one in iteration %lu", rows, fdw_state->rowcount)
              , errhint ("This probably means that you did not set the \"key\" option on all primary key columns.")
              )
            );

  MemoryContextSwitchTo (oldcontext);

  /* empty the result slot */
  ExecClearTuple (slot);

  /* convert result for RETURNING to arrays of values and null indicators */
  convertTuple (fdw_state, slot->tts_values, slot->tts_isnull);

  /* store the virtual tuple */
  ExecStoreVirtualTuple (slot);

  db2Debug1("< db2ExecForeignDelete");
  return slot;
}

/* setModifyParameters
 * Set the parameter values from the values in the slots.
 * "newslot" contains the new values, "oldslot" the old ones.
 */
void setModifyParameters (ParamDesc *paramList, TupleTableSlot * newslot, TupleTableSlot * oldslot, DB2Table *db2Table, DB2Session * session) {
  ParamDesc *param;
  Datum datum;
  bool isnull;
  int32 value_len;
  char *p, *q;
  Oid pgtype;
  db2Debug1("> setModifyParameters");
  for (param = paramList; param != NULL; param = param->next) {
    db2Debug2("  db2Table->cols[%d]->colName: %s  ",param->colnum,db2Table->cols[param->colnum]->colName);
    db2Debug2("  param->bindType: %d  ",param->bindType);
    db2Debug2("  param->colnum  : %d  ",param->colnum);
    db2Debug2("  param->txts    : %d  ",param->txts);
    db2Debug2("  param->type    : %d  ",param->type);
    db2Debug2("  param->value   : '%s'",param->value);
    /* don't do anything for output parameters */
    if (param->bindType == BIND_OUTPUT)
      continue;

    if (db2Table->cols[param->colnum]->colPrimKeyPart != 0) {
      /* for primary key parameters extract the resjunk entry */
      datum = ExecGetJunkAttribute (oldslot, db2Table->cols[param->colnum]->pkey, &isnull);
    }
    else {
      /* for other parameters extract the datum from newslot */
      datum = slot_getattr (newslot, db2Table->cols[param->colnum]->pgattnum, &isnull);
    }

    switch (param->bindType) {
      case BIND_STRING:
      case BIND_NUMBER:
        if (isnull) {
          param->value = NULL;
          break;
        }
        pgtype = db2Table->cols[param->colnum]->pgtype;
        db2Debug2("  db2Table->cols[%d]->pgtype: %d",param->colnum,db2Table->cols[param->colnum]->pgtype);
        /* special treatment for date, timestamps and intervals */
        if (pgtype == DATEOID) {
          param->value = deparseDate (datum);
          break;      /* from switch (param->bindType) */
        } else if (pgtype == TIMESTAMPOID || pgtype == TIMESTAMPTZOID) {
          param->value = deparseTimestamp (datum, false/*(pgtype == TIMESTAMPTZOID)*/);
          break;      /* from switch (param->bindType) */
        } else if (pgtype == TIMEOID || pgtype == TIMETZOID) {
          param->value = deparseTimestamp (datum, false/*(pgtype == TIMETZOID)*/);
          break;      /* from switch (param->bindType) */
        }
        /* convert the parameter value into a string */
        param->value = DatumGetCString (OidFunctionCall1 (output_funcs[param->colnum], datum));
        db2Debug2("  param->value: %s",param->value);
        /* some data types need additional processing */
        switch (db2Table->cols[param->colnum]->pgtype) {
          case UUIDOID:
            /* remove the minus signs for UUIDs */
            for (p = q = param->value; *p != '\0'; ++p, ++q) {
              if (*p == '-')
                ++p;
              *q = *p;
            }
            *q = '\0';
          break;
          case BOOLOID:
            /* convert booleans to numbers */
            if (param->value[0] == 't')
              param->value[0] = '1';
            else
              param->value[0] = '0';
            param->value[1] = '\0';
          break;
        }
      break;
      case BIND_LONG:
      case BIND_LONGRAW:
        if (isnull) {
          param->value = NULL;
          break;
        }
        /* detoast it if necessary */
        datum = (Datum) PG_DETOAST_DATUM (datum);
        /* the first 4 bytes contain the length */
        value_len = VARSIZE (datum) - VARHDRSZ;
        param->value = db2alloc("param->value", value_len);
        memcpy (param->value, VARDATA(datum), value_len);
      break;
      case BIND_OUTPUT:
      break;
    }
    db2Debug2("  param->value   : '%s'",param->value);
  }
  db2Debug1("< setModifyParameters");
}
