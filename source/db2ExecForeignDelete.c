#include <postgres.h>
#include <commands/explain.h>
#include <optimizer/optimizer.h>
#include <access/heapam.h>
#include "db2_fdw.h"
#include "DB2FdwState.h"

/** external variables */
extern bool     dml_in_transaction;
extern regproc* output_funcs;

/** external prototypes */
extern int             db2ExecuteQuery           (DB2Session* session, ParamDesc* paramList);
extern int          db2FetchNext              (DB2Session* session, DB2ResultColumn* resultList);
extern void         db2CloseCursor            (DB2Session* session);
extern void            db2Debug                  (int level, const char* message, ...);
extern void            convertTuple              (DB2Session* session, DB2ResultColumn* reslist, DB2TupleIndexMode index_mode, int natts, Datum* values, bool* nulls);
extern char*           deparseDate               (Datum datum);
extern char*           deparseTimestamp          (Datum datum, bool hasTimezone);

/** local prototypes */
TupleTableSlot* db2ExecForeignDelete (EState* estate, ResultRelInfo* rinfo, TupleTableSlot* slot, TupleTableSlot* planSlot);
void            setModifyParameters       (ParamDesc* paramList, TupleTableSlot* newslot, TupleTableSlot* oldslot, DB2Table* db2Table, DB2Session* session);

/* db2ExecForeignDelete
 * Set the parameter values from the slots and execute the DELETE statement.
 * Returns a slot with the results from the RETRUNING clause.
 */
TupleTableSlot* db2ExecForeignDelete (EState* estate, ResultRelInfo* rinfo, TupleTableSlot* slot, TupleTableSlot* planSlot) {
  DB2FdwState*  fdw_state = (DB2FdwState*) rinfo->ri_FdwState;
  int           rows;
  MemoryContext oldcontext;

  db2Entry1();
  db2Debug2("relid: %d", RelationGetRelid (rinfo->ri_RelationDesc));

  ++fdw_state->rowcount;
  dml_in_transaction = true;

  MemoryContextReset (fdw_state->temp_cxt);
  oldcontext = MemoryContextSwitchTo (fdw_state->temp_cxt);

  /* extract the values from the slot and store them in the parameters */
  setModifyParameters (fdw_state->paramList, slot, planSlot, fdw_state->db2Table, fdw_state->session);

  /* execute the DELETE statement */
  rows = db2ExecuteQuery (fdw_state->session, fdw_state->paramList);

  /* with a RETURNING clause the statement is a SELECT ... FROM NEW/OLD TABLE (...): fetch its row, then close the cursor for the next execution */
  if (fdw_state->resultList != NULL) {
    rows = db2FetchNext (fdw_state->session, fdw_state->resultList);
    db2CloseCursor (fdw_state->session);
  }

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
  convertTuple (fdw_state->session, fdw_state->resultList, DB2_TUPLE_INDEX_ATTRIBUTE,
                slot->tts_tupleDescriptor->natts, slot->tts_values, slot->tts_isnull);

  /* store the virtual tuple */
  ExecStoreVirtualTuple (slot);

  db2Exit1();
  return slot;
}

/* setModifyParameters
 * Set the parameter values from the values in the slots.
 * "newslot" contains the new values, "oldslot" the old ones.
 */
void setModifyParameters (ParamDesc *paramList, TupleTableSlot * newslot, TupleTableSlot * oldslot, DB2Table *db2Table, DB2Session * session) {
  ParamDesc* param = NULL;
  Datum      datum = 0;
  bool       isnull = true;

  db2Entry1();
  for (param = paramList; param != NULL; param = param->next) {
    db2Debug2("db2Table->cols[%d]->colName: %s  ",param->colnum,db2Table->cols[param->colnum]->colName);
    db2Debug2("param->bindType: %d",param->bindType);
    db2Debug2("param->colnum  : %d",param->colnum);
    db2Debug2("param->txts    : %d",param->txts);
    db2Debug2("param->type    : %d",param->type);
    db2Debug2("param->value   : %s - initial",param->value);
    db2Debug3("db2Table->cols[%d]->colPrimKeyPart: %d  ",param->colnum,db2Table->cols[param->colnum]->colPrimKeyPart);
    /* key values come from the old row's junk columns, only UPDATE and DELETE have one (oldslot is NULL for INSERT) */
    if (oldslot != NULL && db2Table->cols[param->colnum]->colPrimKeyPart != 0) {
      if (AttributeNumberIsValid(db2Table->cols[param->colnum]->pkey)) {
        db2Debug2("db2Table->cols[%d]->pkey: %d",param->colnum,db2Table->cols[param->colnum]->pkey);
        datum = ExecGetJunkAttribute (oldslot, db2Table->cols[param->colnum]->pkey, &isnull);
        db2Debug2("primaryKey value from oldslot resjunk entry: %ld",datum);
      } else {
        elog(ERROR, "no JunkAttribute Value found for key column: %s",db2Table->cols[param->colnum]->colName);
      }
      // If null go and check the normal parameter in the slot
      if (isnull) {
        /* for other parameters extract the datum from newslot */
        datum = slot_getattr (newslot, db2Table->cols[param->colnum]->pgattnum, &isnull);
        db2Debug2("parameter value from newslot: %ld",datum);
        isnull = (datum == 0);
      }
    } else {
      /* for other parameters extract the datum from newslot */
      datum = slot_getattr (newslot, db2Table->cols[param->colnum]->pgattnum, &isnull);
      db2Debug2("parameter value from newslot: %ld",datum);
    }

    switch (param->bindType) {
      case BIND_STRING:
      case BIND_NUMBER: {
        if (isnull) {
          param->value = NULL;
          db2Debug2("param->value: %s - (NULL since isnull is set)",param->value);
        } else {
          db2Debug2("db2Table->cols[%d]->pgtype: %d",param->colnum,db2Table->cols[param->colnum]->pgtype);
          /* special treatment for date, timestamps and intervals */
          switch (db2Table->cols[param->colnum]->pgtype) {
            case DATEOID: {
              param->value = deparseDate (datum);
              db2Debug2("param->value: %s - (ought to be a date)",param->value);
            }
            break;
            case TIMESTAMPOID:
            case TIMESTAMPTZOID: {
              param->value = deparseTimestamp (datum, false/*(pgtype == TIMESTAMPTZOID)*/);
              db2Debug2("param->value: %s - (ought to be a timestamp)",param->value);
            }
            break;
            case TIMEOID:
            case TIMETZOID:{
              param->value = deparseTimestamp (datum, false/*(pgtype == TIMETZOID)*/);
              db2Debug2("param->value: %s (ought to be a time)",param->value);
            }
            break;
            case BPCHAROID:
            case VARCHAROID:
            case INTERVALOID:
            case NUMERICOID: {
              /* these functions require the type modifier */
              param->value = DatumGetCString( OidFunctionCall3 (output_funcs[param->colnum], datum, ObjectIdGetDatum (InvalidOid), Int32GetDatum (db2Table->cols[param->colnum]->pgtypmod)));
              db2Debug2("param->value: %s (ought to be a BPCHAR, VARCHAR,INTERVAL or NUMERIC)",param->value);
            }
              break;
            case UUIDOID: {
              char* p = NULL;
              char* q = NULL;

              param->value = DatumGetCString (OidFunctionCall1 (output_funcs[param->colnum], datum));
              db2Debug2("param->value: %s (ought to be a UUID)",param->value);

              /* remove the minus signs for UUIDs */
              for (p = q = param->value; *p != '\0'; ++p, ++q) {
                if (*p == '-')
                  ++p;
                *q = *p;
              }
              *q = '\0';
            }
            break;
            case BOOLOID: {
              /* convert booleans to numbers */
              param->value = DatumGetCString (OidFunctionCall1 (output_funcs[param->colnum], datum));
              db2Debug2("param->value: %s (ought to be a boolean)",param->value);
              param->value[0] = (param->value[0] == 't') ? '1' : '0';
              param->value[1] = '\0';
            }
            break;
            default: {
              /* the others don't */
              /* convert the parameter value into a string */
              param->value = DatumGetCString (OidFunctionCall1 (output_funcs[param->colnum], datum));
              db2Debug2("param->value: %s (ought to be a string)",param->value);
            }
            break;
          }
        }
      }
      break;
      case BIND_LONG:
      case BIND_LONGRAW: {
        if (isnull) {
          param->value = NULL;
          db2Debug2("param->value: %s - (NULL since isnull is set)",param->value);
        } else {
          int32 value_len = 0;

          /* detoast it if necessary */
          datum = (Datum) PG_DETOAST_DATUM (datum);
          /* the first 4 bytes contain the length */
          value_len = VARSIZE (datum) - VARHDRSZ;
          /* one extra (zeroed) byte terminates BIND_LONG values, which are bound with SQL_NTS */
          param->value = db2alloc(value_len + 1,"param->value");
          memcpy (param->value, VARDATA(datum), value_len);
          param->value_len = value_len;
          db2Debug2("param->value: %s (ought to be a LONG or LONGRAW)",param->value);
        }
      }
      break;
      default:
        db2Debug2("unknown BIND_TYPE: %d", param->bindType);
      break;
    }
    db2Debug2("param->value   : %s - finally",param->value);
  }
  db2Exit1();
}
