#include <string.h>
#include "db2_fdw.h"
#include "DB2ResultColumn.h"

/** global variables */

/** external variables */
extern char         db2Message[ERRBUFSIZE];/* contains DB2 error messages, set by db2CheckErr()             */

/** external prototypes */
extern SQLRETURN    db2CheckErr          (SQLRETURN status, SQLHANDLE handle, SQLSMALLINT handleType, int line, char* file);
extern void         db2Error_d           (db2error sqlstate, const char* message, const char* detail, ...);

/** internal prototypes */
void                db2GetLob            (DB2Session* session, DB2ResultColumn* column, char** value, long* value_len);

/* db2GetLob
 * Get the LOB contents and store them in *value and *value_len.
 */
void db2GetLob (DB2Session* session, DB2ResultColumn* column, char** value, long* value_len) {
  SQLRETURN      rc  = SQL_SUCCESS;
  SQLLEN         ind = 0;
  SQLCHAR        buf[LOB_CHUNK_SIZE+1];
  SQLSMALLINT    fcType = (column->colType == DB2_CLOB) ? SQL_C_CHAR : SQL_C_BINARY;
  int            extend = 0;
  db2Entry1();
  db2Debug2("column->colName: '%s'",column->colName);
  db2Debug2("column->resnum :  %d ",column->resnum);
  /* initialize result buffer length */
  *value_len = 0;
  /* read the LOB in chunks */
  do {
    db2Debug2("value_len: %ld",*value_len);
    db2Debug2("reading %d byte chunck of data",sizeof(buf));
    rc = SQLGetData(session->stmtp->hsql, column->resnum, fcType, buf, sizeof(buf), &ind);
    rc = db2CheckErr(rc,session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
    if (rc == SQL_ERROR) {
      db2Error_d ( FDW_UNABLE_TO_CREATE_EXECUTION, "error fetching result: SQLGetData failed to read LOB chunk", db2Message);
    }
    if (rc != 100) {
      switch(ind) {
        case SQL_NULL_DATA:
          db2Debug3("data length is null (SQL_NULL_DATA)");
          extend = 0;
        break;
        case SQL_NO_TOTAL:
          db2Debug3("undefined data length (SQL_NO_TOTAL)");
          extend = LOB_CHUNK_SIZE;
        break;
        default:
          db2Debug3("bytes still remaining: %d", ind);
          extend = (ind < LOB_CHUNK_SIZE) ? ind : LOB_CHUNK_SIZE;
        break;
      }
      /* extend result buffer by ind */
      db2Debug2("value_len: %ld", *value_len);
      db2Debug2("extend   : %d", extend);
      if (*value_len == 0) {
        if (extend > 0) {
          *value = db2alloc (*value_len + extend + 1,"*value");
        } else {
          *value = NULL;
          db2Debug3("not allocating space since the LOB value is apparently NULL");
        }
      } else {
        // do not add another 0 termination byte, since we already have one
        *value = db2realloc (*value_len + extend, *value, "*value");
      }
      // append the buffer read to the value excluding 0 termination byte
      db2Debug2("*value    : %x", *value);
      db2Debug2("*value_len: %x", *value_len);
      if (*value != NULL) {
        db2Debug3("memcpy(%x,%x,%d)",*value+*value_len,buf,extend);
        memcpy(*value + *value_len, buf, extend);
        /* update LOB length */
        *value_len += extend;
      } else {
        db2Debug3("skipping value copy, since value is NULL");
      }
    }
  } while (rc == SQL_SUCCESS_WITH_INFO);

  /* string end for CLOBs */
  db2Debug2("*value   : %x" , *value);
  db2Debug2("value_len: %ld", *value_len);
  if (*value != NULL) {
    (*value)[*value_len] = '\0';
    db2Debug2("strlen of lob: %ld", strlen(*value));
  } else {
    db2Debug2("strlen of lob: 0 since *value is NULL");
  }
  db2Exit1();
}
