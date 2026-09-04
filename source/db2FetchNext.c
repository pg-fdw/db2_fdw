#include <string.h>
#include "db2_fdw.h"
#include "DB2ResultColumn.h"

/** global variables  */

/** external variables */
extern char         db2Message[ERRBUFSIZE];/* contains DB2 error messages, set by db2CheckErr()             */
extern int          err_code;              /* error code, set by db2CheckErr()                              */

/** external prototypes */
extern SQLRETURN db2CheckErr          (SQLRETURN status, SQLHANDLE handle, SQLSMALLINT handleType, int line, char* file);

/** local prototypes */
int db2FetchNext (DB2Session* session, DB2ResultColumn* resultList);

/* db2FetchNext
 * Fetch the next result row, return 1 if there is one, else 0.
 */
int db2FetchNext (DB2Session* session, DB2ResultColumn* resultList) {
  SQLRETURN rc = 0;
  DB2ResultColumn* res = NULL;
  DB2ResultColumn* scan = NULL;
  int max_resnum = 0;
  int i = 0;
  db2Entry1();
  /* make sure there is a statement handle stored in "session" */
  if (session->stmtp == NULL) {
    db2Error (FDW_ERROR, "db2FetchNext internal error: statement handle is NULL");
  }

  /* Reset both the portable value and the driver-owned SQLLEN storage. */
  for (res = resultList; res; res = res->next) {
    res->val_null = 0;
    res->val_len = 0;
    memset(&res->val_indicator, 0, sizeof(res->val_indicator));
    if (res->val != NULL && res->val_size > 0) {
      res->val[0] = '\0';
    }
  }

  /* fetch the next result row */
  rc = SQLFetch(session->stmtp->hsql);
  rc = db2CheckErr(rc, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
  if (rc != SQL_SUCCESS && rc != SQL_NO_DATA) {
    db2Error_d (err_code == 8177 ? FDW_SERIALIZATION_FAILURE : FDW_UNABLE_TO_CREATE_EXECUTION, "error fetching result: SQLFetch failed to fetch next result row", db2Message);
  }

  /* Copy bound SQLLEN indicators out of the ABI-neutral storage. */
  if (rc == SQL_SUCCESS) {
    for (res = resultList; res; res = res->next) {
      SQLLEN indicator = 0;
      int uses_getdata = (res->colType == SQL_DECIMAL || res->colType == SQL_NUMERIC || res->colType == SQL_DECFLOAT);

      if (uses_getdata)
        continue;
      if (sizeof(indicator) > sizeof(res->val_indicator.bytes))
        db2Error(FDW_ERROR, "db2FetchNext internal error: SQLLEN does not fit into result indicator storage");
      memcpy(&indicator, res->val_indicator.bytes, sizeof(indicator));
      res->val_null = (intptr_t) indicator;
    }
  }

  /* Fetch only the deliberately unbound numeric result columns via SQLGetData. */
  if (rc == SQL_SUCCESS && resultList) {
    /*
     * Some DB2 CLI / ODBC driver setups require SQLGetData calls to be made in
     * strict ascending column order. Our internal result column list is not
     * guaranteed to be ordered by resnum, so enforce ordering here.
     */
    for (scan = resultList; scan; scan = scan->next) {
      if (scan->resnum > max_resnum)
        max_resnum = scan->resnum;
    }

    for (i = 1; i <= max_resnum; i++) {
      SQLLEN ind = 0;
      SQLRETURN get_rc_raw;
      SQLRETURN get_rc;
      int want_getdata = 0;

      res = NULL;
      for (scan = resultList; scan; scan = scan->next) {
        if (scan->resnum == i) {
          res = scan;
          break;
        }
      }
      if (res == NULL) {
        continue;
      }

      want_getdata = (res->colType == SQL_DECIMAL || res->colType == SQL_NUMERIC || res->colType == SQL_DECFLOAT);

      if (!want_getdata)
        continue;

      if (res->val == NULL || res->val_size == 0) {
        db2Error (FDW_ERROR, "db2FetchNext internal error: result column buffer is NULL");
      }

      /*
       * Some DB2 CLI/ODBC drivers return fixed-width character data without
       * writing a NUL terminator when using SQLGetData(SQL_C_CHAR). Ensure the
       * buffer is pre-zeroed so the string is always terminated even if the
       * driver only overwrites the payload bytes.
       */
      memset(res->val, 0, res->val_size);

      get_rc_raw = SQLGetData(session->stmtp->hsql, (SQLUSMALLINT) res->resnum,
                              SQL_C_CHAR, res->val, (SQLLEN) res->val_size, &ind);
      get_rc = db2CheckErr(get_rc_raw, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
      if (get_rc != SQL_SUCCESS && get_rc != SQL_NO_DATA) {
        db2Error_d (err_code == 8177 ? FDW_SERIALIZATION_FAILURE : FDW_UNABLE_TO_CREATE_EXECUTION, "error fetching result: SQLGetData failed to fetch column", db2Message);
      }

      if (ind == SQL_NULL_DATA) {
        res->val_null = (intptr_t) SQL_NULL_DATA;
        res->val_len = 0;
        continue;
      }
      if (ind == SQL_NO_TOTAL) {
        /* Best-effort: treat as a C string. */
        res->val[res->val_size - 1] = '\0';
        res->val_len = strlen(res->val);
        res->val_null = (intptr_t) res->val_len;
        continue;
      }

      /* If we got truncation info, grow buffer once and retry. */
      if (get_rc_raw == SQL_SUCCESS_WITH_INFO && ind >= (SQLLEN) res->val_size) {
        size_t needed = (size_t) ind + 1;
        res->val = (char*) db2realloc(needed, res->val, "res->val");
        res->val_size = needed;
        ind = 0;

        memset(res->val, 0, res->val_size);
        get_rc_raw = SQLGetData(session->stmtp->hsql, (SQLUSMALLINT) res->resnum,
                                SQL_C_CHAR, res->val, (SQLLEN) res->val_size, &ind);
        get_rc = db2CheckErr(get_rc_raw, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
        if (get_rc != SQL_SUCCESS && get_rc != SQL_NO_DATA) {
          db2Error_d (err_code == 8177 ? FDW_SERIALIZATION_FAILURE : FDW_UNABLE_TO_CREATE_EXECUTION, "error fetching result: SQLGetData failed to fetch column", db2Message);
        }
        if (ind == SQL_NULL_DATA) {
          res->val_null = (intptr_t) SQL_NULL_DATA;
          res->val_len = 0;
          continue;
        }
        if (ind == SQL_NO_TOTAL) {
          res->val[res->val_size - 1] = '\0';
          res->val_len = strlen(res->val);
          res->val_null = (intptr_t) res->val_len;
          continue;
        }
      }

      res->val_null = (intptr_t) ind;
      res->val_len = (size_t) ind;
      if (res->val_len >= res->val_size) {
        res->val_len = res->val_size - 1;
      }
      res->val[res->val_len] = '\0';
    }
  }

  /* Normalize the already copied indicator and terminate bound text buffers. */
  if (rc == SQL_SUCCESS && resultList) {
    for (res = resultList; res; res = res->next) {
      if (res->val_null == (intptr_t) SQL_NULL_DATA) {
        res->val_null = (intptr_t) SQL_NULL_DATA;
        res->val_len = 0;
      } else if (res->val_null >= 0 && res->val != NULL && res->val_size > 0) {
        /*
         * SQLBindCol reports the payload length through its indicator.  Preserve
         * that bounded length instead of forcing convertTuple() to search for a
         * terminator with strlen().
         */
        res->val_len = (size_t) res->val_null;
        if (res->val_len >= res->val_size)
          res->val_len = res->val_size - 1;
        res->val[res->val_len] = '\0';
      }
    }
  }

  db2Exit1(": %d",(rc == SQL_SUCCESS));
  return (rc == SQL_SUCCESS);
}
