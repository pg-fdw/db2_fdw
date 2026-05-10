#include <string.h>
#include "db2_fdw.h"
#include "DB2ResultColumn.h"

/** global variables  */

/** external variables */
extern char         db2Message[ERRBUFSIZE];/* contains DB2 error messages, set by db2CheckErr()             */
extern int          err_code;              /* error code, set by db2CheckErr()                              */

/** external prototypes */
extern void      db2Error             (db2error sqlstate, const char* message);
extern void      db2Error_d           (db2error sqlstate, const char* message, const char* detail, ...);
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
  SQLULEN retrieve_data = SQL_RD_ON;
  SQLRETURN attr_rc = SQL_SUCCESS;
  int was_rd_off = 0;
  db2Entry1();
  /* make sure there is a statement handle stored in "session" */
  if (session->stmtp == NULL) {
    db2Error (FDW_ERROR, "db2FetchNext internal error: statement handle is NULL");
  }
  /* fetch the next result row */
  rc = SQLFetch (session->stmtp->hsql);
  rc = db2CheckErr(rc, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
  if (rc != SQL_SUCCESS && rc != SQL_NO_DATA) {
    db2Error_d (err_code == 8177 ? FDW_SERIALIZATION_FAILURE : FDW_UNABLE_TO_CREATE_EXECUTION, "error fetching result: SQLFetch failed to fetch next result row", db2Message);
  }

  /* Determine whether SQLFetch retrieved data into bound columns. */
  attr_rc = SQLGetStmtAttr(session->stmtp->hsql, SQL_ATTR_RETRIEVE_DATA, &retrieve_data, 0, NULL);
  if (!SQL_SUCCEEDED(attr_rc))
    retrieve_data = SQL_RD_ON;

  was_rd_off = (retrieve_data == SQL_RD_OFF);

  /*
   * DB2 CLI note:
   * Some driver setups behave as if SQL_RD_OFF disables *all* retrieval for the
   * current row (including SQLGetData). To avoid SQLFetch-time conversion while
   * still allowing SQLGetData, keep SQL_RD_OFF during SQLFetch, then temporarily
   * switch to SQL_RD_ON before calling SQLGetData.
   */
  if (rc == SQL_SUCCESS && retrieve_data == SQL_RD_OFF) {
    SQLRETURN set_rc;
    set_rc = SQLSetStmtAttr(session->stmtp->hsql, SQL_ATTR_RETRIEVE_DATA, (SQLPOINTER) SQL_RD_ON, 0);
    if (!SQL_SUCCEEDED(set_rc))
      retrieve_data = SQL_RD_ON;
  }

  /*
   * If SQL_ATTR_RETRIEVE_DATA is SQL_RD_OFF, SQLFetch did not retrieve into any
   * bound columns. Fetch values for all (non-LOB) result columns via SQLGetData.
   *
   * Otherwise, only fetch DECIMAL/NUMERIC/DECFLOAT via SQLGetData.
   */
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
        if (retrieve_data == SQL_RD_OFF) {
          db2Error (FDW_ERROR, "db2FetchNext internal error: missing result column for resnum");
        }
        continue;
      }

      if (retrieve_data == SQL_RD_OFF) {
        /* Skip LOBs here; convertTuple/db2GetLob will fetch them separately. */
        if (res->colType == SQL_BLOB || res->colType == SQL_CLOB)
          continue;
        want_getdata = 1;
      } else {
        want_getdata = (res->colType == SQL_DECIMAL || res->colType == SQL_NUMERIC || res->colType == SQL_DECFLOAT);
      }

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

  /*
   * Defensive normalization for NULL indicators in SQL_RD_ON mode.
   *
   * We store the indicator in an intptr_t (DB2ResultColumn.val_null) and cast it
   * to SQLLEN* when binding. Some driver/toolchain combinations appear to write
   * a 32-bit SQL_NULL_DATA (-1) into the lower 4 bytes only, leaving stale high
   * bits. That can turn a NULL into a large positive value, which later code
   * interprets as NOT NULL and may try to parse an empty buffer (e.g. timestamp
   * "" -> invalid input syntax).
   */
  if (rc == SQL_SUCCESS && resultList && retrieve_data != SQL_RD_OFF) {
    for (res = resultList; res; res = res->next) {
      if ((int32_t) res->val_null == (int32_t) SQL_NULL_DATA) {
        res->val_null = (intptr_t) SQL_NULL_DATA;
        res->val_len = 0;
      }
    }
  }

  /*
   * If this fetch started in SQL_RD_OFF mode, we temporarily switched to
   * SQL_RD_ON to allow SQLGetData. Restore SQL_RD_OFF so subsequent SQLFetch
   * calls continue to avoid fetch-time conversion and we keep refreshing all
   * columns per-row via SQLGetData.
   */
  if (rc == SQL_SUCCESS && was_rd_off) {
    SQLRETURN set_back_rc;
    set_back_rc = SQLSetStmtAttr(session->stmtp->hsql, SQL_ATTR_RETRIEVE_DATA, (SQLPOINTER) SQL_RD_OFF, 0);
    set_back_rc = db2CheckErr(set_back_rc, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
    if (set_back_rc != SQL_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error fetching result: failed to restore SQL_ATTR_RETRIEVE_DATA=SQL_RD_OFF", db2Message);
    }
  }
  db2Exit1(": %d",(rc == SQL_SUCCESS));
  return (rc == SQL_SUCCESS);
}
