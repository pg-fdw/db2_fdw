#include <string.h>
#include <stdint.h>
#include "db2_fdw.h"
#include "ParamDesc.h"
#include "DB2ResultColumn.h"

#define SQL_VALUE_PTR_ULEN(v) ((SQLPOINTER)(uintptr_t)(SQLULEN)(v))

/** global variables */

/** external variables */
extern char         db2Message[ERRBUFSIZE];/* contains DB2 error messages, set by db2CheckErr()             */

/** external prototypes */
extern SQLRETURN    db2CheckErr          (SQLRETURN status, SQLHANDLE handle, SQLSMALLINT handleType, int line, char* file);
extern HdlEntry*    db2AllocStmtHdl      (SQLSMALLINT type, DB2ConnEntry* connp, db2error error, const char* errmsg);
extern SQLSMALLINT  c2param              (SQLSMALLINT fparamType);
extern char*        param2name           (SQLSMALLINT fparamType);

/** internal prototypes */
void                db2PrepareQuery      (DB2Session* session, const char *query, DB2ResultColumn* resultList, unsigned long prefetch, int fetchsize);

/* db2PrepareQuery
 * Prepares an SQL statement for execution.
 * This function should handle everything that has to be done only once even if the statement is executed multiple times, that is:
 * - For SELECT statements, defines the result values to be stored in db2Table.
 * - For DML statements, allocates LOB locators for the RETURNING clause in db2Table.
 * - Set the prefetch options.
 */
void db2PrepareQuery (DB2Session* session, const char *query, DB2ResultColumn* resultList, unsigned long prefetch, int fetchsize) {
  int               col_pos     = 0;
  int               is_select   = 0;
  int               for_update  = 0;
  SQLRETURN         rc          = 0;
  DB2ResultColumn*  res         = NULL;
  int               need_getdata = 0;

  /* figure out if the query is a SELECT / SELECT FOR UPDATE */
  is_select  = (strncmp (query, "SELECT", 6) == 0);
  for_update = (strstr (query, "FOR UPDATE") != NULL);

  #ifdef FIXED_FETCH_SIZE
  // Until the proper handling of multiple rows results on a single query are added the fetch size must be 1
  fetchsize = 1;
  #endif

  /*
   * DECIMAL/NUMERIC/DECFLOAT are retrieved separately with SQLGetData.  All
   * other columns stay bound.  In particular, do not switch the whole result
   * set to SQL_RD_OFF: that changes the lifetime and conversion semantics of
   * every column and was the source of a broad SELECT workaround, not a fix.
   */
  for (res = resultList; res; res = res->next) {
    if (res->colType == SQL_DECIMAL || res->colType == SQL_NUMERIC || res->colType == SQL_DECFLOAT) {
      need_getdata = 1;
      break;
    }
  }
  if (need_getdata)
    fetchsize = 1;

  db2Entry1();
  db2Debug2("query    : '%s'",query);
  db2Debug2("prefetch : %d",prefetch);
  db2Debug2("fetchsize: %d",fetchsize);
  /* make sure there is no statement handle stored in "session" */
  if (session->stmtp != NULL) {
    db2Error(FDW_ERROR, "db2PrepareQuery internal error: statement handle is not NULL");
  }

  /* create statement handle */
  session->stmtp = db2AllocStmtHdl(SQL_HANDLE_STMT, session->connp, FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: failed to allocate statement handle");
  db2Debug2("session->stmtp->hsql: %d",session->stmtp->hsql);
  /* set prefetch options */
  if (is_select) {
    SQLULEN prefetch_rows = prefetch;
    SQLULEN cur_fetchsize = fetchsize;
    db2Debug3("IS_SELECT");
    if (for_update) {
      db2Debug3("FOR UPDATE");
      // Make the cursor sensitive scrollable (e.g., static) so PREFETCH_NROWS applies
      rc = SQLSetStmtAttr(session->stmtp->hsql, SQL_ATTR_CURSOR_TYPE, (SQLPOINTER)SQL_CURSOR_DYNAMIC, 0);
      rc = db2CheckErr(rc, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
      if (rc != SQL_SUCCESS) {
        db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: SQLSetStmtAttr failed to make cursor dynamic", db2Message);
      }
      db2Debug3("set cursor dynamic");
      rc = SQLSetStmtAttr(session->stmtp->hsql, SQL_ATTR_CONCURRENCY, (SQLPOINTER)SQL_CONCUR_LOCK, 0);
      rc = db2CheckErr(rc, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
      if (rc != SQL_SUCCESS) {
        db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: SQLSetStmtAttr failed to make cursor pessemistic", db2Message);
      }
      db2Debug3("set cursor pessemistic");
    } else {
      /*
       * Keep the forward-only cursor introduced in 18.2.  Several DB2 CLI
       * setups reject the former static cursor/prefetch combination with
       * CLI0111E / SQLSTATE 22003.
       */
      rc = SQLSetStmtAttr(session->stmtp->hsql, SQL_ATTR_CURSOR_TYPE, (SQLPOINTER)SQL_CURSOR_FORWARD_ONLY, 0);
      rc = db2CheckErr(rc, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
      if (rc != SQL_SUCCESS) {
        db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: SQLSetStmtAttr failed to make cursor forward-only", db2Message);
      }
      db2Debug3("set cursor forward-only");
    }

    /* The current implementation owns one value/indicator buffer per column. */
    rc = SQLSetStmtAttr(session->stmtp->hsql, SQL_ATTR_ROW_ARRAY_SIZE, SQL_VALUE_PTR_ULEN(cur_fetchsize), 0);
    rc = db2CheckErr(rc, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
    if (rc != SQL_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: SQLSetStmtAttr failed to set fetchsize in statement handle", db2Message);
    }
    db2Debug2("set cursor fetchsize: %d",cur_fetchsize);

    /* PREFETCH_NROWS only applies to the scrollable FOR UPDATE cursor. */
    if (for_update) {
      rc = SQLSetStmtAttr(session->stmtp->hsql, SQL_ATTR_PREFETCH_NROWS, SQL_VALUE_PTR_ULEN(prefetch_rows), 0);
      rc = db2CheckErr(rc, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
      if (rc != SQL_SUCCESS) {
        db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: SQLSetStmtAttr failed to set number of prefetched rows in statement handle", db2Message);
      }
      db2Debug2("set cursor prefetch: %d",prefetch_rows);
    }
  }

  /* prepare the statement */
  db2Debug2("query to prepare: '%s'",query);
  rc = SQLPrepare(session->stmtp->hsql, (SQLCHAR*)query, SQL_NTS);
  rc = db2CheckErr(rc, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
  if (rc != SQL_SUCCESS) {
    db2Error_d(FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: SQLPrepare failed to prepare remote query", db2Message);
  }

  /* loop through expected result columns */
  for (res = resultList; res; res = res->next){
    SQLSMALLINT fparamType = c2param((SQLSMALLINT)res->colType);
    int use_getdata = 0;
    size_t needed = 0;
    /* Unfortunately DB2 handles DML statements with a RETURNING clause quite different from SELECT statements.
     * In the latter, the result columns are "defined", i.e. bound to some storage space.
     * This definition is only necessary once, even if the query is executed multiple times, so we do this here.
     * RETURNING clause are handled in db2ExecuteQuery, here we only allocate locators for LOB columns in RETURNING clauses.
     */
    /* figure out in which format we want the results */
    if (res->pgtype == UUIDOID) {
      fparamType = SQL_C_CHAR;
    }

    /*
     * For DECIMAL/NUMERIC/DECFLOAT results, avoid binding with SQLBindCol.
     * Some DB2 CLI setups throw CLI0111E / SQLSTATE 22003 during SQLFetch when
     * converting bound numeric columns.
     *
     * We instead fetch these columns via SQLGetData after SQLFetch.
     */
    use_getdata = (res->colType == SQL_DECIMAL || res->colType == SQL_NUMERIC || res->colType == SQL_DECFLOAT);

    /*
     * Numeric result columns are typically bound as SQL_C_CHAR.
     *
     * Some DB2 CLI setups report CLI0111E / SQLSTATE 22003 during SQLFetch when
     * converting DECIMAL/NUMERIC into too-small output buffers.
     *
     * Ensure the buffer is large enough for the textual representation:
     *  - precision digits
     *  - optional sign
     *  - optional decimal point
     *  - NUL terminator
     * For DECFLOAT, use a conservative minimum to accommodate exponent forms.
     */
    if (res->colType == SQL_DECIMAL || res->colType == SQL_NUMERIC || res->colType == SQL_DECFLOAT) {
      size_t prec = (res->colSize > 0 ? res->colSize : 32);
      size_t scale = (res->colScale > 0 ? res->colScale : 0);
      needed = prec + 2 /* sign + NUL */ + (scale > 0 ? 1 /* '.' */ : 0);
      if (res->colType == SQL_DECFLOAT && needed < 64)
        needed = 64;

      if (res->val_size < needed) {
        res->val = (char*) db2realloc(needed, res->val, "res->val");
        res->val_size = needed;
      }
    }

    db2Debug2("res->colName       : %s" ,res->colName);
    db2Debug2("res->colSize       : %ld",res->colSize);
    db2Debug2("res->colType       : %d" ,res->colType);
    db2Debug2("res->colScale      : %d" ,res->colScale);
    db2Debug2("res->colNulls      : %d" ,res->colNulls);
    db2Debug2("res->colChars      : %ld",res->colChars);
    db2Debug2("res->colBytes      : %ld",res->colBytes);
    db2Debug2("res->colPrimKeyPart: %d" ,res->colPrimKeyPart);
    db2Debug2("res->colCodepage   : %d" ,res->colCodepage);
    db2Debug2("res->val           : %x" ,res->val);
    db2Debug2("res->val_size      : %ld",res->val_size);
    db2Debug2("res->val_len       : %ld" ,(long) res->val_len);
    db2Debug2("res->val_null      : %ld" ,(long) res->val_null);
    db2Debug2("res->resnum        : %d" ,res->resnum);
    db2Debug2("fparamType: %d (%s)",fparamType,param2name(fparamType));

    if (use_getdata) {
      /* DECIMAL/NUMERIC/DECFLOAT are always fetched via SQLGetData. */
      res->val_null = (intptr_t) SQL_NULL_DATA;
      res->val_len = 0;
    } else {
      if (sizeof(SQLLEN) > sizeof(res->val_indicator.bytes)) {
        db2Error(FDW_ERROR, "db2PrepareQuery internal error: SQLLEN does not fit into result indicator storage");
      }
      memset(&res->val_indicator, 0, sizeof(res->val_indicator));
      db2Debug2("SQLBindCol(%d,%d,%d(%s),%x,%ld,%x)",session->stmtp->hsql,res->resnum, fparamType, param2name(fparamType), res->val, res->val_size, res->val_indicator.bytes);
      rc = SQLBindCol (session->stmtp->hsql,res->resnum, fparamType, res->val, res->val_size, (SQLLEN*) res->val_indicator.bytes);
      rc = db2CheckErr(rc, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
      if (rc != SQL_SUCCESS) {
        db2Error_d(FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: SQLBindCol failed to define result value", db2Message);
      }
    }
    col_pos++;
  }

  db2Debug2("is_select: %s",is_select ? "true" : "false");
  db2Debug2("col_pos: %d",col_pos);
  if (is_select && col_pos == 0) {
    /* No columns selected (i.e., SELECT '1' FROM or COUNT(*)).
     * Use persistent buffers from statement handle to avoid stack deallocation issues.
     * This fixes the segfault when using aggregate functions without WHERE clause.
     */
    rc = SQLBindCol(session->stmtp->hsql, 1, SQL_C_CHAR, session->stmtp->dummy_buffer, sizeof(session->stmtp->dummy_buffer), &session->stmtp->dummy_null);
    rc = db2CheckErr(rc, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
    if (rc != SQL_SUCCESS) {
      db2Error_d ( FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: SQLBindCol failed to define result value", db2Message);
    }
  }
  db2Exit1();
}
