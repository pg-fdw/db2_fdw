#include <string.h>
#include <stdio.h>
#include "db2_fdw.h"
#include "ParamDesc.h"

/** global variables */

/** external variables */
extern char         db2Message[ERRBUFSIZE];/* contains DB2 error messages, set by db2CheckErr()             */
extern int          err_code;              /* error code, set by db2CheckErr()                              */

/** external prototypes */
extern SQLRETURN    db2CheckErr          (SQLRETURN status, SQLHANDLE handle, SQLSMALLINT handleType, int line, char* file);
extern void         db2Error_d           (db2error sqlstate, const char* message, const char* detail, ...);
extern SQLSMALLINT  param2c              (SQLSMALLINT fcType);
extern void         parse2num_struct     (const char* s, SQL_NUMERIC_STRUCT* ns);
extern char*        c2name               (short fcType);
extern void         db2BindParameter     (DB2Session* session, ParamDesc* param, SQLLEN* indicators, int param_count, int col_num);

/** internal prototypes */
int                 db2ExecuteInsert     (DB2Session* session, ParamDesc* paramList);

/* db2ExecuteInsert
 * Execute a prepared statement and fetches the first result row.
 * The parameters ("bind variables") are filled from paramList.
 * Returns the count of processed rows.
 * This can be called several times for a prepared SQL statement.
 */
int db2ExecuteInsert (DB2Session* session, ParamDesc* paramList) {
  SQLLEN*     indicators   = NULL;
  ParamDesc*  param        = NULL;
  SQLRETURN   rc           = 0;
  SQLSMALLINT outlen       = 0;
  SQLCHAR     cname[256]   = {0};  /* 256 is usually plenty; see note below */
  int         rowcount     = 0;
  int         param_count  = 0;
  int         indicator_count = 0;
  
  db2Entry1();
  for (param = paramList; param != NULL; param = param->next) {
    ++param_count;
  }
  db2Debug2("paramcount: %d",param_count);
  /*
   * Allocate a temporary array of indicators.
   *
   * We use 1-based indexing below (indicator[1..param_count]) to match the
   * parameter numbering passed to SQLBindParameter.
   */
  indicator_count = param_count + 1;
  indicators = db2alloc(indicator_count * sizeof(SQLLEN), "indicators[%d]", indicator_count);

  /* bind the parameters */
  param_count = 0;
  for (param = paramList; param; param = param->next) {
    ++param_count;
    /* colnum must map to the column position in the table, as a parameter that must be 1 based */
    db2BindParameter(session, param, &indicators[param_count], param_count, param->colnum+1);
  }

  /* execute the query and get the first result row */
  db2Debug2("session->stmtp->hsql: %d",session->stmtp->hsql);
  rc = SQLGetCursorName(session->stmtp->hsql, cname, (SQLSMALLINT)sizeof(cname), &outlen); 
  rc = db2CheckErr(rc, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
  if (rc != SQL_SUCCESS) {
    db2Error_d(FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: SQLGetCusorName failed to obtain cursor name", db2Message);
  }
  db2Debug2("cursor name: '%s'", cname);
  rc = SQLExecute (session->stmtp->hsql);
  rc = db2CheckErr(rc, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
  if (rc != SQL_SUCCESS && rc != SQL_NO_DATA) {
    /* use the correct SQLSTATE for serialization failures */
    db2Error_d(err_code == 8177 ? FDW_SERIALIZATION_FAILURE : FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: SQLExecute failed to execute remote query", db2Message);
  }

  /* db2free all indicators */
  db2free (indicators, "indicators[%d]", indicator_count);
  if (rc == SQL_NO_DATA) {
    db2Debug3("SQL_NO_DATA");
  } else {
    SQLINTEGER  rowcount_val = 0;

    /* get the number of processed rows (important for DML) */
    rc = SQLRowCount(session->stmtp->hsql, &rowcount_val);
    rc = db2CheckErr(rc, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
    if (rc != SQL_SUCCESS) {
      db2Error_d ( FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: SQLRowCount failed to get number of affected rows", db2Message);
    }
    db2Debug2("rowcount_val: %lld", rowcount_val);
    rowcount = (int) rowcount_val;
  }
  db2Exit1(": %d",rowcount);
  return rowcount;
}
