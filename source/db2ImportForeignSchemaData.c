#include <string.h>
#include <stdio.h>
#include <stdbool.h>
#include "db2_fdw.h"

/** global variables */

/** external variables */
extern char         db2Message[ERRBUFSIZE];/* contains DB2 error messages, set by db2CheckErr()             */
extern int          err_code;              /* error code, set by db2CheckErr()                              */

/** external prototypes */
extern char*        db2CopyText           (const char* string, int size, int quote);
extern SQLRETURN    db2CheckErr           (SQLRETURN status, SQLHANDLE handle, SQLSMALLINT handleType, int line, char* file);
extern void         db2Error_d            (db2error sqlstate, const char* message, const char* detail, ...);
extern char*        c2name                (short fcType);
extern HdlEntry*    db2AllocStmtHdl       (SQLSMALLINT type, DB2ConnEntry* connp, db2error error, const char* errmsg);
extern void         db2FreeStmtHdl        (HdlEntry* handlep, DB2ConnEntry* connp);

/** internal prototypes */
       bool         isForeignSchema       (DB2Session* session, char* schema);
       char**       getForeignTableList   (DB2Session* session, char* schema, int list_type, char* table_list);
       DB2Table*    describeForeignTable  (DB2Session* session, char* schema, char* tabname);
static void         describeForeignColumns(DB2Session* session, char* schema, char* tabname, DB2Table* db2Table);

/* isForeignSchema
 * Check if the given schema exists in the remote DB2 database.
 * Returns true if the schema exists, false if it does not exist.
 */
bool isForeignSchema(DB2Session* session, char* schema) {
  bool      fResult       = false;
  HdlEntry* stmtp         = NULL;
  SQLBIGINT count         = 0;
  SQLLEN    ind           = SQL_NTS;
  SQLLEN    ind_c         = 0;
  SQLRETURN result        = 0;
  char*     schema_query  = "SELECT COUNT(*) AS COUNTER FROM SYSCAT.SCHEMATA WHERE SCHEMANAME = ?";

  db2Entry1("(schema: '%s')", schema);
  db2Debug2("count               : %lld", (long long)count);
  db2Debug2("schema query        : '%s'", schema_query);
  /* create statement handle */
  stmtp = db2AllocStmtHdl(SQL_HANDLE_STMT, session->connp, FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: failed to allocate statement handle");
  db2Debug2("stmp->hsql : %d",stmtp->hsql);
  db2Debug2("stmp->type : %d",stmtp->type);
  /* prepare the query */
  result = SQLPrepare(stmtp->hsql, (SQLCHAR*)schema_query, SQL_NTS);
  db2Debug2("SQLPrepare rc       : %d",result);
  result = db2CheckErr(result, stmtp->hsql, stmtp->type, __LINE__, __FILE__);
  if (result != SQL_SUCCESS) {
    db2Error_d ( FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLPrepare failed to prepare schema query", db2Message);
  }
  /* bind the parameter */
  result = SQLBindParameter(stmtp->hsql, 1, SQL_PARAM_INPUT,SQL_C_CHAR, SQL_VARCHAR, 128, 0, schema, sizeof(schema), &ind);
  db2Debug2("SQLBindParameter1 NAME = '%s', ind = %d,  rc : %d",schema, ind, result);
  result = db2CheckErr(result, stmtp->hsql, stmtp->type, __LINE__, __FILE__);
  if (result != SQL_SUCCESS) {
    db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLBindParameter failed to bind parameter", db2Message);
  }
  /* define the result value */
  result = SQLBindCol (stmtp->hsql, 1, SQL_C_SBIGINT, &count, 0, &ind_c);
  db2Debug2("SQLBindCol rc : %d",result);
  result = db2CheckErr(result, stmtp->hsql, stmtp->type, __LINE__, __FILE__);
  if (result != SQL_SUCCESS) {
    db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLBindCol failed to define result", db2Message);
  }
  /* execute the query and get the first result row */
  result = SQLExecute(stmtp->hsql);
  db2Debug2("SQLExecute rc : %d",result);
  result = db2CheckErr(result, stmtp->hsql, stmtp->type, __LINE__, __FILE__);
  if (result != SQL_SUCCESS) {
    db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLExecute failed to execute schema query", db2Message);
  } else {
    result = SQLFetch(stmtp->hsql);
    db2Debug2("SQLFetch rc : %d, count = %lld, ind_c = %d",result, (long long)count, ind_c);
    result = db2CheckErr(result, stmtp->hsql, stmtp->type, __LINE__, __FILE__);
    if (result != SQL_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLFetch failed to execute schema query", db2Message);
    }
  }
  db2Debug2("count(*) = %lld, ind_c = %d", (long long)count, ind_c);
  /* release the statement handle */
  db2FreeStmtHdl(stmtp, session->connp);
  stmtp = NULL;
  /* return false if the remote schema does not exist */
  fResult = (count > 0);
  db2Exit1(": %s, result = %s", schema, fResult ? "true" : "false");
 return fResult;
}

/* getForeignTableList
 * Get the list of tables in the given schema on the remote DB2 database.
 * Returns an allocated array of table names, terminated by a NULL entry.
 */
char** getForeignTableList(DB2Session* session, char* schema, int list_type, char* table_list){
  SQLRETURN   rc            = 0;
  HdlEntry*   stmtp         = NULL;
  SQLLEN      ind_s         = SQL_NTS;
  char*       column_query  = NULL;
  SQLCHAR     tab_buf [TABLE_NAME_LEN];
  SQLLEN      ind_tab;
  int         tabidx        = 0;
  char**      tabnames      = NULL;
   db2Entry1("(schema: '%s', list_type: %d, table_list: '%s')", schema, list_type, table_list);
  switch(list_type){
      case 0: {   /* FDW_IMPORT_SCHEMA_ALL      */
        char* query_str = "SELECT T.TABNAME FROM SYSCAT.TABLES T  WHERE UPPER(T.TABSCHEMA) = UPPER(?) AND T.TYPE IN ('T','V') ORDER BY T.TABNAME";
        int   s_len     = strlen(query_str)+1;
        column_query = db2alloc(s_len, "column_query");
        strncpy(column_query,query_str,s_len);
      }
      break;
      case 1: {   /* FDW_IMPORT_SCHEMA_LIMIT_TO */
        char* query_str = "SELECT T.TABNAME FROM SYSCAT.TABLES T WHERE UPPER(T.TABSCHEMA) = UPPER(?) AND T.TYPE IN ('T','V') AND UPPER(T.TABNAME) IN (%s) ORDER BY T.TABNAME";
        int   s_len     = strlen(query_str) + strlen(table_list) + 1;
        column_query = db2alloc(s_len, "column_query");
        snprintf(column_query,s_len,query_str,table_list);
      }
      break;
      case 2: {   /* FDW_IMPORT_SCHEMA_EXCEPT   */
        char* query_str = "SELECT T.TABNAME FROM SYSCAT.TABLES T WHERE UPPER(T.TABSCHEMA) = UPPER(?) AND T.TYPE IN ('T','V') AND UPPER(T.TABNAME) NOT IN (%s) ORDER BY T.TABNAME";
        int   s_len     = strlen(query_str) + strlen(table_list) + 1;
        column_query = db2alloc(s_len, "column_query");
        snprintf(column_query,s_len,query_str,table_list);
      }
      break;
      default:
        db2Debug2("schema import type: %d", list_type);
        db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "invalid schema import type", db2Message);
      break;
    }
  db2Debug2("column query : '%s'", column_query);
  /* create statement handle */
  stmtp = db2AllocStmtHdl(SQL_HANDLE_STMT, session->connp, FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: failed to allocate statement handle");
  
  /* prepare the query */
  rc = SQLPrepare(stmtp->hsql, (SQLCHAR*)column_query, SQL_NTS);
  db2Debug2("SQLPrepare rc : %d",rc);
  rc = db2CheckErr(rc, stmtp->hsql, stmtp->type,  __LINE__, __FILE__);
  if (rc != SQL_SUCCESS) {
    db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLPrepare failed to prepare remote query", db2Message);
  }
  /* bind the parameter */
  rc = SQLBindParameter(stmtp->hsql, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, 128, 0, schema, sizeof(schema), &ind_s);
  db2Debug2("SQLBindParameter table_schema = '%s' rc : %d",schema, rc);
  rc = db2CheckErr(rc, stmtp->hsql, stmtp->type, __LINE__, __FILE__);
  if (rc != SQL_SUCCESS) {
    db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLBindParameter failed to bind parameter", db2Message);
  }
  rc = SQLBindCol(stmtp->hsql, 1, SQL_C_CHAR, tab_buf, sizeof(tab_buf), &ind_tab);
  db2Debug2("SQLBindCol1 rc : %d",rc);
  rc = db2CheckErr(rc, stmtp->hsql, stmtp->type,  __LINE__, __FILE__);
  if (rc != SQL_SUCCESS) {
    db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLBindCol failed to define result for table name", db2Message);
  }
  
  /* execute the query and get the first result row */
  rc = SQLExecute (stmtp->hsql);
  db2Debug2("SQLExecute rc : %d",rc);
  rc = db2CheckErr(rc, stmtp->hsql, stmtp->type, __LINE__, __FILE__);
  if (rc != SQL_SUCCESS && rc != SQL_NO_DATA) {
    db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLExecute failed to execute column query", db2Message);
  }
  tabidx   = 0;
  rc = SQLFetch(stmtp->hsql);
  rc = db2CheckErr(rc, stmtp->hsql, stmtp->type, __LINE__, __FILE__);
  if (rc != SQL_SUCCESS && rc != SQL_NO_DATA) {
    db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLFetch failed to execute column query", db2Message);
  }
  tabnames = (char**) db2alloc( (tabidx + 1) * sizeof(char*), "tabnames");
  while(rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO) {
    tabnames[tabidx] = NULL;
    db2Debug2("tabname[%d] : '%s', ind: %d", tabidx, tab_buf, ind_tab);
    if (ind_tab != SQL_NULL_DATA) {
      char* tabname = (char*) db2alloc(strlen((char*)tab_buf)+1, "tabname");
      strncpy(tabname, (char*)tab_buf, strlen((char*)tab_buf)+1);
      tabnames[tabidx] = tabname;
    }
    rc = SQLFetch(stmtp->hsql);
    rc = db2CheckErr(rc, stmtp->hsql, stmtp->type, __LINE__, __FILE__);
    if (rc != SQL_SUCCESS && rc != SQL_NO_DATA) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLFetch failed to execute column query", db2Message);
    }
    tabidx++;
    tabnames = (char**) db2realloc((tabidx + 1) * sizeof(char*), tabnames, "tabnames");
  }
  tabnames[tabidx] = NULL;
  /* release the statement handle */
  db2FreeStmtHdl(stmtp, session->connp);
  stmtp = NULL;
  db2free(column_query,"column_query");
  db2Exit1(": [%d]", tabidx-1);
  return tabnames;
}

/* describeForeignTable
 * Find the remote DB2 table and describes it.
 * Returns an allocated data structure with the results.
 */
DB2Table* describeForeignTable (DB2Session* session, char* schema, char* tabname) {
  DB2Table*   reply;
  HdlEntry*   stmthp;
  char*       qtable    = NULL;
  char*       qschema   = NULL;
  char*       tablename = NULL;
  SQLCHAR*    query     = NULL;
  int         i;
  int         length;
  SQLSMALLINT ncols;
  SQLCHAR     colName[128];
  SQLSMALLINT nameLen;
  SQLSMALLINT dataType;
  SQLULEN     colSize;
  SQLLEN      charlen;
  SQLLEN      bin_size;
  SQLSMALLINT scale;
  SQLSMALLINT nullable;
  SQLINTEGER  codepage = 0;
  SQLRETURN   rc = 0;

  db2Entry1("(schema: %s, tablename: %s)", schema, tabname);
  /* get a complete quoted table name */
  qtable = db2CopyText (tabname, strlen (tabname), 1);
  length = strlen (qtable);
  if (schema != NULL) {
    qschema = db2CopyText (schema, strlen (schema), 1);
    length += strlen (qschema) + 1;
  }
  tablename = db2alloc (length + 1,"tablename");
  tablename[0] = '\0';		/* empty */
  if (schema != NULL) {
    strncat (tablename, qschema,length);
    strncat (tablename, ".",length);
  }
  strncat (tablename, qtable,length);
  db2free (qtable,"qtable");
  if (schema != NULL)
    db2free (qschema,"qschema");

  /* construct a "SELECT * FROM ..." query to describe columns */
  length += 40;
  query = db2alloc (length + 1, "query");
  snprintf ((char*)query, length+1, (char*)"SELECT * FROM %s FETCH FIRST 1 ROW ONLY", tablename);

  /* create statement handle */
  stmthp = db2AllocStmtHdl(SQL_HANDLE_STMT, session->connp, FDW_UNABLE_TO_CREATE_REPLY, "error describing remote table: failed to allocate statement handle");

  /* prepare the query */
  rc = SQLPrepare(stmthp->hsql, query, SQL_NTS);
  rc = db2CheckErr(rc, stmthp->hsql,stmthp->type, __LINE__, __FILE__);
  if (rc != SQL_SUCCESS) {
    db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error describing remote table: SQLPrepare failed to prepare query", db2Message);
  }
  /* execute the query */
  rc = SQLExecute(stmthp->hsql);
  rc= db2CheckErr(rc, stmthp->hsql, stmthp->type, __LINE__, __FILE__);
  if (rc != SQL_SUCCESS) {
    if (err_code == 942)
      db2Error_d (FDW_TABLE_NOT_FOUND, "table not found",
                  "DB2 table %s does not exist or does not allow read access;%s", tablename,
                  db2Message, "DB2 table names are case sensitive (normally all uppercase).");
    else
      db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error describing remote table: SQLExecute failed to describe table", db2Message);
  }
  db2free(query,"query");

  /* allocate an db2Table struct for the results */
  reply          = db2alloc (sizeof (DB2Table),"DB2Table* reply");
  reply->name    = tabname;
  db2Debug2("table description");
  db2Debug2("reply->name   : '%s'", reply->name);
  reply->batchsz = DEFAULT_BATCHSZ;

  /* get the number of columns */
  rc = SQLNumResultCols(stmthp->hsql, &ncols);
  rc = db2CheckErr(rc, stmthp->hsql, stmthp->type, __LINE__, __FILE__);
  if (rc  != SQL_SUCCESS) {
    db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error describing remote table: SQLNumResultCols failed to get number of columns", db2Message);
  }

  reply->ncols = ncols;
  reply->cols = (DB2Column**) db2alloc (sizeof (DB2Column*) *reply->ncols,"reply->cols(%d)",reply->ncols);
  db2Debug2("reply->ncols  : %d", reply->ncols);

  /* loop through the column list */
  for (i = 1; i <= reply->ncols; ++i) {
    /* allocate an db2Column struct for the column */
    reply->cols[i - 1]                 = (DB2Column *) db2alloc (sizeof (DB2Column), "reply->cols[%d - 1]", i);
    reply->cols[i - 1]->colPrimKeyPart = 0;
    reply->cols[i - 1]->colCodepage    = 0;
    reply->cols[i - 1]->pgname         = NULL;
    reply->cols[i - 1]->pgattnum       = 0;
    reply->cols[i - 1]->pgtype         = 0;
    reply->cols[i - 1]->pgtypmod       = 0;
    reply->cols[i - 1]->used           = 0;
    reply->cols[i - 1]->pkey           = 0;
    reply->cols[i - 1]->noencerr       = NO_ENC_ERR_NULL;

    /* get the parameter descriptor for the column */
    rc = SQLDescribeCol(stmthp->hsql
                       , i                  // index of column in table
                       , (SQLCHAR*)&colName // column name
                       , sizeof(colName)    // buffer length
                       , &nameLen           // column name length
                       , &dataType          // column data type
                       , &colSize           // column data type size
                       , &scale             // column data type precision
                       , &nullable          // column nullable
                       );
    rc = db2CheckErr(rc, stmthp->hsql, stmthp->type, __LINE__, __FILE__);
    if (rc != SQL_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error describing remote table: SQLDescribeCol failed to get column data", db2Message);
    }
    reply->cols[i - 1]->colName  = db2strdup((char*)colName,"reply->cols[%d - 1]->colName",i);
    db2Debug2("reply->cols[%d]->colName  : '%s'", (i-1), reply->cols[i - 1]->colName);
    db2Debug2("dataType: %d", dataType);
    reply->cols[i - 1]->colType  = (short)  dataType;
    if (dataType == -7){
      // datatype -7 does not exist it seems to be used for SQL_BOOLEAN wrongly
      reply->cols[i - 1]->colType = SQL_BOOLEAN;
    }
    db2Debug2("reply->cols[%d]->colType  : %d (%s)", (i-1), reply->cols[i - 1]->colType,c2name(reply->cols[i - 1]->colType));
    reply->cols[i - 1]->colSize  = (size_t) colSize;
    db2Debug2("reply->cols[%d]->colSize  : %ld", (i-1), reply->cols[i - 1]->colSize);
    reply->cols[i - 1]->colScale = (short)  scale;
    db2Debug2("reply->cols[%d]->colScale : %d", (i-1), reply->cols[i - 1]->colScale);
    reply->cols[i - 1]->colNulls = (short)  nullable;
    db2Debug2("reply->cols[%d]->colNulls : %d", (i-1), reply->cols[i - 1]->colNulls);

    /* get the number of characters for string fields */
    rc = SQLColAttribute (stmthp->hsql, i, SQL_DESC_PRECISION, NULL, 0, NULL, &charlen);
    rc = db2CheckErr(rc, stmthp->hsql, stmthp->type, __LINE__, __FILE__);
    if (rc != SQL_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error describing remote table: SQLColAttribute failed to get column length", db2Message);
    }
    reply->cols[i - 1]->colChars = (size_t) charlen;
    db2Debug2("reply->cols[%d]->colChars : %ld", (i-1), reply->cols[i - 1]->colChars);

    /* get the binary length for RAW fields */
    rc = SQLColAttribute (stmthp->hsql, i, SQL_DESC_OCTET_LENGTH, NULL, 0, NULL, &bin_size);
    rc = db2CheckErr(rc, stmthp->hsql, stmthp->type, __LINE__, __FILE__);
    if (rc != SQL_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error describing remote table: SQLColAttribute failed to get column size", db2Message);
    }
    reply->cols[i - 1]->colBytes = (size_t) bin_size;
    db2Debug2("reply->cols[%d]->colBytes : %ld", (i-1), reply->cols[i - 1]->colBytes);

    /* get the columns codepage */
    rc = SQLColAttribute(stmthp->hsql, i, SQL_DESC_CODEPAGE, NULL, 0, NULL, (SQLPOINTER)&codepage);
    rc = db2CheckErr(rc, stmthp->hsql, stmthp->type, __LINE__, __FILE__);
    if (rc != SQL_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error describing remote table: SQLColAttribute failed to get column codepage", db2Message);
    }
    reply->cols[i - 1]->colCodepage = (int) codepage;
    db2Debug2("reply->cols[%d]->colCodepage : %d", (i-1), reply->cols[i - 1]->colCodepage);

    /* Unfortunately a LONG VARBINARY is of type LONG VARCHAR but the codepage is set to 0 */
    if (reply->cols[i-1]->colType == SQL_LONGVARCHAR && reply->cols[i-1]->colCodepage == 0){
      reply->cols[i-1]->colType = SQL_LONGVARBINARY;
    }

    /* determine db2Type and length to allocate */
    switch (reply->cols[i - 1]->colType) {
      case SQL_CHAR:
      case SQL_VARCHAR:
      case SQL_LONGVARCHAR:
        reply->cols[i - 1]->val_size = bin_size + 1;
      break;
      case SQL_BLOB:
      case SQL_CLOB:
        reply->cols[i - 1]->val_size = bin_size + 1;
      break;
      case SQL_GRAPHIC:
      case SQL_VARGRAPHIC:
      case SQL_LONGVARGRAPHIC:
      case SQL_WCHAR:
      case SQL_WVARCHAR:
      case SQL_WLONGVARCHAR:
      case SQL_DBCLOB:
        reply->cols[i - 1]->val_size = bin_size + 1;
      break;
      case SQL_BOOLEAN:
        reply->cols[i - 1]->val_size = bin_size + 1;
      break;
      case SQL_INTEGER:
      case SQL_SMALLINT:
          reply->cols[i - 1]->val_size = charlen + 2;
      break;
      case SQL_NUMERIC:
      case SQL_DECIMAL:
        if (scale == 0)
          reply->cols[i - 1]->val_size = bin_size;
        else
          reply->cols[i - 1]->val_size = (scale > colSize ? scale : colSize) + 5;
      break;
      case SQL_REAL:
      case SQL_DOUBLE:
      case SQL_FLOAT:
      case SQL_DECFLOAT:
        reply->cols[i - 1]->val_size = 24 + 1;
      break;
      case SQL_TYPE_DATE:
      case SQL_TYPE_TIME:
      case SQL_TYPE_TIMESTAMP:
      case SQL_TYPE_TIMESTAMP_WITH_TIMEZONE:
        reply->cols[i - 1]->val_size = colSize + 1;
      break;
      case SQL_BIGINT:
        reply->cols[i - 1]->val_size = 24;
      break;
      case SQL_XML:
        reply->cols[i - 1]->val_size = LOB_CHUNK_SIZE + 1;
      break;
      case SQL_BINARY:
      case SQL_VARBINARY:
      case SQL_LONGVARBINARY:
        reply->cols[i - 1]->val_size = bin_size;
      break;
      default:
      break;
    }
    db2Debug2("reply->cols[%d]->val_size : %d", (i-1), reply->cols[i - 1]->val_size);
  }
  /* release statement handle, this takes care of the parameter handles */
  db2FreeStmtHdl(stmthp, session->connp);
  if (reply != NULL) {
    /* get the primary key information for the table and mark the columns in the reply */
    describeForeignColumns(session, schema, tabname, reply);
  }
  db2Exit1(": %x", reply);
  return reply;
}

/* describeForeignColumns
 * Get the primary key information for the given table and mark the columns in the reply.
 */
static void describeForeignColumns(DB2Session* session, char* schema, char* tabname, DB2Table* db2Table) {
  int          colidx  = 0;
  HdlEntry*    stmtp   = NULL;
  SQLRETURN    rc      = 0;
  SQLSMALLINT  keyseq_val;
  SQLLEN       ind_key;
  SQLSMALLINT  cp_val;
  SQLLEN       ind_cp;
  SQLLEN       ind_s  = SQL_NTS;
  SQLLEN       ind_t  = SQL_NTS;
  char*        query  = "SELECT COALESCE(C.KEYSEQ, 0) AS KEY, C.CODEPAGE FROM SYSCAT.COLUMNS C WHERE UPPER(C.TABSCHEMA) = UPPER(?) AND UPPER(C.TABNAME) = UPPER(?) AND COALESCE(C.HIDDEN,'') = '' ORDER BY C.COLNO";

  db2Entry1("(schema: %s, tabname: %s)", schema, tabname);
  db2Debug2("query : '%s'", query);
  /* create statement handle */
  stmtp = db2AllocStmtHdl(SQL_HANDLE_STMT, session->connp, FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: failed to allocate statement handle");

  /* prepare the query */
  rc = SQLPrepare(stmtp->hsql, (SQLCHAR*)query, SQL_NTS);
  db2Debug2("SQLPrepare rc : %d",rc);
  rc = db2CheckErr(rc, stmtp->hsql, stmtp->type,  __LINE__, __FILE__);
  if (rc != SQL_SUCCESS) {
    db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLPrepare failed to prepare remote query", db2Message);
  }

  /* bind the parameter 1 - schema */
  rc = SQLBindParameter(stmtp->hsql, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 128, 0, schema, 0, &ind_s);
  db2Debug2("SQLBindParameter table_schema = '%s' rc : %d",schema, rc);
  rc = db2CheckErr(rc, stmtp->hsql, stmtp->type, __LINE__, __FILE__);
  if (rc != SQL_SUCCESS) {
    db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLBindParameter failed to bind parameter", db2Message);
  }
  /* bind the parameter 2 - tablename */
  rc = SQLBindParameter(stmtp->hsql, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 128, 0, tabname, 0, &ind_t);
  db2Debug2("SQLBindParameter table_name = '%s' rc : %d",tabname, rc);
  rc = db2CheckErr(rc, stmtp->hsql, stmtp->type, __LINE__, __FILE__);
  if (rc != SQL_SUCCESS) {
    db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLBindParameter failed to bind parameter", db2Message);
  }
  /* bind result column 1 - KEYSEQ */
  rc = SQLBindCol(stmtp->hsql, 1, SQL_C_SHORT, &keyseq_val, 0, &ind_key);
  db2Debug2("SQLBindCol1 rc : %d",rc);
  rc = db2CheckErr(rc, stmtp->hsql, stmtp->type,  __LINE__, __FILE__);
  if (rc != SQL_SUCCESS) {
    db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLBindCol failed to define result for primary key", db2Message);
  }
  /* bind result column 2 - CODEPAGE */
  rc = SQLBindCol(stmtp->hsql, 2, SQL_C_SHORT, &cp_val, 0, &ind_cp);
  db2Debug2("SQLBindCol2 rc : %d",rc);
  rc = db2CheckErr(rc, stmtp->hsql, stmtp->type,  __LINE__, __FILE__);
  if (rc != SQL_SUCCESS) {
    db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLBindCol failed to define result for codepage", db2Message);
  }
  /* execute the query and get the first result row */
  rc = SQLExecute (stmtp->hsql);
  db2Debug2("SQLExecute rc : %d",rc);
  rc = db2CheckErr(rc, stmtp->hsql, stmtp->type, __LINE__, __FILE__);
  if (rc != SQL_SUCCESS && rc != SQL_NO_DATA) {
    db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLExecute failed to execute column query", db2Message);
  }

  /* fetch the first result row */
  colidx = 0;
  rc = SQLFetch(stmtp->hsql);
  rc = db2CheckErr(rc, stmtp->hsql, stmtp->type, __LINE__, __FILE__);
  if (rc != SQL_SUCCESS && rc != SQL_NO_DATA) {
    db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLFetch failed to fetch result row", db2Message);
  }
  while(rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO) {

    db2Debug2("keyseq_val: %d, ind: %d", keyseq_val, ind_key);
    db2Table->cols[colidx]->colPrimKeyPart = (ind_key   == SQL_NULL_DATA) ? 0 : (int) keyseq_val;
    db2Debug2("cp_val    : %d, ind: %d", cp_val, ind_cp);
    db2Table->cols[colidx]->colCodepage    = (ind_cp    == SQL_NULL_DATA) ? 0 : (int) cp_val;

    db2Debug2("db2Table->cols[%d]->colName       : %s "     , colidx, db2Table->cols[colidx]->colName );
    db2Debug2("db2Table->cols[%d]->colType       : %d - %s,", colidx, db2Table->cols[colidx]->colType, c2name(db2Table->cols[colidx]->colType));
    db2Debug2("db2Table->cols[%d]->colSize       : %d"      , colidx, db2Table->cols[colidx]->colSize );
    db2Debug2("db2Table->cols[%d]->colBytes      : %d"      , colidx, db2Table->cols[colidx]->colBytes );
    db2Debug2("db2Table->cols[%d]->colChars      : %d"      , colidx, db2Table->cols[colidx]->colChars );
    db2Debug2("db2Table->cols[%d]->colScale      : %d"      , colidx, db2Table->cols[colidx]->colScale);
    db2Debug2("db2Table->cols[%d]->colNulls      : %d"      , colidx, db2Table->cols[colidx]->colNulls);
    db2Debug2("db2Table->cols[%d]->colPrimKeyPart: %d"      , colidx, db2Table->cols[colidx]->colPrimKeyPart);
    db2Debug2("db2Table->cols[%d]->colCodepage   : %d"      , colidx, db2Table->cols[colidx]->colCodepage);
    db2Debug2("db2Table->cols[%d]->val_size      : %d"      , colidx, db2Table->cols[colidx]->val_size);

    /* fetch the next result row */
    rc = SQLFetch(stmtp->hsql);
    rc = db2CheckErr(rc, stmtp->hsql, stmtp->type, __LINE__, __FILE__);
    if (rc != SQL_SUCCESS && rc != SQL_NO_DATA) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLFetch failed to fetch result row", db2Message);
    }
    colidx++;
  } 
  db2Debug3("End of Data reached");
  /* release the statement handle */
  db2FreeStmtHdl(stmtp, session->connp);
  db2Exit1();
}