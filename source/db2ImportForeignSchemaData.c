#include <string.h>
#include <stdio.h>
#include <stdbool.h>
#include "db2_fdw.h"

/** global variables */

/** external variables */
extern char         db2Message[ERRBUFSIZE];/* contains DB2 error messages, set by db2CheckErr()             */

/** external prototypes */
extern SQLRETURN    db2CheckErr           (SQLRETURN status, SQLHANDLE handle, SQLSMALLINT handleType, int line, char* file);
extern char*        c2name                (short fcType);
extern short        name2c                (char* typename);
extern HdlEntry*    db2AllocStmtHdl       (SQLSMALLINT type, DB2ConnEntry* connp, db2error error, const char* errmsg);
extern void         db2FreeStmtHdl        (HdlEntry* handlep, DB2ConnEntry* connp);

/** internal prototypes */
       bool         isForeignSchema       (DB2Session* session, char* schema);
       char**       getForeignTableList   (DB2Session* session, char* schema, int list_type, char* table_list, char* importtype);
       DB2Table*    describeForeignTable  (DB2Session* session, char* schema, char* tabname);
static void         describeForeignColumns(DB2Session* session, char* schema, char* tabname, DB2Table* db2Table);
static void         catalog2Column        (DB2Column* col, char* colname, char* typename, int length, short scale, int codepage, char* stringunits, int unitslength, char nulls);
static void         setValSize            (DB2Column* col);
static int          getForeignTableColNum (DB2Session* session, char* schema, char* tabname);

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
char** getForeignTableList(DB2Session* session, char* schema, int list_type, char* table_list, char* importtype){
  SQLRETURN   rc            = 0;
  HdlEntry*   stmtp         = NULL;
  SQLLEN      ind_s         = SQL_NTS;
  char*       column_query  = NULL;
  SQLCHAR     tab_buf [TABLE_NAME_LEN];
  SQLLEN      ind_tab;
  int         tabidx        = 0;
  char**      tabnames      = NULL;
  db2Entry1("(schema: '%s', list_type: %d, table_list: '%s')", schema, list_type, table_list);

  if (importtype == NULL) {
    importtype = "T','V";
  }

  switch(list_type){
      case 0: {   /* FDW_IMPORT_SCHEMA_ALL      */
        char* query_str = "SELECT T.TABNAME FROM SYSCAT.TABLES T  WHERE UPPER(T.TABSCHEMA) = UPPER(?) AND T.TYPE IN ('%s') ORDER BY T.TABNAME";
        int   s_len     = strlen(query_str)+strlen(importtype)+1;
        column_query = db2alloc(s_len, "column_query");
        snprintf(column_query,s_len,query_str,importtype);
      }
      break;
      case 1: {   /* FDW_IMPORT_SCHEMA_LIMIT_TO */
        char* query_str = "SELECT T.TABNAME FROM SYSCAT.TABLES T WHERE UPPER(T.TABSCHEMA) = UPPER(?) AND T.TYPE IN ('%s') AND UPPER(T.TABNAME) IN (%s) ORDER BY T.TABNAME";
        int   s_len     = strlen(query_str) + strlen(importtype) + strlen(table_list) + 1;
        column_query = db2alloc(s_len, "column_query");
        snprintf(column_query,s_len,query_str,importtype,table_list);
      }
      break;
      case 2: {   /* FDW_IMPORT_SCHEMA_EXCEPT   */
        char* query_str = "SELECT T.TABNAME FROM SYSCAT.TABLES T WHERE UPPER(T.TABSCHEMA) = UPPER(?) AND T.TYPE IN ('%s') AND UPPER(T.TABNAME) NOT IN (%s) ORDER BY T.TABNAME";
        int   s_len     = strlen(query_str) + strlen(importtype) + strlen(table_list) + 1;
        column_query = db2alloc(s_len, "column_query");
        snprintf(column_query,s_len,query_str,importtype,table_list);
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
 * Find the remote DB2 table and describe it from the catalog (SYSCAT.COLUMNS).
 * Returns an allocated data structure with the results.
 */
DB2Table* describeForeignTable (DB2Session* session, char* schema, char* tabname) {
  DB2Table* reply = NULL;
  int       ncols = 0;

  db2Entry1("(schema: %s, tablename: %s)", schema, tabname);
  /* the number of columns also tells whether the table exists */
  ncols = getForeignTableColNum(session, schema, tabname);
  if (ncols == 0) {
    db2Error_df (FDW_TABLE_NOT_FOUND, "table not found",
                 "DB2 table \"%s\".\"%s\" does not exist or has no columns;%s", schema, tabname,
                 "DB2 table names are case sensitive (normally all uppercase).");
  }

  /* allocate an db2Table struct for the results */
  reply          = db2alloc (sizeof (DB2Table),"DB2Table* reply");
  reply->name    = tabname;
  reply->batchsz = DEFAULT_BATCHSZ;
  reply->ncols   = ncols;
  reply->cols    = (DB2Column**) db2alloc (sizeof (DB2Column*) * reply->ncols,"reply->cols(%d)",reply->ncols);
  db2Debug2("reply->name   : '%s'", reply->name);
  db2Debug2("reply->ncols  : %d", reply->ncols);

  /* describe the columns */
  describeForeignColumns(session, schema, tabname, reply);
  db2Exit1(": %x", reply);
  return reply;
}

/* describeForeignColumns
 * Describe the columns of the given table from SYSCAT.COLUMNS, including the primary key information.
 * db2Table->cols must be allocated for db2Table->ncols columns; ncols is reduced if the catalog returns fewer rows.
 */
static void describeForeignColumns(DB2Session* session, char* schema, char* tabname, DB2Table* db2Table) {
  int          colidx  = 0;
  HdlEntry*    stmtp   = NULL;
  SQLRETURN    rc      = 0;
  SQLSMALLINT  keyseq_val;
  SQLLEN       ind_key;
  SQLCHAR      name_val[129];
  SQLLEN       ind_name;
  SQLCHAR      type_val[129];
  SQLLEN       ind_type;
  SQLINTEGER   len_val;
  SQLLEN       ind_len;
  SQLSMALLINT  scale_val;
  SQLLEN       ind_scale;
  SQLSMALLINT  cp_val;
  SQLLEN       ind_cp;
  SQLCHAR      units_val[12];
  SQLLEN       ind_units;
  SQLINTEGER   ulen_val;
  SQLLEN       ind_ulen;
  SQLCHAR      nulls_val[2];
  SQLLEN       ind_nulls;
  DB2Column*   col;
  SQLLEN       ind_s  = SQL_NTS;
  SQLLEN       ind_t  = SQL_NTS;
  /* The BASE* columns resolve a distinct type (METATYPE 'T') to its built-in source type, e.g. DB2SECURITYLABEL -> VARCHAR,
   * so name2c() can map it; structured types (e.g. ST_GEOMETRY) keep their own name and map to SQL_UNKNOWN_TYPE.
   * TYPEMODULENAME IS NULL excludes module-scoped types, which can share a name (e.g. CONNECTION) and are never column types.
   * Schema and table name are compared exactly, like the quoted names of a SELECT: MYTAB and MyTab are different tables.
   */
  char*        query  = "SELECT COALESCE(C.KEYSEQ, 0) AS KEY, C.COLNAME"
                        ", CASE WHEN D.METATYPE = 'T' THEN D.SOURCENAME ELSE C.TYPENAME END AS BASETYPE"
                        ", CASE WHEN D.METATYPE = 'T' THEN D.LENGTH     ELSE C.LENGTH   END AS BASELENGTH"
                        ", CASE WHEN D.METATYPE = 'T' THEN D.SCALE      ELSE C.SCALE    END AS BASESCALE"
                        ", CASE WHEN D.METATYPE = 'T' THEN D.CODEPAGE   ELSE C.CODEPAGE END AS BASECODEPAGE"
                        ", C.TYPESTRINGUNITS, C.STRINGUNITSLENGTH, C.NULLS"
                        " FROM SYSCAT.COLUMNS C"
                        " LEFT JOIN SYSCAT.DATATYPES D ON D.TYPESCHEMA = C.TYPESCHEMA AND D.TYPENAME = C.TYPENAME AND D.TYPEMODULENAME IS NULL"
                        " WHERE C.TABSCHEMA = ? AND C.TABNAME = ? AND COALESCE(C.HIDDEN,'') = '' ORDER BY C.COLNO";

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
  /* bind result column 2 - COLNAME */
  rc = SQLBindCol(stmtp->hsql, 2, SQL_C_CHAR, name_val, sizeof(name_val), &ind_name);
  db2Debug2("SQLBindCol2 rc : %d",rc);
  rc = db2CheckErr(rc, stmtp->hsql, stmtp->type,  __LINE__, __FILE__);
  if (rc != SQL_SUCCESS) {
    db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLBindCol failed to define result for column name", db2Message);
  }
  /* bind result column 3 - BASETYPE */
  rc = SQLBindCol(stmtp->hsql, 3, SQL_C_CHAR, type_val, sizeof(type_val), &ind_type);
  db2Debug2("SQLBindCol3 rc : %d",rc);
  rc = db2CheckErr(rc, stmtp->hsql, stmtp->type,  __LINE__, __FILE__);
  if (rc != SQL_SUCCESS) {
    db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLBindCol failed to define result for base type", db2Message);
  }
  /* bind result column 4 - BASELENGTH */
  rc = SQLBindCol(stmtp->hsql, 4, SQL_C_LONG, &len_val, 0, &ind_len);
  db2Debug2("SQLBindCol4 rc : %d",rc);
  rc = db2CheckErr(rc, stmtp->hsql, stmtp->type,  __LINE__, __FILE__);
  if (rc != SQL_SUCCESS) {
    db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLBindCol failed to define result for length", db2Message);
  }
  /* bind result column 5 - BASESCALE */
  rc = SQLBindCol(stmtp->hsql, 5, SQL_C_SHORT, &scale_val, 0, &ind_scale);
  db2Debug2("SQLBindCol5 rc : %d",rc);
  rc = db2CheckErr(rc, stmtp->hsql, stmtp->type,  __LINE__, __FILE__);
  if (rc != SQL_SUCCESS) {
    db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLBindCol failed to define result for scale", db2Message);
  }
  /* bind result column 6 - BASECODEPAGE */
  rc = SQLBindCol(stmtp->hsql, 6, SQL_C_SHORT, &cp_val, 0, &ind_cp);
  db2Debug2("SQLBindCol6 rc : %d",rc);
  rc = db2CheckErr(rc, stmtp->hsql, stmtp->type,  __LINE__, __FILE__);
  if (rc != SQL_SUCCESS) {
    db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLBindCol failed to define result for codepage", db2Message);
  }
  /* bind result column 7 - TYPESTRINGUNITS */
  rc = SQLBindCol(stmtp->hsql, 7, SQL_C_CHAR, units_val, sizeof(units_val), &ind_units);
  db2Debug2("SQLBindCol7 rc : %d",rc);
  rc = db2CheckErr(rc, stmtp->hsql, stmtp->type,  __LINE__, __FILE__);
  if (rc != SQL_SUCCESS) {
    db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLBindCol failed to define result for string units", db2Message);
  }
  /* bind result column 8 - STRINGUNITSLENGTH */
  rc = SQLBindCol(stmtp->hsql, 8, SQL_C_LONG, &ulen_val, 0, &ind_ulen);
  db2Debug2("SQLBindCol8 rc : %d",rc);
  rc = db2CheckErr(rc, stmtp->hsql, stmtp->type,  __LINE__, __FILE__);
  if (rc != SQL_SUCCESS) {
    db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLBindCol failed to define result for string units length", db2Message);
  }
  /* bind result column 9 - NULLS */
  rc = SQLBindCol(stmtp->hsql, 9, SQL_C_CHAR, nulls_val, sizeof(nulls_val), &ind_nulls);
  db2Debug2("SQLBindCol9 rc : %d",rc);
  rc = db2CheckErr(rc, stmtp->hsql, stmtp->type,  __LINE__, __FILE__);
  if (rc != SQL_SUCCESS) {
    db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLBindCol failed to define result for nulls", db2Message);
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
    /* the columns were counted by a separate query, a concurrent ALTER TABLE could have added one since */
    if (colidx >= db2Table->ncols) {
      db2Debug2("catalog returned more columns than counted (%d), ignoring the rest", db2Table->ncols);
      break;
    }

    col = (DB2Column*) db2alloc (sizeof (DB2Column), "db2Table->cols[%d]", colidx);
    catalog2Column( col
                  , (ind_name  == SQL_NULL_DATA) ? ""   : (char*) name_val
                  , (ind_type  == SQL_NULL_DATA) ? ""   : (char*) type_val
                  , (ind_len   == SQL_NULL_DATA) ? 0    : (int)   len_val
                  , (ind_scale == SQL_NULL_DATA) ? 0    : (short) scale_val
                  , (ind_cp    == SQL_NULL_DATA) ? 0    : (int)   cp_val
                  , (ind_units == SQL_NULL_DATA) ? ""   : (char*) units_val
                  , (ind_ulen  == SQL_NULL_DATA) ? 0    : (int)   ulen_val
                  , (ind_nulls == SQL_NULL_DATA) ? 'Y'  : (char)  nulls_val[0]
                  );
    col->colPrimKeyPart = (ind_key == SQL_NULL_DATA) ? 0 : (int) keyseq_val;
    col->noencerr       = NO_ENC_ERR_NULL;
    setValSize(col);
    db2Table->cols[colidx] = col;

    db2Debug2("db2Table->cols[%d]->colName       : %s "     , colidx, col->colName );
    db2Debug2("db2Table->cols[%d]->colType       : %d - %s,", colidx, col->colType, c2name(col->colType));
    db2Debug2("db2Table->cols[%d]->colSize       : %d"      , colidx, col->colSize );
    db2Debug2("db2Table->cols[%d]->colBytes      : %d"      , colidx, col->colBytes );
    db2Debug2("db2Table->cols[%d]->colChars      : %d"      , colidx, col->colChars );
    db2Debug2("db2Table->cols[%d]->colScale      : %d"      , colidx, col->colScale);
    db2Debug2("db2Table->cols[%d]->colNulls      : %d"      , colidx, col->colNulls);
    db2Debug2("db2Table->cols[%d]->colPrimKeyPart: %d"      , colidx, col->colPrimKeyPart);
    db2Debug2("db2Table->cols[%d]->colCodepage   : %d"      , colidx, col->colCodepage);
    db2Debug2("db2Table->cols[%d]->val_size      : %d"      , colidx, col->val_size);

    /* fetch the next result row */
    rc = SQLFetch(stmtp->hsql);
    rc = db2CheckErr(rc, stmtp->hsql, stmtp->type, __LINE__, __FILE__);
    if (rc != SQL_SUCCESS && rc != SQL_NO_DATA) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLFetch failed to fetch result row", db2Message);
    }
    colidx++;
  } 
  /* or a concurrent ALTER TABLE dropped one */
  if (colidx < db2Table->ncols) {
    db2Debug2("catalog returned fewer columns (%d) than counted (%d)", colidx, db2Table->ncols);
    db2Table->ncols = colidx;
  }
  db2Debug3("End of Data reached");
  /* release the statement handle */
  db2FreeStmtHdl(stmtp, session->connp);
  db2Exit1();
}

/* setValSize
 * Determine the size of the result buffer for a column.
 */
static void setValSize(DB2Column* col) {
  db2Entry4();
  switch (col->colType) {
    case SQL_CHAR:
    case SQL_VARCHAR:
    case SQL_LONGVARCHAR:
    case SQL_BLOB:
    case SQL_CLOB:
    case SQL_GRAPHIC:
    case SQL_VARGRAPHIC:
    case SQL_LONGVARGRAPHIC:
    case SQL_WCHAR:
    case SQL_WVARCHAR:
    case SQL_WLONGVARCHAR:
    case SQL_DBCLOB:
    case SQL_BOOLEAN:
    case SQL_BINARY:
    case SQL_VARBINARY:
    case SQL_LONGVARBINARY:
      /* one spare byte, the fetch path always terminates the buffer */
      col->val_size = col->colBytes + 1;
    break;
    case SQL_INTEGER:
    case SQL_SMALLINT:
      col->val_size = col->colChars + 2;
    break;
    case SQL_NUMERIC:
    case SQL_DECIMAL:
      if (col->colScale == 0)
        col->val_size = col->colBytes + 1;
      else
        col->val_size = ((size_t) col->colScale > col->colSize ? (size_t) col->colScale : col->colSize) + 5;
    break;
    case SQL_REAL:
    case SQL_DOUBLE:
    case SQL_FLOAT:
    case SQL_DECFLOAT:
    case SQL_BIGINT:
      col->val_size = 24 + 1;
    break;
    case SQL_TYPE_DATE:
    case SQL_TYPE_TIME:
    case SQL_TYPE_TIMESTAMP:
    case SQL_TYPE_TIMESTAMP_WITH_TIMEZONE:
      col->val_size = col->colSize + 1;
    break;
    case SQL_XML:
      col->val_size = LOB_CHUNK_SIZE + 1;
    break;
    default:
    break;
  }
  db2Exit4(": %ld", col->val_size);
}

/* catalog2Column
 * Build the DB2-side column description from a SYSCAT.COLUMNS row (distinct types already resolved to their source type),
 * with the values SQLDescribeCol and SQLColAttribute (SQL_DESC_PRECISION, SQL_DESC_OCTET_LENGTH) deliver, which the
 * db2size/db2bytes/db2chars options have always been based on:
 *   colSize  - SQLDescribeCol column size (precision for numerics, display size for datetime)
 *   colChars - SQL_DESC_PRECISION, only set for numerics, TIMESTAMP and BOOLEAN
 *   colBytes - SQL_DESC_OCTET_LENGTH, ODBC transfer octet length
 * Verified against the DB2 SAMPLE schema and TC030; distinct types are not verified yet.
 */
static void catalog2Column(DB2Column* col, char* colname, char* typename, int length, short scale, int codepage, char* stringunits, int unitslength, char nulls) {
  db2Entry4("(colname: %s, typename: %s, length: %d, scale: %d, codepage: %d, stringunits: %s, unitslength: %d, nulls: %c)", colname, typename, length, scale, codepage, stringunits, unitslength, nulls);
  col->colName     = db2strdup(colname, "col->colName");
  col->colType     = name2c(typename);
  col->colScale    = scale;
  col->colNulls    = (nulls == 'N') ? SQL_NO_NULLS : SQL_NULLABLE;
  col->colCodepage = codepage;

  /* FOR BIT DATA columns are stored as character types with codepage 0 */
  if (codepage == 0) {
    switch (col->colType) {
      case SQL_CHAR:        col->colType = SQL_BINARY;        break;
      case SQL_VARCHAR:     col->colType = SQL_VARBINARY;     break;
      case SQL_LONGVARCHAR: col->colType = SQL_LONGVARBINARY; break;
      default:                                                break;
    }
  }

  switch (col->colType) {
    case SQL_CHAR:
    case SQL_VARCHAR:
    case SQL_LONGVARCHAR:
    case SQL_CLOB:
      /* CODEUNITS32 (unitslength) unverified */
      col->colSize  = length;
      col->colBytes = length;
      col->colChars = 0;
    break;
    case SQL_GRAPHIC:
    case SQL_VARGRAPHIC:
    case SQL_LONGVARGRAPHIC:
    case SQL_DBCLOB:
      /* LENGTH counts double-byte characters */
      col->colSize  = length;
      col->colBytes = (size_t) length * 2;
      col->colChars = 0;
    break;
    case SQL_BINARY:
    case SQL_VARBINARY:
    case SQL_LONGVARBINARY:
    case SQL_BLOB:
      col->colSize  = length;
      col->colBytes = length;
      col->colChars = 0;
    break;
    case SQL_SMALLINT:
      col->colSize  = 5;
      col->colBytes = 2;
      col->colChars = 5;
    break;
    case SQL_INTEGER:
      col->colSize  = 10;
      col->colBytes = 4;
      col->colChars = 10;
    break;
    case SQL_BIGINT:
      col->colSize  = 19;
      col->colBytes = 8;
      col->colChars = 19;
    break;
    case SQL_REAL:
      col->colSize  = 7;
      col->colBytes = 4;
      col->colChars = 7;
    break;
    case SQL_DOUBLE:
      col->colSize  = 15;
      col->colBytes = 8;
      col->colChars = 15;
    break;
    case SQL_DECIMAL:
      /* LENGTH is the precision; octet length adds sign and decimal point */
      col->colSize  = length;
      col->colBytes = length + 2;
      col->colChars = length;
    break;
    case SQL_DECFLOAT:
      /* LENGTH is 8 or 16 bytes for DECFLOAT(16) / DECFLOAT(34) */
      col->colSize  = (length == 8) ? 16 : 34;
      col->colBytes = length;
      col->colChars = col->colSize;
    break;
    case SQL_TYPE_DATE:
      col->colSize  = 10;
      col->colBytes = 6;
      col->colChars = 0;
    break;
    case SQL_TYPE_TIME:
      col->colSize  = 8;
      col->colBytes = 6;
      col->colChars = 0;
    break;
    case SQL_TYPE_TIMESTAMP:
      /* SCALE holds the fractional second digits; SQL_DESC_PRECISION reports the display size here */
      col->colSize  = 19 + ((scale > 0) ? scale + 1 : 0);
      col->colBytes = 16;
      col->colChars = col->colSize;
    break;
    case SQL_BOOLEAN:
      col->colSize  = 1;
      col->colBytes = 1;
      col->colChars = 1;
    break;
    default:
      /* XML and anything else */
      col->colSize  = length;
      col->colBytes = length;
      col->colChars = 0;
    break;
  }
  db2Exit4();
}

/* getForeignTableColNum
 * Number of (not hidden) columns of the given table in SYSCAT.COLUMNS, 0 if the table does not exist.
 */
static int getForeignTableColNum(DB2Session* session, char* schema, char* tabname) {
  HdlEntry*   stmtp    = NULL;
  SQLRETURN   rc       = 0;
  SQLLEN      ind_s    = SQL_NTS;
  SQLLEN      ind_t    = SQL_NTS;
  SQLLEN      ind_cnt  = SQL_NTS;
  SQLSMALLINT clcnt    = 0;
  char*       query    = "SELECT COUNT(*) AS COLCOUNT FROM SYSCAT.COLUMNS C WHERE C.TABSCHEMA = ? AND C.TABNAME = ? AND COALESCE(C.HIDDEN,'') = ''";
  int         colcount = 0;

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
  /* bind the parameter */
  rc = SQLBindParameter(stmtp->hsql, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, 128, 0, schema, sizeof(schema), &ind_s);
  db2Debug2("SQLBindParameter table_schema = '%s' rc : %d",schema, rc);
  rc = db2CheckErr(rc, stmtp->hsql, stmtp->type, __LINE__, __FILE__);
  if (rc != SQL_SUCCESS) {
    db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLBindParameter failed to bind parameter", db2Message);
  }
  /* bind the parameter */
  rc = SQLBindParameter(stmtp->hsql, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, 128, 0, tabname, sizeof(tabname), &ind_t);
  db2Debug2("SQLBindParameter table_name = '%s' rc : %d",tabname, rc);
  rc = db2CheckErr(rc, stmtp->hsql, stmtp->type, __LINE__, __FILE__);
  if (rc != SQL_SUCCESS) {
    db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLBindParameter failed to bind parameter", db2Message);
  }
  rc = SQLBindCol(stmtp->hsql, 1, SQL_C_SHORT, &clcnt, 0, &ind_cnt);
  db2Debug2("SQLBindCol1 rc : %d",rc);
  rc = db2CheckErr(rc, stmtp->hsql, stmtp->type,  __LINE__, __FILE__);
  if (rc != SQL_SUCCESS) {
    db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLBindCol failed to define result for table colcount", db2Message);
  }
    /* execute the query and get the first result row */
  rc = SQLExecute (stmtp->hsql);
  db2Debug2("SQLExecute rc : %d",rc);
  rc = db2CheckErr(rc, stmtp->hsql, stmtp->type, __LINE__, __FILE__);
  if (rc != SQL_SUCCESS && rc != SQL_NO_DATA) {
    db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLExecute failed to execute query", db2Message);
  }
  rc = SQLFetch(stmtp->hsql);
  rc = db2CheckErr(rc, stmtp->hsql, stmtp->type, __LINE__, __FILE__);
  if (rc != SQL_SUCCESS && rc != SQL_NO_DATA) {
    db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLFetch failed to execute query", db2Message);
  }  
  /* release the statement handle */
  db2FreeStmtHdl(stmtp, session->connp);
  stmtp = NULL;

  db2Debug2("clcnt    : %d, ind: %d", clcnt, ind_cnt);
  colcount = (ind_cnt == SQL_NULL_DATA) ? 0 : (int) clcnt;

  db2Exit1(": [%d]", colcount);
  return colcount;
}