#include <string.h>
#include <stdio.h>
#include <sqlcli1.h>
#include <postgres_ext.h>
#include "db2_fdw.h"

/** global variables */

/** external variables */
extern char         db2Message[ERRBUFSIZE];/* contains DB2 error messages, set by db2CheckErr()             */

/** external prototypes */
extern void*        db2alloc             (const char* type, size_t size);
extern void         db2free              (void* p);
extern void         db2Debug1            (const char* message, ...);
extern void         db2Debug2            (const char* message, ...);
extern void         db2Debug3            (const char* message, ...);
extern SQLRETURN    db2CheckErr          (SQLRETURN status, SQLHANDLE handle, SQLSMALLINT handleType, int line, char* file);
extern void         db2Error_d           (db2error sqlstate, const char* message, const char* detail, ...);
extern char*        c2name               (short fcType);
extern HdlEntry*    db2AllocStmtHdl      (SQLSMALLINT type, DB2ConnEntry* connp, db2error error, const char* errmsg);
extern void         db2FreeStmtHdl       (HdlEntry* handlep, DB2ConnEntry* connp);

/** internal prototypes */
int                 db2GetImportColumn   (DB2Session* session, char* schema, char* table_list, int list_type, char* tabname, char* colName, short* colType, size_t* colLen, short* colScale, short* colNulls, int* key, int* cp);

/** db2GetImportColumn
 *   Get the next element in the ordered list of tables and their columns for "schema".
 *   Returns 0 if there are no more columns, -1 if the remote schema does not exist, else 1.
 */
int db2GetImportColumn(DB2Session* session, char* schema, char* table_list, int list_type, char* tabname, char* colName, short* colType, size_t* colLen, short* colScale, short* colNulls, int* key, int* cp) {
  /* the static variables will contain data returned to the caller */
  SQLCHAR      tab_buf [TABLE_NAME_LEN];
  SQLLEN       ind_tab;
  SQLCHAR      col_buf [COLUMN_NAME_LEN];
  SQLLEN       ind_col;
  SQLCHAR      typ_buf [19];
  SQLLEN       ind_typ;
  SQLINTEGER   len_val;
  SQLLEN       ind_len;
  SQLSMALLINT  scale_val;
  SQLLEN       ind_scale;
  SQLCHAR      nulls_val[2];
  SQLLEN       ind_nulls;
  SQLSMALLINT  keyseq_val;
  SQLLEN       ind_key;
  SQLSMALLINT  cp_val;
  SQLLEN       ind_cp;
  SQLRETURN    result         = 0;

  db2Debug1("> db2GetImportCol");
  db2Debug2("  session: %x", session);
  db2Debug2("  session->connp: %x", session->connp);
  db2Debug2("  session->emvp : %x", session->envp);
  db2Debug2("  session->stmtp: %x", session->stmtp);
  /* return a pointer to the static variables */

  /* when first called, check if the schema does exist */
  if (session->stmtp == NULL) {
    SQLBIGINT  count        = 0;
    SQLLEN     ind          = SQL_NTS;
    SQLLEN     ind_c        = 0;
    char*      schema_query = "SELECT COUNT(*) AS COUNTER FROM SYSCAT.SCHEMATA WHERE SCHEMANAME = ?";
    db2Debug2("  count               : %lld", (long long)count);
    db2Debug2("  schema query        : '%s'", schema_query);

    /* create statement handle */
    session->stmtp = db2AllocStmtHdl(SQL_HANDLE_STMT, session->connp, FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: failed to allocate statement handle");
    db2Debug2("  session->stmp->hsql : %d",session->stmtp->hsql);
    db2Debug2("  session->stmp->type : %d",session->stmtp->type);
    /* prepare the query */
    result = SQLPrepare(session->stmtp->hsql, (SQLCHAR*)schema_query, SQL_NTS);
    db2Debug2("  SQLPrepare rc       : %d",result);
    result = db2CheckErr(result, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
    if (result != SQL_SUCCESS) {
      db2Error_d ( FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLPrepare failed to prepare schema query", db2Message);
    }

    /* bind the parameter */
    result = SQLBindParameter(session->stmtp->hsql, 1, SQL_PARAM_INPUT,SQL_C_CHAR, SQL_VARCHAR, 128, 0, schema, sizeof(schema), &ind);
    db2Debug2("  SQLBindParameter1 NAME = '%s', ind = %d,  rc : %d",schema, ind, result);
    result = db2CheckErr(result, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
    if (result != SQL_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLBindParameter failed to bind parameter", db2Message);
    }

    /* define the result value */
    result = SQLBindCol (session->stmtp->hsql, 1, SQL_C_SBIGINT, &count, 0, &ind_c);
    db2Debug2("  SQLBindCol rc : %d",result);
    result = db2CheckErr(result, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
    if (result != SQL_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLBindCol failed to define result", db2Message);
    }

    /* execute the query and get the first result row */
    result = SQLExecute(session->stmtp->hsql);
    db2Debug2("  SQLExecute rc : %d",result);
    result = db2CheckErr(result, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
    if (result != SQL_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLExecute failed to execute schema query", db2Message);
    } else {
      result = SQLFetch(session->stmtp->hsql);
      db2Debug2("  SQLFetch rc : %d, count = %lld, ind_c = %d",result, (long long)count, ind_c);
      result = db2CheckErr(result, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
      if (result != SQL_SUCCESS) {
        db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLFetch failed to execute schema query", db2Message);
      }
    }
    db2Debug2("  count(*) = %lld, ind_c = %d", (long long)count, ind_c);
    /* release the statement handle */
    db2FreeStmtHdl(session->stmtp, session->connp);
    db2Debug2("  session->connp: %x", session->connp);
    db2Debug2("  session->emvp : %x", session->envp);
    db2Debug2("  session->stmtp: %x", session->stmtp);
    db2Debug2("  try to set session->stmtp to NULL");
    session->stmtp = NULL;
    /* return -1 if the remote schema does not exist */
    if (count == 0) {
      db2Debug1("< db2GetImportCol - returns: -1");
      return -1;
    }
  }

  /* when first calles, prepare the query to obtain the schema data */
  if (session->stmtp == NULL) {
    SQLLEN   ind_s         = SQL_NTS;
    char*    column_query  = NULL;

    switch(list_type){
      case 0: {   /* FDW_IMPORT_SCHEMA_ALL      */
        char* query_str = "SELECT T.TABNAME, C.COLNAME, C.TYPENAME, C.LENGTH, C.SCALE, C.NULLS, COALESCE(C.KEYSEQ, 0) AS KEY, C.CODEPAGE"
                          " FROM SYSCAT.TABLES T JOIN SYSCAT.COLUMNS C ON T.TABSCHEMA = C.TABSCHEMA AND T.TABNAME   = C.TABNAME"
                          " WHERE UPPER(T.TABSCHEMA) = UPPER(?) AND T.TYPE IN ('T','V') AND COALESCE(C.HIDDEN,'') = '' ORDER BY T.TABNAME, C.COLNO";
        int   s_len     = strlen(query_str)+1;
        column_query = db2alloc("column_query",s_len);
        strncpy(column_query,query_str,s_len);
      }
      break;
      case 1: {   /* FDW_IMPORT_SCHEMA_LIMIT_TO */
        char* query_str = "SELECT T.TABNAME, C.COLNAME, C.TYPENAME, C.LENGTH, C.SCALE, C.NULLS, COALESCE(C.KEYSEQ, 0) AS KEY, C.CODEPAGE"
                          " FROM SYSCAT.TABLES T JOIN SYSCAT.COLUMNS C ON T.TABSCHEMA = C.TABSCHEMA AND T.TABNAME   = C.TABNAME"
                          " WHERE UPPER(T.TABSCHEMA) = UPPER(?) AND T.TYPE IN ('T','V') AND UPPER(T.TABNAME) IN (%s) AND COALESCE(C.HIDDEN,'') = '' ORDER BY T.TABNAME, C.COLNO";
        int   s_len     = strlen(query_str) + strlen(table_list) + 1;
        column_query = db2alloc("column_query",s_len);
        snprintf(column_query,s_len,query_str,table_list);
      }
      break;
      case 2: {   /* FDW_IMPORT_SCHEMA_EXCEPT   */
        char* query_str = "SELECT T.TABNAME, C.COLNAME, C.TYPENAME, C.LENGTH, C.SCALE, C.NULLS, COALESCE(C.KEYSEQ, 0) AS KEY, C.CODEPAGE"
                          " FROM SYSCAT.TABLES T JOIN SYSCAT.COLUMNS C ON T.TABSCHEMA = C.TABSCHEMA AND T.TABNAME   = C.TABNAME"
                          " WHERE UPPER(T.TABSCHEMA) = UPPER(?) AND T.TYPE IN ('T','V') AND UPPER(T.TABNAME) NOT IN (%s) AND COALESCE(C.HIDDEN,'') = '' ORDER BY T.TABNAME, C.COLNO";
        int   s_len     = strlen(query_str) + strlen(table_list) + 1;
        column_query = db2alloc("column_query",s_len);
        snprintf(column_query,s_len,query_str,table_list);
      }
      break;
      default:
        db2Debug2("  schema import type: %d", list_type);
        db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "invalid schema import type", db2Message);
      break;
    }
    db2Debug2("  column query : '%s'", column_query);
    /* create statement handle */
    session->stmtp = db2AllocStmtHdl(SQL_HANDLE_STMT, session->connp, FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: failed to allocate statement handle");

    /* prepare the query */
    result = SQLPrepare(session->stmtp->hsql, (SQLCHAR*)column_query, SQL_NTS);
    db2Debug2("  SQLPrepare rc : %d",result);
    result = db2CheckErr(result, session->stmtp->hsql, session->stmtp->type,  __LINE__, __FILE__);
    if (result != SQL_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLPrepare failed to prepare remote query", db2Message);
    }

    /* bind the parameter */
    result = SQLBindParameter(session->stmtp->hsql, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, 128, 0, schema, sizeof(schema), &ind_s);
    db2Debug2("  SQLBindParameter table_schema = '%s' rc : %d",schema, result);
    result = db2CheckErr(result, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
    if (result != SQL_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLBindParameter failed to bind parameter", db2Message);
    }

    result = SQLBindCol(session->stmtp->hsql, 1, SQL_C_CHAR, tab_buf, sizeof(tab_buf), &ind_tab);
    db2Debug2("  SQLBindCol1 rc : %d",result);
    result = db2CheckErr(result, session->stmtp->hsql, session->stmtp->type,  __LINE__, __FILE__);
    if (result != SQL_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLBindCol failed to define result for table name", db2Message);
    }

    result = SQLBindCol(session->stmtp->hsql, 2, SQL_C_CHAR, col_buf, sizeof(col_buf), &ind_col);
    db2Debug2("  SQLBindCol2 rc : %d",result);
    result = db2CheckErr(result, session->stmtp->hsql, session->stmtp->type,  __LINE__, __FILE__);
    if (result != SQL_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLBindCol failed to define result for column name", db2Message);
    }

    result = SQLBindCol(session->stmtp->hsql, 3, SQL_C_CHAR, typ_buf, sizeof(typ_buf), &ind_typ);
    db2Debug2("  SQLBindCol3 rc : %d",result);
    result = db2CheckErr(result, session->stmtp->hsql, session->stmtp->type,  __LINE__, __FILE__);
    if (result != SQL_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLBindCol failed to define result for type name", db2Message);
    }

    result = SQLBindCol(session->stmtp->hsql, 4, SQL_C_LONG, &len_val, 0, &ind_len);
    db2Debug2("  SQLBindCol4 rc : %d",result);
    result = db2CheckErr(result, session->stmtp->hsql, session->stmtp->type,  __LINE__, __FILE__);
    if (result != SQL_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLBindCol failed to define result for character length", db2Message);
    }

    result = SQLBindCol(session->stmtp->hsql, 5, SQL_C_SHORT, &scale_val, 0, &ind_scale);
    db2Debug2("  SQLBindCol5 rc : %d",result);
    result = db2CheckErr(result, session->stmtp->hsql, session->stmtp->type,  __LINE__, __FILE__);
    if (result != SQL_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLBindCol failed to define result for type scale", db2Message);
    }

    result = SQLBindCol(session->stmtp->hsql, 6, SQL_C_CHAR, nulls_val, sizeof(nulls_val), &ind_nulls);
    db2Debug2("  SQLBindCol6 rc : %d",result);
    result = db2CheckErr(result, session->stmtp->hsql, session->stmtp->type,  __LINE__, __FILE__);
    if (result != SQL_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLBindCol failed to define result for nullability", db2Message);
    }

    result = SQLBindCol(session->stmtp->hsql, 7, SQL_C_SHORT, &keyseq_val, 0, &ind_key);
    db2Debug2("  SQLBindCol7 rc : %d",result);
    result = db2CheckErr(result, session->stmtp->hsql, session->stmtp->type,  __LINE__, __FILE__);
    if (result != SQL_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLBindCol failed to define result for primary key", db2Message);
    }

    result = SQLBindCol(session->stmtp->hsql, 8, SQL_C_SHORT, &cp_val, 0, &ind_cp);
    db2Debug2("  SQLBindCol8 rc : %d",result);
    result = db2CheckErr(result, session->stmtp->hsql, session->stmtp->type,  __LINE__, __FILE__);
    if (result != SQL_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLBindCol failed to define result for type scale", db2Message);
    }

    /* execute the query and get the first result row */
    result = SQLExecute (session->stmtp->hsql);
    db2Debug2("  SQLExecute rc : %d",result);
    result = db2CheckErr(result, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
    if (result != SQL_SUCCESS && result != SQL_NO_DATA) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLExecute failed to execute column query", db2Message);
    }
    db2free(column_query);
  }

  /* for any subsequent call, just fetch the next row from that cursor */
  if (session->stmtp != NULL) {
    /* fetch the next result row */
    result = SQLFetch(session->stmtp->hsql);
    result = db2CheckErr(result, session->stmtp->hsql, session->stmtp->type,  __LINE__, __FILE__);
    if (result != SQL_SUCCESS && result != SQL_NO_DATA) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error importing foreign schema: SQLFetchScroll failed to fetch next result row", db2Message);
    }
  }

  if (result == SQL_NO_DATA) {
    db2Debug3("  End of Data reached");
    /* release the statement handle */
    db2FreeStmtHdl(session->stmtp, session->connp);
    session->stmtp = NULL;
    db2Debug1("< db2GetImportCol - returns: 0");
    return 0;
  } else {
    char* typename = (char*)typ_buf;
    db2Debug2("  tabname : '%s', ind: %d", tab_buf   , ind_tab  );
    db2Debug2("  colname : '%s', ind: %d", col_buf   , ind_col  );
    db2Debug2("  typename: '%s', ind: %d", typ_buf   , ind_typ  );
    db2Debug2("  length  : %d  , ind: %d", len_val   , ind_len  );
    db2Debug2("  scale   : %d  , ind: %d", scale_val , ind_scale);
    db2Debug2("  isnull  : '%s', ind: %d", nulls_val , ind_nulls);
    db2Debug2("  key     : %d  , ind: %d", keyseq_val, ind_key  );
    db2Debug2("  codepage: %d  , ind: %d", cp_val    , ind_cp   );
    if (ind_tab == SQL_NULL_DATA)
      tabname[0] = '\0';
    else
      strncpy(tabname, (char*)tab_buf, TABLE_NAME_LEN);
    if (ind_col == SQL_NULL_DATA)
      colName[0] = '\0';
    else
      strncpy(colName, (char*)col_buf, COLUMN_NAME_LEN);
    *colLen    = (ind_len   == SQL_NULL_DATA) ? 0 : (size_t) len_val;
    *colScale  = (ind_scale == SQL_NULL_DATA) ? 0 : (short) scale_val;
    *colNulls  = (ind_nulls == SQL_NULL_DATA) ? 0 : (nulls_val[0] == 'Y');
    *key       = (ind_key   == SQL_NULL_DATA) ? 0 : (int) keyseq_val;
    *cp        = (ind_cp    == SQL_NULL_DATA) ? 0 : (int) cp_val;
    /* figure out correct data type */
         if (strcmp (typename, "VARCHAR"      ) == 0) *colType = SQL_VARCHAR;
    else if (strcmp (typename, "LONG VARCHAR" ) == 0 && *cp != 0) *colType = SQL_LONGVARCHAR;
    else if (strcmp (typename, "LONG VARCHAR" ) == 0 && *cp == 0) *colType = SQL_LONGVARBINARY;
    else if (strcmp (typename, "CHARACTER"    ) == 0) *colType = SQL_CHAR;
    else if (strcmp (typename, "BINARY"       ) == 0) *colType = SQL_BINARY;
    else if (strcmp (typename, "VARBINARY"    ) == 0) *colType = SQL_VARBINARY;
    else if (strcmp (typename, "SMALLINT"     ) == 0) *colType = SQL_SMALLINT;
    else if (strcmp (typename, "INTEGER"      ) == 0) *colType = SQL_INTEGER;
    else if (strcmp (typename, "BIGINT"       ) == 0) *colType = SQL_BIGINT;
    else if (strcmp (typename, "DATE"         ) == 0) *colType = SQL_TYPE_DATE;
    else if (strcmp (typename, "TIMESTAMP"    ) == 0) *colType = SQL_TYPE_TIMESTAMP;
    else if (strcmp (typename, "TIME"         ) == 0) *colType = SQL_TYPE_TIME;
    else if (strcmp (typename, "XML"          ) == 0) *colType = SQL_XML;
    else if (strcmp (typename, "BLOB"         ) == 0) *colType = SQL_BLOB;
    else if (strcmp (typename, "CLOB"         ) == 0) *colType = SQL_CLOB;
    else if (strcmp (typename, "DECIMAL"      ) == 0) *colType = SQL_DECIMAL;
    else if (strcmp (typename, "GRAPHIC"      ) == 0) *colType = SQL_GRAPHIC;
    else if (strcmp (typename, "VARGRAPHIC"   ) == 0) *colType = SQL_VARGRAPHIC;
    else if (strcmp (typename, "DECFLOAT"     ) == 0) *colType = SQL_DECFLOAT;
    else if (strcmp (typename, "DOUBLE"       ) == 0) *colType = SQL_DOUBLE;
    else if (strcmp (typename, "REAL"         ) == 0) *colType = SQL_REAL;
    else if (strcmp (typename, "FLOAT"        ) == 0) *colType = SQL_FLOAT;
    else if (strcmp (typename, "BOOLEAN"      ) == 0) *colType = SQL_BOOLEAN;
    else {
      db2Debug1("  unknown typename: '%s'",typename);
      *colType = SQL_UNKNOWN_TYPE;
    }
    db2Debug2("  colType : %s (%d)", c2name(*colType), *colType);
  }
  db2Debug1("< db2GetImportCol - returns: 1");
  return 1;
}
