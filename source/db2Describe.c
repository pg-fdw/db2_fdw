#include <string.h>
#include <stdio.h>
#include <stdbool.h>
#include "db2_fdw.h"

/** global variables */

/** external variables */
extern char         db2Message[ERRBUFSIZE];/* contains DB2 error messages, set by db2CheckErr()             */
extern int          err_code;              /* error code, set by db2CheckErr()                              */

/** external prototypes */
extern bool         optionIsTrue         (const char* value);
extern SQLRETURN    db2CheckErr          (SQLRETURN status, SQLHANDLE handle, SQLSMALLINT handleType, int line, char* file);
extern void         db2Error_d           (db2error sqlstate, const char* message, const char* detail, ...);
extern char*        db2CopyText          (const char* string, int size, int quote);
extern char*        c2name               (short fcType);
extern HdlEntry*    db2AllocStmtHdl      (SQLSMALLINT type, DB2ConnEntry* connp, db2error error, const char* errmsg);
extern void         db2FreeStmtHdl       (HdlEntry* handlep, DB2ConnEntry* connp);

/** internal prototypes */
DB2Table*           db2Describe          (DB2Session* session, char* schema, char* table, char* pgname, char* noencerr, char* batchsz);

/* db2Describe
 * Find the remote DB2 table and describe it.
 * Returns an allocated data structure with the results.
 */
DB2Table* db2Describe (DB2Session* session, char* schema, char* table, char* pgname, char* noencerr, char* batchsz) {
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

  db2Entry1();
  /* get a complete quoted table name */
  qtable = db2CopyText (table, strlen (table), 1);
  length = strlen (qtable);
  if (schema != NULL) {
    qschema = db2CopyText (schema, strlen (schema), 1);
    length += strlen (qschema) + 1;
  }
  tablename = db2alloc (length + 1, "tablename");
  tablename[0] = '\0';		/* empty */
  if (schema != NULL) {
    strncat (tablename, qschema,length);
    strncat (tablename, ".",length);
  }
  strncat (tablename, qtable,length);
  db2free (qtable, "qtable");
  if (schema != NULL)
    db2free (qschema, "qschema");

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
  rc = db2CheckErr(rc, stmthp->hsql, stmthp->type, __LINE__, __FILE__);
  if (rc != SQL_SUCCESS) {
    if (err_code == 942)
      db2Error_d (FDW_TABLE_NOT_FOUND, "table not found",
                  "DB2 table %s for foreign table \"%s\" does not exist or does not allow read access;%s,%s", tablename, pgname,
                  db2Message, "DB2 table names are case sensitive (normally all uppercase).");
    else
      db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error describing remote table: SQLExecute failed to describe table", db2Message);
  }
  db2free(query, "query");

  /* allocate an db2Table struct for the results */
  reply          = db2alloc (sizeof (DB2Table), "DB2Table reply");
  reply->name    = tablename;
  db2Debug2("table description");
  db2Debug2("reply->name   : '%s'", reply->name);
  reply->pgname  = pgname;
  db2Debug2("reply->pgname : '%s'", reply->pgname);
  reply->npgcols = 0;

  reply->batchsz = DEFAULT_BATCHSZ;
  if (batchsz != NULL) {
    char* end;
    reply->batchsz = strtol(batchsz,&end,10);
  }

  /* get the number of columns */
  rc = SQLNumResultCols(stmthp->hsql, &ncols);
  rc = db2CheckErr(rc, stmthp->hsql, stmthp->type, __LINE__, __FILE__);
  if (rc  != SQL_SUCCESS) {
    db2Error_d (FDW_UNABLE_TO_CREATE_REPLY, "error describing remote table: SQLNumResultCols failed to get number of columns", db2Message);
  }

  reply->ncols = ncols;
  reply->cols = (DB2Column**) db2alloc ((sizeof (DB2Column*) * reply->ncols), "DB2Columns* reply->cols(%d)",reply->ncols);
  db2Debug2("reply->ncols  : %d", reply->ncols);

  /* loop through the column list */
  for (i = 1; i <= reply->ncols; ++i) {
    /* allocate an db2Column struct for the column */
    reply->cols[i - 1]                 = (DB2Column *) db2alloc (sizeof (DB2Column)," DB2Column reply->cols[%s - 1]", i);
    reply->cols[i - 1]->colPrimKeyPart = 0;
    reply->cols[i - 1]->colCodepage    = 0;
    reply->cols[i - 1]->pgname         = NULL;
    reply->cols[i - 1]->pgattnum       = 0;
    reply->cols[i - 1]->pgtype         = 0;
    reply->cols[i - 1]->pgtypmod       = 0;
    reply->cols[i - 1]->used           = 0;
    reply->cols[i - 1]->pkey           = 0;
    reply->cols[i - 1]->noencerr       = NO_ENC_ERR_NULL;

    if (noencerr != NULL) {
      reply->cols[i - 1]->noencerr = (optionIsTrue(noencerr)) ? NO_ENC_ERR_TRUE : NO_ENC_ERR_FALSE;
    }

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
    reply->cols[i - 1]->colName  = db2CopyText ((char*) colName, (int) nameLen, 1);
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
  db2Exit1(": %x", reply);
  return reply;
}
