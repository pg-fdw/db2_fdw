#include <string.h>
#include <ctype.h>
#include "db2_fdw.h"

/** local prototypes */
SQLSMALLINT   c2param              (SQLSMALLINT fparamType);
char*         param2name           (SQLSMALLINT fparamType);
SQLSMALLINT   param2c              (SQLSMALLINT fcType);
short         c2dbType             (short fcType);
char*         c2name               (short fcType);

/** c2param
 *   Find db2's c-Type (SQL_) from a fParamType (SQL_C_).
 *   We are only mapping BLOB and CLOB.
 *   Everything else is mapped to a String.
 */
SQLSMALLINT param2c(SQLSMALLINT fparamType) {
  SQLSMALLINT fcType = SQL_UNKNOWN_TYPE;
  switch (fparamType) {
    case SQL_C_BLOB_LOCATOR:
      fcType = SQL_BLOB;
      break;
    case SQL_C_CLOB_LOCATOR:
      fcType = SQL_CLOB;
      break;
    default:
      /* all other columns are converted to strings */
      fcType = SQL_CHAR;
      break;
  }
  return fcType;
}

/** param2name
 *    For debugging purposes, this function provides a human readable
 *    value for a parameter type provided
 */
char* param2name(SQLSMALLINT fparamType){
  char* name = NULL;
  switch (fparamType) {
    case SQL_C_BLOB_LOCATOR:
      name = "SQL_C_BLOB_LOCATOR";
      break;
    case SQL_C_CLOB_LOCATOR:
      name = "SQL_C_CLOB_LOCATOR";
      break;
    case SQL_C_SBIGINT:
      name = "SQL_C_SBIGINT";
      break;
    case SQL_C_SHORT:
      name = "SQL_C_SHORT";
      break;
    case SQL_C_LONG:
      name = "SQL_C_LONG";
      break;
    case SQL_C_CHAR:
      name = "SQL_C_CHAR";
      break;
    default:
      /* all other columns are converted to strings */
      name = "error, this type has not been converted properly";
      break;
  }
  return name;

}

/** param2c
 *   Find db2's paramType (SQL_C_) from a cTyp (SQL_).
 *   We are only mapping BLOB and CLOB.
 *   Everything else is mapped to a String.
 *   It is the counter function of param2c.
 */
SQLSMALLINT c2param (SQLSMALLINT fcType) {
  SQLSMALLINT fparamType = SQL_C_CHAR;
  db2Entry4("(fcType: %d)",fcType);
  switch (fcType) {
    case SQL_BLOB:
      fparamType = SQL_C_BLOB_LOCATOR;
      db2Debug5("SQL_BLOB => SQL_C_LOCATOR");
      break;
    case SQL_CLOB:
      fparamType = SQL_C_CLOB_LOCATOR;
      db2Debug5("SQL_COB => SQL_C_CLOB_LOCATOR");
      break;
    default:
      /* all other columns are converted to strings */
      fparamType = SQL_C_CHAR;
      db2Debug5("%s => SQL_C_CHAR",c2name(fcType));
      break;
  }
  db2Exit4(": %d",fparamType);
  return fparamType;
}

/** c2dbType
 *    Map a fcType to the fdw internal value representation.
 *    This is required for all functions that cannot use sqlcli1.h
 *    but only use postgres.h.
 */
short c2dbType(short fcType){
  short dbType = DB2_UNKNOWN_TYPE;
  switch (fcType) {
    case SQL_CHAR:
      dbType = DB2_CHAR;
    break;
    case SQL_DECIMAL:
      dbType = DB2_DECIMAL;
    break;
    case SQL_INTEGER:
      dbType = DB2_INTEGER;
    break;
    case SQL_SMALLINT:
      dbType = DB2_SMALLINT;
    break;
    case SQL_NUMERIC:
      dbType = DB2_NUMERIC;
    break;
    case SQL_FLOAT:
      dbType = DB2_FLOAT;
    break;
    case SQL_REAL:
      dbType = DB2_REAL;
    break;
    case SQL_DOUBLE:
      dbType = DB2_DOUBLE;
    break;
    case SQL_DATETIME:
      dbType = DB2_DATETIME;
    break;
    case SQL_VARCHAR:
      dbType = DB2_VARCHAR;
    break;
    case SQL_BOOLEAN:
      dbType = DB2_BOOLEAN;
    break;
    case SQL_ROW:
      dbType = DB2_ROW;
    break;
    case SQL_WCHAR:
      dbType = DB2_WCHAR;
    break;
    case SQL_WLONGVARCHAR:
      dbType = DB2_WLONGVARCHAR;
    break;
    case SQL_DECFLOAT:
      dbType = DB2_DECFLOAT;
    break;
    case SQL_TYPE_DATE:
      dbType = DB2_TYPE_DATE;
    break;
    case SQL_TYPE_TIME:
      dbType = DB2_TYPE_TIME;
    break;
    case SQL_TYPE_TIMESTAMP:
      dbType = DB2_TYPE_TIMESTAMP;
    break;
    case SQL_TYPE_TIMESTAMP_WITH_TIMEZONE:
      dbType = DB2_TYPE_TIMESTAMP_WITH_TIMEZONE;
    break;
    case SQL_GRAPHIC:
      dbType = DB2_GRAPHIC;
    break;
    case SQL_VARGRAPHIC:
      dbType = DB2_VARGRAPHIC;
    break;
    case SQL_LONGVARGRAPHIC:
      dbType = DB2_LONGVARGRAPHIC;
    break;
    case SQL_BLOB:
      dbType = DB2_BLOB;
    break;
    case SQL_CLOB:
      dbType = DB2_CLOB;
    break;
    case SQL_DBCLOB:
      dbType = DB2_DBCLOB;
    break;
    case SQL_XML:
      dbType = DB2_XML;
    break;
    case SQL_LONGVARCHAR:
      dbType = DB2_LONGVARCHAR;
    break;
    case SQL_WVARCHAR:
      dbType = DB2_WVARCHAR;
    break;
    case SQL_BIGINT:
      dbType = DB2_BIGINT;
    break;
    case SQL_BINARY:
      dbType = DB2_BINARY;
    break;
    case SQL_VARBINARY:
     dbType = DB2_VARBINARY;
    break;
    case SQL_LONGVARBINARY:
      dbType = DB2_LONGVARBINARY;
    break;
    case SQL_UNKNOWN_TYPE:
    default: 
      dbType = DB2_UNKNOWN_TYPE;
    break;
  }
  return dbType;
}

/** c2name
 *    For debugging purpose provide a human readable text on a
 *    given SQL data type value.
 */
char* c2name(short fcType){
  char* name   = NULL;
  switch (fcType) {
    case SQL_CHAR:
      name = "SQL_CHAR";
    break;
    case SQL_DECIMAL:
      name = "SQL_DECIMAL";
    break;
    case SQL_INTEGER:
      name = "SQL_INTEGER";
    break;
    case SQL_SMALLINT:
      name = "SQL_SMALLINT";
    break;
    case SQL_NUMERIC:
      name = "SQL_NUMERIC";
    break;
    case SQL_FLOAT:
      name = "SQL_FLOAT";
    break;
    case SQL_REAL:
      name = "SQL_REAL";
    break;
    case SQL_DOUBLE:
      name = "SQL_DOUBLE";
    break;
    case SQL_DATETIME:
      name = "SQL_DATETIME";
    break;
    case SQL_VARCHAR:
      name = "SQL_VARCHAR";
    break;
    case SQL_BOOLEAN:
      name = "SQL_BOOLEAN";
    break;
    case SQL_ROW:
      name = "SQL_ROW";
    break;
    case SQL_WCHAR:
      name = "SQL_WCHAR";
    break;
    case SQL_WLONGVARCHAR:
      name = "SQL_WLONGVARCHAR";
    break;
    case SQL_DECFLOAT:
      name = "SQL_DECFLOAT";
    break;
    case SQL_TYPE_DATE:
      name = "SQL_TYPE_DATE";
    break;
    case SQL_TYPE_TIME:
      name = "SQL_TYPE_TIME";
    break;
    case SQL_TYPE_TIMESTAMP:
      name = "SQL_TYPE_TIMESTAMP";
    break;
    case SQL_TYPE_TIMESTAMP_WITH_TIMEZONE:
      name = "SQL_TYPE_TIMESTAMP_WITH_TIMEZONE";
    break;
    case SQL_GRAPHIC:
      name = "SQL_GRAPHIC";
    break;
    case SQL_VARGRAPHIC:
      name = "SQL_VARGRAPHIC";
    break;
    case SQL_LONGVARGRAPHIC:
      name = "SQL_LONGVARGRAPHIC";
    break;
    case SQL_BLOB:
      name = "SQL_BLOB";
    break;
    case SQL_CLOB:
      name = "SQL_CLOB";
    break;
    case SQL_DBCLOB:
      name = "SQL_DBCLOB";
    break;
    case SQL_XML:
      name = "SQL_XML";
    break;
    case SQL_LONGVARCHAR:
      name = "SQL_LONGVARCHAR";
    break;
    case SQL_WVARCHAR:
      name = "SQL_WVARCHAR";
    break;
    case SQL_BIGINT:
      name = "SQL_BIGINT";
    break;
    case SQL_BINARY:
      name = "SQL_BINARY";
    break;
    case SQL_VARBINARY:
     name = "SQL_VARBINARY";
    break;
    case SQL_LONGVARBINARY:
      name = "SQL_LONGVARBINARY";
    break;
    case SQL_UNKNOWN_TYPE:
    default: 
      name = "SQL_UNKNOWN_TYPE";
    break;
  }
  return name;
}