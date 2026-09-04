#include <string.h>
#include <stdio.h>
#include "db2_fdw.h"
#include "ParamDesc.h"

/** global variables */

/** external variables */
extern char         db2Message[ERRBUFSIZE];/* contains DB2 error messages, set by db2CheckErr()             */

/** external prototypes */
extern SQLRETURN    db2CheckErr          (SQLRETURN status, SQLHANDLE handle, SQLSMALLINT handleType, int line, char* file);
extern SQLSMALLINT  param2c              (SQLSMALLINT fcType);
extern char*        c2name               (short fcType);

/** internal prototypes */
void db2BindParameter (DB2Session* session, ParamDesc* param, SQLLEN* indicator, int param_count, int col_num);

void db2BindParameter (DB2Session* session, ParamDesc* param, SQLLEN* indicator, int param_count, int col_num) {
  SQLRETURN   rc           = 0;
  db2Entry1();
  db2Debug2("param_count     : %d",param_count);
  db2Debug2("col_num         : %d",col_num);
  db2Debug2("param->value    : %s",param->value);
  db2Debug2("param->colnum   : %d",param->colnum);
  db2Debug2("param->bindType : %d",param->bindType);
  if (param->colnum >= 0) {
    db2Debug2("colName         : %s",param->colName);
  }
  switch (param->bindType) {
      case BIND_NUMBER: {
        db2Debug3("param->bindType: BIND_NUMBER");
        *indicator = (SQLLEN) ((param->value == NULL) ? SQL_NULL_DATA : 0);
        db2Debug2("param_ind       : %d",*indicator);
        db2Debug2("colType         : %d - %s",param->colType,c2name(param->colType));
        switch (param->colType) {
          case SQL_BIGINT:{
            char*      end     = NULL;
            SQLBIGINT* sqlbint = NULL;
            if (param->value != NULL) {
              sqlbint  = db2alloc(sizeof(SQLBIGINT), "SQLBIGINT sqlbigint");
              *sqlbint = strtoll(param->value,&end,10);
              db2Debug2("sqlbint: %d",*sqlbint);
            }
            rc = SQLBindParameter( session->stmtp->hsql
                                 , col_num
                                 , SQL_PARAM_INPUT
                                 , SQL_C_SBIGINT
                                 , param->colType
                                 , 0
                                 , 0
                                 , sqlbint
                                 , 0
                                 , indicator
                                 );
          }
          break;
          case SQL_SMALLINT:{
            char*        end     = NULL;
            SQLSMALLINT* sqlsint = NULL;
            if (param->value != NULL) {
              sqlsint  = db2alloc(sizeof(SQLSMALLINT), "SQLSMALLINT sqlsint");
              *sqlsint = strtol(param->value,&end,10);
              db2Debug2("sqlsint: %d",*sqlsint);
            }
            rc = SQLBindParameter( session->stmtp->hsql
                                 , col_num
                                 , SQL_PARAM_INPUT
                                 , SQL_C_SSHORT
                                 , param->colType
                                 , 0
                                 , 0
                                 , sqlsint
                                 , 0
                                 , indicator
                                 );
          }
          break;
          case SQL_INTEGER: {
            char*       end    = NULL;
            SQLINTEGER* sqlint = NULL;
            if (param->value != NULL) {
              sqlint  = db2alloc(sizeof(SQLINTEGER),"SQLINTEGER sqlint");
              *sqlint = strtol(param->value,&end,10);
              db2Debug2("sqlint: %d",*sqlint);
            }
            rc = SQLBindParameter( session->stmtp->hsql
                                 , col_num
                                 , SQL_PARAM_INPUT
                                 , SQL_C_SLONG
                                 , param->colType
                                 , 0
                                 , 0
                                 , sqlint
                                 , 0
                                 , indicator
                                 );
          }
          break;
          case SQL_DECIMAL:
          case SQL_NUMERIC:
          case SQL_FLOAT:
          case SQL_REAL:
          case SQL_DOUBLE:
          case SQL_DECFLOAT: {
            /*
             * Bind numeric values as strings.
             * The previous SQL_C_NUMERIC + SQL_NUMERIC_STRUCT path relied on parse2num_struct(), which hard-coded precision/scale and could
             * trigger DB2 CLI0111E / SQLSTATE 22003 for values that don't match the target column's precision/scale.
             */
            *indicator = (SQLLEN) ((param->value == NULL) ? SQL_NULL_DATA : SQL_NTS);
            db2Debug2("param_ind       : %d",*indicator);

            rc = SQLBindParameter( session->stmtp->hsql
                                 , col_num
                                 , SQL_PARAM_INPUT
                                 , SQL_C_CHAR
                                 , param->colType
                                 , param->colSize
                                 , 0
                                 , (SQLPOINTER) param->value
                                 , 0
                                 , indicator
                                 );
          }
          break;
          default: {
            snprintf(db2Message, ERRBUFSIZE, "unsupported sql number type: %d - %s" , param->colType, c2name(param->colType)); 
            db2Error_d(FDW_UNABLE_TO_CREATE_EXECUTION, "error executing isrt query: unable to bind parameter", db2Message);
          }
          break;
        }
      }
      break;
      case BIND_STRING: {
        db2Debug3("param->bindType: BIND_STRING");
        *indicator = (SQLLEN) ((param->value == NULL) ? SQL_NULL_DATA : SQL_NTS);
        db2Debug2("param_ind       : %d",*indicator);
        rc = SQLBindParameter( session->stmtp->hsql
                             , col_num
                             , SQL_PARAM_INPUT
                             , SQL_C_CHAR
                             , SQL_VARCHAR
                             , param->colSize
                             , 0
                             , (SQLPOINTER) param->value
                             , 0
                             , indicator
                             );
      }
      break;
      case BIND_LONGRAW: {
        db2Debug3("param->bindType: BIND_LONGRAW");
        *indicator = (SQLLEN) ((param->value == NULL) ? SQL_NULL_DATA : SQL_NTS);
        db2Debug2("param_ind       : %d",*indicator);
        rc = SQLBindParameter( session->stmtp->hsql
                             , col_num
                             , SQL_PARAM_INPUT
                             , SQL_C_BINARY
                             , SQL_LONGVARBINARY
                             , param->colSize
                             , 0
                             , (SQLPOINTER) param->value
                             , 0
                             , indicator
                             );
      }
      break;
      case BIND_LONG: {
        db2Debug3("param->bindType: BIND_LONG");
        *indicator = (SQLLEN) ((param->value == NULL) ? SQL_NULL_DATA : SQL_NTS);
        db2Debug2("param_ind       : %d",*indicator);
        db2Debug2("param->value    : '%s'",param->value);
        rc = SQLBindParameter( session->stmtp->hsql
                             , col_num
                             , SQL_PARAM_INPUT
                             , SQL_C_CHAR
                             , SQL_LONGVARCHAR
                             , param->colSize
                             , 0
                             , (SQLPOINTER) param->value
                             , 0
                             , indicator
                             );
      }
      break;
      case BIND_OUTPUT: {
        SQLSMALLINT fcType;
        SQLSMALLINT fParamType;
        db2Debug2("param->bindType: BIND_OUTPUT");
        *indicator = (SQLLEN) ((param->value == NULL) ? SQL_NULL_DATA : 0);
        db2Debug2("param_ind       : %d",*indicator);
        if (param->type == UUIDOID) {
          /* the type input function will interpret the string value correctly */
          fcType = SQL_CHAR;
        } else {
          fcType = param->colType;
        }
        fParamType = param2c(fcType);
        rc = SQLBindParameter( session->stmtp->hsql
                             , param_count
                             , SQL_PARAM_OUTPUT
                             , fParamType
                             , fcType
                             , param->colSize
                             , 0
                             , (SQLPOINTER) param->value
                             , param->val_size
                             , indicator
                             );
      }
      break;
  }
  /* bind the value to the parameter */
  rc = db2CheckErr(rc,  session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
  if (rc != SQL_SUCCESS) {
    db2Error_d(FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: SQLBindParameter failed to bind parameter", db2Message);
  }
  db2Exit1();
}