#include <string.h>
#include <stdio.h>
#include "db2_fdw.h"

/** global variables  */
int                 err_code = 0;          /* error code, set by db2CheckErr()                              */
char                db2Message[ERRBUFSIZE];/* contains DB2 error messages, set by db2CheckErr()             */

/** external variables */

/** external prototypes */

/** local prototypes */
SQLRETURN db2CheckErr (SQLRETURN status, SQLHANDLE handle, SQLSMALLINT handleType, int line, char* file);

/* db2CheckErr
 * Call SQLGetDiagRec to get sqlcode, sqlstate and db2 error message.
 * It sets the global err_code with a value, so subsequent code can evaluate.
 * It populates the db2Message with SQLCODE, SQLSTATE and the DB2 message text.
 * It modifys the result to SQL_SUCCESS in case the status was SQL_SUCCESS_WITH_INFO.
 * It sets err_code to 100 upon SQL_NO_DATA.
 * 
 * @param status     the returncode from a previous executed SQL API call
 * @param handle     the handle used in that previous SQL API call
 * @param handleType the type of handle used (HENV, HDBC, HSTMT, etc)
 * @param line       the source-code-line db2CheckErr was invoked from
 * @param file       the name of the sourcefile db2CheckErr was invoked from
 * 
 * @return SQLRETURN passing back the status, which in cases is modified
 * @since  1.0.0
 */
SQLRETURN db2CheckErr (SQLRETURN status, SQLHANDLE handle, SQLSMALLINT handleType, int line, char* file) {
  db2Entry4();
  memset (db2Message,0x00,sizeof(db2Message));
  switch (status) {
    case SQL_INVALID_HANDLE: {
      snprintf(db2Message,ERRBUFSIZE,"-CI INVALID HANDLE-----\nline=%d\nfile=%s\n",line,file);
      err_code = -1;
    }
    break;
    case SQL_ERROR: {
      SQLCHAR     message    [SQL_MAX_MESSAGE_LENGTH];
      SQLCHAR     submessage [SUBMESSAGE_LEN];
      SQLCHAR     sqlstate   [SQLSTATE_LEN];
      SQLINTEGER  sqlcode;
      SQLINTEGER  diag_column_number = 0;
      SQLLEN      diag_row_number = 0;
      char        diag_info   [128];
      SQLSMALLINT msgLen;
      int         i = 1;

      memset(submessage,0x00,SUBMESSAGE_LEN);
      memset(message   ,0x00,SQL_MAX_MESSAGE_LENGTH);
      memset(diag_info,0x00,sizeof(diag_info));
 
      while (SQL_SUCCEEDED(SQLGetDiagRec(handleType,handle,i,sqlstate,&sqlcode,message,SQL_MAX_MESSAGE_LENGTH,&msgLen))) {
        db2Debug5("SQLCODE :  %d ",sqlcode);
        db2Debug5("SQLSTATE:  %s ",sqlstate);
        db2Debug5("MESSAGE : '%s'",message);
        if (i == 1) {
          SQLRETURN diag_rc;
          diag_rc = SQLGetDiagField(handleType, handle, i, SQL_DIAG_COLUMN_NUMBER,
                                   &diag_column_number, (SQLSMALLINT) sizeof(diag_column_number), NULL);
          if (SQL_SUCCEEDED(diag_rc)) {
            db2Debug5("DIAG_COLUMN_NUMBER: %d", diag_column_number);
          }
          diag_rc = SQLGetDiagField(handleType, handle, i, SQL_DIAG_ROW_NUMBER,
                                   &diag_row_number, (SQLSMALLINT) sizeof(diag_row_number), NULL);
          if (SQL_SUCCEEDED(diag_rc)) {
            db2Debug5("DIAG_ROW_NUMBER   : %ld", (long) diag_row_number);
          }
          if (diag_column_number != 0 || diag_row_number != 0) {
            snprintf(diag_info, sizeof(diag_info),
                     "DIAG_COLUMN_NUMBER=%d\nDIAG_ROW_NUMBER=%ld\n",
                     diag_column_number, (long) diag_row_number);
          }
        }
        snprintf((char*)submessage, SUBMESSAGE_LEN, "SQLSTATE = %s  SQLCODE = %d\nline=%d\nfile=%s\n", sqlstate,sqlcode,line,file);
        if (diag_info[0] != '\0') {
          if ((sizeof(submessage) - strlen((char*)submessage)) > strlen(diag_info) + 1) {
            size_t avail = sizeof(submessage) - strlen((char*)submessage) - 1;
            strncat((char*)submessage, diag_info, avail);
          }
        }
        if ((sizeof(db2Message) - strlen((char*)db2Message)) > strlen((char*)submessage) + 1) {
          size_t avail = sizeof(db2Message) - strlen((char*)db2Message) - 1;
          strncat((char*)db2Message, (char*)submessage, avail);
        }
        if ((sizeof(db2Message) - strlen((char*)db2Message)) > strlen((char*)message) + 2) {
          size_t avail = sizeof(db2Message) - strlen((char*)db2Message) - 1;
          strncat((char*)db2Message, (char*)message, avail);
          avail = sizeof(db2Message) - strlen((char*)db2Message) - 1;
          strncat((char*)db2Message, "\n", avail);
        }
        if (i == 1) {
          err_code = ((sqlcode == -911 || sqlcode == -913) && strcmp((char*)sqlstate,"40001") == 0) ? 8177 : abs(sqlcode);
        }
        i++;
        memset(submessage,0x00,SUBMESSAGE_LEN);
        memset(message   ,0x00,SQL_MAX_MESSAGE_LENGTH);
      }
    }
    break;
    case SQL_SUCCESS_WITH_INFO: {
      status  = SQL_SUCCESS;
      err_code = 0;
    }
    break;
    case SQL_NO_DATA: {
      strncpy (db2Message, "SQL0100W: no data found", sizeof(db2Message));
      err_code = 100;
    }
    break;
  }
  db2Debug5("db2Message: '%s'",db2Message);
  db2Debug5("err_code  :  %d ",err_code);
  db2Exit4(": %d",status);
  return status;
}
