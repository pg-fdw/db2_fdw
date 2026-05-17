#include <string.h>
#include <postgres.h>
#include <miscadmin.h>
#include <utils/guc.h>
#include "db2_fdw.h"

_Thread_local static int debug_depth = 0;

/* get a PostgreSQL error code from an db2error */
#define to_sqlstate(x) \
  (x==FDW_UNABLE_TO_ESTABLISH_CONNECTION ? ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION : \
  (x==FDW_UNABLE_TO_CREATE_REPLY ? ERRCODE_FDW_UNABLE_TO_CREATE_REPLY : \
  (x==FDW_TABLE_NOT_FOUND ? ERRCODE_FDW_TABLE_NOT_FOUND : \
  (x==FDW_UNABLE_TO_CREATE_EXECUTION ? ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION : \
  (x==FDW_OUT_OF_MEMORY ? ERRCODE_FDW_OUT_OF_MEMORY : \
  (x==FDW_SERIALIZATION_FAILURE ? ERRCODE_T_R_SERIALIZATION_FAILURE : ERRCODE_FDW_ERROR))))))

/** local prototype */
void db2Error    (db2error sqlstate, const char* message);
void db2Error_d  (db2error sqlstate, const char* message, const char* detail, ...) __attribute__ ((format (gnu_printf, 2, 0)));

/* db2Error_d
 * Report a PostgreSQL error with a detail message.
 */
void db2Error_d (db2error sqlstate, const char *message, const char *detail, ...) {
  char    cBuffer [4000];
  va_list arg_marker;
  /* if the backend was terminated, report that rather than the DB2 error */
  CHECK_FOR_INTERRUPTS ();
  va_start(arg_marker, detail);
  vsnprintf(cBuffer, sizeof(cBuffer), detail, arg_marker);
  ereport (ERROR, (errcode (to_sqlstate (sqlstate)), errmsg ("%s", message), errdetail ("%s", cBuffer)));
  va_end  (arg_marker);
}

/* db2error
 * Report a PostgreSQL error without detail message.
 */
void db2Error (db2error sqlstate, const char *message) {
  /* use errcode_for_file_access() if the message contains %m */
  if (strstr(message, "%m")) {
    ereport (ERROR, (errcode_for_file_access (), errmsg (message, "")));
  } else {
    ereport (ERROR, (errcode (to_sqlstate (sqlstate)), errmsg ("%s", message)));
  }
}

int isLogLevel(int level) {
  return (level >= log_min_messages);
}

void db2EntryExit(int level, int entry, const char* message, ...) {
  if (db2IsLogEnabled(level)) {
    char    cBuffer [4000];
    va_list arg_marker;
    va_start(arg_marker, message);
    vsnprintf (cBuffer, sizeof(cBuffer), message, arg_marker);

    if (entry == 1) {
        db2Debug(level, cBuffer);
        ++debug_depth;

    } else {
      --debug_depth;
      db2Debug(level, cBuffer);
    }
  }
}

void db2Debug(int level, const char* message, ...) {
  if (db2IsLogEnabled(level)) {
    char    cBuffer [4000];
    int     dLevel  = DEBUG5;
    int     offset  = (2*debug_depth);
    va_list arg_marker;

    memset(cBuffer, ' ', offset);
    cBuffer[offset] = '\0';

    va_start (arg_marker, message);
    vsnprintf (cBuffer+offset, sizeof(cBuffer)-offset, message, arg_marker);
    switch(level){
      case 1:
      dLevel = DEBUG1;
      break;
      case 2:
      dLevel = DEBUG2;
      break;
      case 3:
      dLevel = DEBUG3;
      break;
      case 4:
      dLevel = DEBUG4;
      break;
      case 5:
      default:
      dLevel = DEBUG5;
      break;
    }
    elog (dLevel, "%s", cBuffer);
    va_end   (arg_marker);
  }
}
