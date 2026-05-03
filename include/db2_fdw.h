/*-------------------------------------------------------------------------------
 *
 * db2_fdw.h
 * This header file contains all definitions that are shared by all source files.
 * It is necessary to split db2xa_fdw into two source files because
 * PostgreSQL and DB2 headers cannot be #included at the same time.
 *
 *-------------------------------------------------------------------------------
 */
#ifndef _db2_fdw_h_
#define _db2_fdw_h_

#ifdef POSTGRES_H

#include <foreign/foreign.h>
#include <foreign/fdwapi.h>
#include <nodes/pathnodes.h>

#define PG_SUPPORTED_MIN_VERSION 130000

#if PG_VERSION_NUM >= 150000
#define STRVAL(arg) ((String *)(arg))->sval
#else
#define STRVAL(arg) ((Value*)(arg))->val.str
#endif

#if PG_VERSION_NUM < PG_SUPPORTED_MIN_VERSION
#error "This extension requires PostgreSQL version 13.0 or higher."
#endif

#ifndef PQ_QUERY_PARAM_MAX_LIMIT
#define PQ_QUERY_PARAM_MAX_LIMIT 65535
#endif /* PQ_QUERY_PARAM_MAX_LIMIT */

#else  /* POSTGRES_H */

#include <sqlcli1.h>
#include <postgres_ext.h>

#endif /* POSTGRES_H */


/* db2_fdw version */
#define DB2_FDW_VERSION             "18.2.0"
/* number of bytes to read per LOB chunk */
#define LOB_CHUNK_SIZE              8192
#define ERRBUFSIZE                  2000
#define SUBMESSAGE_LEN              200
#define EXPLAIN_LINE_SIZE           1000
#define DEFAULT_MAX_LONG            32767
#define DEFAULT_PREFETCH            100
#define DEFAULT_FETCHSZ             1
#define FIXED_FETCH_SIZE
#define DEFAULT_BATCHSZ             100
#define TABLE_NAME_LEN              129
#define COLUMN_NAME_LEN             129
#define SQLSTATE_LEN                6
#define DB2_MAX_ATTR_PREFETCH_NROWS 1024
#define DB2_MAX_ATTR_ROW_ARRAY_SIZE 32768

/* Default CPU cost to start up a foreign query. */
#define DEFAULT_FDW_STARTUP_COST    100.0

/* Default CPU cost to process 1 row (above and beyond cpu_tuple_cost). */
#define DEFAULT_FDW_TUPLE_COST      0.2

/* If no remote estimates, assume a sort costs 20% extra */
#define DEFAULT_FDW_SORT_MULTIPLIER 1.2

#ifdef SQL_H_SQLCLI1
#include "HdlEntry.h"
#include "DB2ConnEntry.h"
#include "DB2EnvEntry.h"
#include "DB2Session.h"
typedef unsigned char DB2Text;
#endif
typedef struct db2Session DB2Session;

/* Some PostgreSQL versions have no constant definition for the OID of type uuid */
#ifndef UUIDOID
#define UUIDOID 2950
#endif

typedef enum {
  NO_ENC_ERR_NULL,
  NO_ENC_ERR_TRUE,
  NO_ENC_ERR_FALSE
} db2NoEncErrType;

#include "DB2Column.h"
#include "DB2Table.h"

/* types to store parameter descriprions */
typedef enum {
  BIND_STRING,
  BIND_NUMBER,
  BIND_LONG,
  BIND_LONGRAW,
  BIND_OUTPUT
} db2BindType;

/* PostgreSQL error messages we need */
typedef enum {
  FDW_ERROR,
  FDW_UNABLE_TO_ESTABLISH_CONNECTION,
  FDW_UNABLE_TO_CREATE_REPLY,
  FDW_UNABLE_TO_CREATE_EXECUTION,
  FDW_TABLE_NOT_FOUND,
  FDW_OUT_OF_MEMORY,
  FDW_SERIALIZATION_FAILURE
} db2error;

#define OPT_NLS_LANG          "nls_lang"
#define OPT_DBSERVER          "dbserver"
#define OPT_USER              "user"
#define OPT_PASSWORD          "password"
#define OPT_JWT_TOKEN         "jwt_token"
#define OPT_SCHEMA            "schema"
#define OPT_TABLE             "table"
#define OPT_MAX_LONG          "max_long"
#define OPT_READONLY          "readonly"
#define OPT_KEY               "key"

#define OPT_DB2TYPE           "db2type"
#define OPT_DB2SIZE           "db2size"
#define OPT_DB2BYTES          "db2bytes"
#define OPT_DB2CHARS          "db2chars"
#define OPT_DB2SCALE          "db2scale"
#define OPT_DB2NULL           "db2null"
#define OPT_DB2CCSID          "db2ccsid"

#define OPT_SAMPLE            "sample_percent"
#define OPT_PREFETCH          "prefetch"
#define OPT_NO_ENCODING_ERROR "no_encoding_error"
#define OPT_BATCH_SIZE        "batch_size"
#define OPT_FETCHSZ           "fetchsize"

/* types for the DB2 table description */
typedef enum {
  DB2_UNKNOWN_TYPE,
  DB2_CHAR,
  DB2_NUMERIC,
  DB2_DECIMAL,
  DB2_INTEGER,
  DB2_SMALLINT,
  DB2_FLOAT,
  DB2_REAL,
  DB2_DOUBLE,
  DB2_DATETIME,
  DB2_VARCHAR,
  DB2_BOOLEAN,
  DB2_ROW,
  DB2_WCHAR,
  DB2_WLONGVARCHAR,
  DB2_DECFLOAT,
  DB2_TYPE_DATE,
  DB2_TYPE_TIME,
  DB2_TYPE_TIMESTAMP,
  DB2_TYPE_TIMESTAMP_WITH_TIMEZONE,
  DB2_GRAPHIC,
  DB2_VARGRAPHIC,
  DB2_LONGVARGRAPHIC,
  DB2_BLOB,
  DB2_CLOB,
  DB2_DBCLOB,
  DB2_XML,
  DB2_LONGVARCHAR,
  DB2_WVARCHAR,
  DB2_BIGINT,
  DB2_BINARY,
  DB2_VARBINARY,
  DB2_LONGVARBINARY
} nonSQLType;

/* Options for case folding for names in IMPORT FOREIGN TABLE. */
typedef enum { CASE_KEEP, CASE_LOWER, CASE_SMART } fold_t;

#define REL_ALIAS_PREFIX            "r"
#define SUBQUERY_REL_ALIAS_PREFIX	"s"
#define SUBQUERY_COL_ALIAS_PREFIX	"c"

/* Handy macro to add relation name qualification */
#define ADD_REL_QUALIFIER(buf, varno)  appendStringInfo((buf), "%s%d.", REL_ALIAS_PREFIX, (varno))
#define serializeInt(x)                makeConst(INT4OID, -1, InvalidOid, 4, Int32GetDatum((int32)(x)), false, true)
#define serializeOid(x)                makeConst(OIDOID, -1, InvalidOid, 4, ObjectIdGetDatum(x), false, true)


extern void* db2Alloc  (size_t size, const char* message, ...) __attribute__ ((format (gnu_printf, 2, 0)));
extern void  db2Free   (void* p, const char* message, ...) __attribute__ ((format (gnu_printf, 2, 0)));
extern void* db2ReAlloc(size_t size, void* p, const char* message, ...) __attribute__ ((format (gnu_printf, 3, 0)));
extern char* db2StrDup (const char* source, const char* message, ...) __attribute__ ((format (gnu_printf, 2, 0)));

#define db2alloc(size, msg, ...) \
    db2Alloc( size, "%d %s:%s %s", __LINE__, __FILE__, __func__, msg, ##__VA_ARGS__)

#define db2free(pvoid, msg, ...) \
    db2Free( pvoid, "%d %s:%s %s", __LINE__, __FILE__, __func__, msg, ##__VA_ARGS__)

#define db2realloc(size, pvoid, msg, ...) \
    db2ReAlloc( size, pvoid, "%d %s:%s %s", __LINE__, __FILE__, __func__, msg, ##__VA_ARGS__)

#define db2strdup(src, msg, ...) \
    db2StrDup( src, "%d %s:%s %s", __LINE__, __FILE__, __func__, msg, ##__VA_ARGS__)


// keep the values in sync with the LogLevels defined in elog.h of PostgreSQL
// output, that only appears in log
#define DB2DEBUG5    10
#define DB2DEBUG4    11
#define DB2DEBUG3    12
#define DB2DEBUG2    13
#define DB2DEBUG1    14
#define DB2LOG       15
// output, that only appears not in log
#define DB2LINFO     17
#define DB2LNOTICE   18
#define DB2LWARNING  20
#define DB2LERROR    21

extern int  isLogLevel  (int level);
extern void db2EntryExit(int level, int entry, const char* message, ...) __attribute__ ((format (gnu_printf, 3, 0)));
extern void db2Debug    (int level, const char* message, ...) __attribute__ ((format (gnu_printf, 2, 0)));

#define db2IsLogEnabled(level) \
    isLogLevel(level)

#define db2Entry1(fmt, ...) \
    db2EntryExit(DB2DEBUG1, 1, "> %s:%d:%s" fmt, __FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define db2Entry2(fmt, ...) \
    db2EntryExit(DB2DEBUG2, 1, "> %s:%d:%s" fmt, __FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define db2Entry3(fmt, ...) \
    db2EntryExit(DB2DEBUG3, 1, "> %s:%d:%s" fmt, __FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define db2Entry4(fmt, ...) \
    db2EntryExit(DB2DEBUG4, 1, "> %s:%d:%s" fmt, __FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define db2Entry5(fmt, ...) \
    db2EntryExit(DB2DEBUG5, 1, "> %s:%d:%s" fmt, __FILE__, __LINE__, __func__, ##__VA_ARGS__)

#define db2Exit1(fmt, ...) \
    db2EntryExit(DB2DEBUG1, 0, "< %s:%d:%s" fmt, __FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define db2Exit2(fmt, ...) \
    db2EntryExit(DB2DEBUG2, 0, "< %s:%d:%s" fmt, __FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define db2Exit3(fmt, ...) \
    db2EntryExit(DB2DEBUG3, 0, "< %s:%d:%s" fmt, __FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define db2Exit4(fmt, ...) \
    db2EntryExit(DB2DEBUG4, 0, "< %s:%d:%s" fmt, __FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define db2Exit5(fmt, ...) \
    db2EntryExit(DB2DEBUG5, 0, "< %s:%d:%s" fmt, __FILE__, __LINE__, __func__, ##__VA_ARGS__)

#define db2Debug1(fmt, ...) \
    db2Debug(DB2DEBUG1, "1 %s:%d:%s " fmt, __FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define db2Debug2(fmt, ...) \
    db2Debug(DB2DEBUG2, "2 %s:%d:%s " fmt, __FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define db2Debug3(fmt, ...) \
    db2Debug(DB2DEBUG3, "3 %s:%d:%s " fmt, __FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define db2Debug4(fmt, ...) \
    db2Debug(DB2DEBUG4, "4 %s:%d:%s " fmt, __FILE__, __LINE__, __func__, ##__VA_ARGS__)
#define db2Debug5(fmt, ...) \
    db2Debug(DB2DEBUG5, "5 %s:%d:%s " fmt, __FILE__, __LINE__, __func__, ##__VA_ARGS__)

#define db2Log(fmt, ...) \
    db2Debug(DB2LOG, "l %s:%d:%s " fmt, __FILE__, __LINE__, __func__, ##__VA_ARGS__)

#define db2LogInfo(fmt, ...) \
    db2Debug(DB2LINFO, "i %s:%d:%s " fmt, __FILE__, __LINE__, __func__, ##__VA_ARGS__)

#define db2LogNotice(fmt, ...) \
    db2Debug(DB2LNOTICE, "n %s:%d:%s " fmt, __FILE__, __LINE__, __func__, ##__VA_ARGS__)

#define db2LogWarn(fmt, ...) \
    db2Debug(DB2LWARNING, "w %s:%d:%s " fmt, __FILE__, __LINE__, __func__, ##__VA_ARGS__)
    
#define db2LogError(fmt, ...) \
    db2Debug(DB2LERROR, "e %s:%d:%s " fmt, __FILE__, __LINE__, __func__, ##__VA_ARGS__)

#ifndef MIN
#define MIN(a, b) ((a) < (b) ? (a) : (b))
#endif

#endif