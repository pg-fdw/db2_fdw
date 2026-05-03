/*-------------------------------------------------------------------------
 *
 * db2_fdw.h
 *   This header file contains all definitions that are shared by db2_fdw.c
 *   and db2_utils.c.
 *   It is necessary to split db2xa_fdw into two source files because
 *   PostgreSQL and DB2 headers cannot be #included at the same time.
 *
 *-------------------------------------------------------------------------
 */
#ifndef _db2_fdw_h_
#define _db2_fdw_h_

#ifdef POSTGRES_H
#if PG_VERSION_NUM >= 150000
#define STRVAL(arg) ((String *)(arg))->sval
#else
#define STRVAL(arg) ((Value*)(arg))->val.str
#endif

#if PG_VERSION_NUM < 130000
#error "This extension requires PostgreSQL version 13.0 or higher."
#endif

/* defined in backend/commands/analyze.c */
//#ifndef WIDTH_THRESHOLD
//#define WIDTH_THRESHOLD 1024
//#endif /* WIDTH_THRESHOLD */

/* array_create_iterator has a new signature from 9.5 on */
#define array_create_iterator(arr, slice_ndim) array_create_iterator(arr, slice_ndim, NULL)
#define JOIN_API

/* the useful macro IS_SIMPLE_REL is defined in v10, backport */
#ifndef IS_SIMPLE_REL
#define IS_SIMPLE_REL(rel) \
  ((rel)->reloptkind == RELOPT_BASEREL || \
  (rel)->reloptkind == RELOPT_OTHER_MEMBER_REL)
#endif

/* GetConfigOptionByName has a new signature from 9.6 on */
#define GetConfigOptionByName(name, varname) GetConfigOptionByName(name, varname, false)


/* list API has changed in v13 */
#define list_next(l, e) lnext((l), (e))
#define do_each_cell(cell, list, element) for_each_cell(cell, (list), (element))

/* older versions don't have JSONOID */
#ifndef JSONOID
#define JSONOID InvalidOid
#endif

/* "table_open" was "heap_open" before v12 */
#endif /* POSTGRES_H */

/* this one is safe to include and gives us Oid */
/*
#include <postgres_ext.h>
#include <stdbool.h>
#include <sys/types.h>
*/

/* db2_fdw version */
#define DB2_FDW_VERSION "18.1.2"
/* number of bytes to read per LOB chunk */
#define LOB_CHUNK_SIZE    8192
#define ERRBUFSIZE        2000
#define SUBMESSAGE_LEN    200
#define EXPLAIN_LINE_SIZE 1000
#define DEFAULT_MAX_LONG  32767
#define DEFAULT_PREFETCH  200
#define DEFAULT_BATCHSZ   100
#define TABLE_NAME_LEN    129
#define COLUMN_NAME_LEN   129
#define SQLSTATE_LEN      6

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

/*
#ifndef SQL_H_SQLCLI1
#include "DB2FdwState.h"
#include "DB2FdwOption.h"
#endif
*/

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
#define OPT_SAMPLE            "sample_percent"
#define OPT_PREFETCH          "prefetch"
#define OPT_NO_ENCODING_ERROR "no_encoding_error"
#define OPT_BATCH_SIZE        "batch_size"

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

/** Options for case folding for names in IMPORT FOREIGN TABLE.
 */
typedef enum { CASE_KEEP, CASE_LOWER, CASE_SMART } fold_t;

#define REL_ALIAS_PREFIX    "r"
/* Handy macro to add relation name qualification */
#define ADD_REL_QUALIFIER(buf, varno)  appendStringInfo((buf), "%s%d.", REL_ALIAS_PREFIX, (varno))
#define serializeInt(x)                makeConst(INT4OID, -1, InvalidOid, 4, Int32GetDatum((int32)(x)), false, true)
#define serializeOid(x)                makeConst(OIDOID, -1, InvalidOid, 4, ObjectIdGetDatum(x), false, true)

#ifndef MIN
#define MIN(a, b) ((a) < (b) ? (a) : (b))
#endif

#endif