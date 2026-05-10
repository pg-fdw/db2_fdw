#ifndef DB2RESULTCOLUMN_H
#define DB2RESULTCOLUMN_H

/*
 * ODBC/CLI uses SQLLEN* as the indicator pointer for SQLBindCol.
 *
 * This header is included from PostgreSQL-facing code paths where we avoid
 * including DB2 CLI headers (sqlcli.h/sqlcli1.h) due to header conflicts.
 * Also, DB2's sqlcli.h defines SQLLEN as a macro, so attempting to typedef it
 * here is fragile.
 *
 * We therefore store the indicator in a toolchain-agnostic pointer-sized
 * integer, and cast to SQLLEN* only in the DB2-CLI compilation units.
 */
#include <stdint.h>
/** DB2ResultColumn
 *  A full descriptor of a DB2 table column and its corresponding PG column.
 * 
 *  @author Thomas Muenz
 *  @since  18.2.0
 */
typedef struct db2ResultColumn {
  char*                   colName;       // column name in DB2
  short                   colType;       // column data type in DB2
  size_t                  colSize;       // column size
  short                   colScale;      // column scale of size describing digits right of decimal point
  short                   colNulls;      // column is nullable
  size_t                  colChars;      // numer of characters fit in column size, it is less if UTF8, 16 or DBCS
  size_t                  colBytes;      // number of bytes representing colSize
  int                     colPrimKeyPart;// 1 if column is part of the primary key - only relevant for UPDATE or DELETE
  int                     colCodepage;   // codepage set for this column (only set on char columns), if 0 the content is binary
  int                     pgbaserelid;   // range table index of this column's relation
  char*                   pgname;        // PG column name
  int                     pgattnum;      // PG attribute number
  Oid                     pgtype;        // PG data type
  int                     pgtypmod;      // PG type modifier
  int                     pkey;          // nonzero for primary keys, later set to the resjunk attribute number
  int                     resnum;        // position of result in cursor 1 based
  char*                   val;           // buffer for DB2 to return results in (LOB locator for LOBs)
  size_t                  val_size;      // allocated size in val
  size_t                  val_len;       // actual length of val
  intptr_t                val_null;      // NULL indicator (cast to SQLLEN* at SQLBindCol)
  db2NoEncErrType         noencerr;      // no encoding error produced
  struct db2ResultColumn* next;
} DB2ResultColumn;

#endif