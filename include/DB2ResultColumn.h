#ifndef DB2RESULTCOLUMN_H
#define DB2RESULTCOLUMN_H

/*
 * ODBC/CLI uses SQLLEN* as the indicator pointer for SQLBindCol.
 *
 * This header is also included by PostgreSQL-facing translation units where
 * the DB2 CLI headers cannot be included.  In addition, SQLLEN is 32 bit for
 * the native Db2 CLI build and 64 bit for some ODBC builds.  Binding the
 * driver directly to an intptr_t therefore gives it an object of the wrong
 * effective type on at least one of these builds.
 *
 * Keep an aligned, opaque eight-byte area for the driver-owned SQLLEN value.
 * DB2 CLI translation units copy the value to val_null after each fetch.  That
 * keeps the SQLBindCol ABI separate from the portable result metadata.
 */
#include <stdint.h>

typedef union db2ResultIndicatorStorage {
  int64_t       alignment;
  unsigned char bytes[8];
} DB2ResultIndicatorStorage;

/*
 * Defines how a DB2 cursor result column maps to a PostgreSQL tuple slot.
 *
 * ForeignScan slots that have an fdw_scan_tlist are laid out in remote result
 * order, even for scans of a base relation.  ANALYZE and DML RETURNING slots,
 * on the other hand, use the physical relation attribute order.  The two
 * layouts cannot be inferred from the number of slot attributes: a reordered
 * SELECT of every table column has the same attribute count as the relation.
 */
typedef enum db2TupleIndexMode {
  DB2_TUPLE_INDEX_ATTRIBUTE,
  DB2_TUPLE_INDEX_RESULT
} DB2TupleIndexMode;
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
  intptr_t                val_null;      // normalized NULL/length indicator used by PostgreSQL-facing code
  DB2ResultIndicatorStorage val_indicator; // aligned SQLLEN storage owned by DB2 CLI while a column is bound
  db2NoEncErrType         noencerr;      // no encoding error produced
  struct db2ResultColumn* next;
} DB2ResultColumn;

#endif
