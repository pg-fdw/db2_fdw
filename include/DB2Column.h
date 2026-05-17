#ifndef DB2COLUMN_H
#define DB2COLUMN_H
/** DB2Column
 *  A full descriptor of a DB2 table column and its corresponding PG column.
 * 
 *  @author Ing. Wolfgang Brandl
 *  @since  1.0
 */
typedef struct db2Column {
  char*           colName;        // column name in DB2
  short           colType;        // column data type in DB2
  size_t          colSize;        // column size
  short           colScale;       // column scale of size describing digits right of decimal point
  short           colNulls;       // column is nullable
  size_t          colChars;       // numer of characters fit in column size, it is less if UTF8, 16 or DBCS
  size_t          colBytes;       // number of bytes representing colSize
  int             colPrimKeyPart; // 1 if column is part of the primary key - only relevant for UPDATE or DELETE
  int             colCodepage;    // codepage set for this column (only set on char columns), if 0 the content is binary
  int             pgrelid;        // range table index of this column's relation
  char*           pgname;         // PG column name
  int             pgattnum;       // PG attribute number
  Oid             pgtype;         // PG data type
  int             pgtypmod;       // PG type modifier
  int             used;           // is the column used in the query?
  int             pkey;           // nonzero for primary keys, later set to the resjunk attribute number
  size_t          val_size;       // allocated size in val
  db2NoEncErrType noencerr;       // no encoding error produced
} DB2Column;

#endif