#ifndef PARAMDESC_H
#define PARAMDESC_H
/** ParamDesc
 *  The descriptor of one parameter in the query. It might be a parameter receiving a result from a selected column
 *  or a parameter used in any of the where clauses.
 * 
 *  @author Ing Wolfgang Brandl
 *  @since  1.0
 */
typedef struct paramDesc {
  char*               colName;   // column name in DB2
  short               colType;   // column data type in DB2
  size_t              colSize;   // column size
  Oid                 type;      // PG data type
  db2BindType         bindType;  // which type to use for binding to DB2 statement
  char*               value;     // value rendered for DB2
  size_t              val_size;  // size to allocate val with in bytes
  void*               node;      // the executable expression
  int                 colnum;    // corresponding column in DB2Table (-1 in SELECT queries unless output column)
  int                 txts;      // transaction timestamp
  struct paramDesc*   next;      // next ParamDesc element in the list
} ParamDesc;
#endif