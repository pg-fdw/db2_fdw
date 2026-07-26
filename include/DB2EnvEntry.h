#ifndef ENVENTRY_H
#define ENVENTRY_H
/** DB2EnvEntry
 *  There is at most one DB2 environment handle per backend, cached in the
 *  global "rootenvEntry".
 *
 *  Attached to it is a pure forward linked list of Db2ConnEntry elements
 *  in "connlist". By that the code is able to reuse any active connection handle in that
 *  chain.
 *
 *  @see    DB2ConnEntry.h for more details on a DB2ConnEntry element of connlist.
 *  @author Ing. Wolfgang Brandl
 *  @since  1.0
 */
typedef struct envEntry {
  SQLHENV             henv;       // SQL environment handle
  DB2ConnEntry*       connlist;   // double linked list of DB2ConnEntry elements
} DB2EnvEntry;

#endif