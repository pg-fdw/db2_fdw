#include "db2_fdw.h"

/** external variables */

/** external prototypes */
extern void      db2Error             (db2error sqlstate, const char* message);
extern SQLRETURN db2CheckErr          (SQLRETURN status, SQLHANDLE handle, SQLSMALLINT handleType, int line, char* file);

/** local prototypes */
       void      db2FreeStmtHdl       (HdlEntry* handlep, DB2ConnEntry* connp);
static HdlEntry* findhdlEntry         (HdlEntry* start, SQLHANDLE hsql);

/** db2FreeStmtHdl
 *  release a DB2 statement handle, remove it from the cached list.
 */
void db2FreeStmtHdl (HdlEntry* handlep, DB2ConnEntry* connp) {
  HdlEntry* entryp      = handlep;
  HdlEntry* prev_entryp = NULL;
  SQLRETURN rc          = 0;

  db2Entry1("(handlep: %x, connp: %x)",handlep, connp);
  db2Debug2("handlep: %x ->hsql: %d ->type: %d ->next: %x", handlep, handlep->hsql, handlep->type, handlep->next);
  db2Debug2("connp  : %x ->handlelist: %x", connp, connp->handlelist);

  /* find the predecessor of handlep in the list of handles starting from connp->handlelist*/
  prev_entryp = findhdlEntry(connp->handlelist, handlep->hsql);
  /* remember prev_entryp might be actually the root element at conp->handlelist*/
  db2Debug3("prev_entryp: %x ->hsql : %d ->type : %d->next : %x", prev_entryp, prev_entryp->hsql, prev_entryp->type, prev_entryp->next);

  /* release the handle */
  rc = SQLFreeHandle(handlep->type, handlep->hsql);
  rc = db2CheckErr(rc, handlep->hsql, handlep->type, __LINE__, __FILE__ );

  /* remove it */
  if (connp->handlelist == prev_entryp) {
    /* we closed the one and only element of connp->handlelist */
    /* entryp->next must be NULL, so it is safe to assign it to connp->handlelist*/
    connp->handlelist = entryp->next;
    db2Debug3("connp->handlelist: '%x'", connp->handlelist);
  } else {
    /* we closed one element of connp->handlelist */
    /* here we need to set handlep->next to prev_entryp->next isolating entryp for subsequent release*/
    prev_entryp->next = handlep->next;
    db2Debug3("prev_entryp->next: '%x'", prev_entryp->next);
  }
  db2Debug2("HdlEntry freeed: %x",entryp);
  free (entryp);
  db2Exit1();
}

/* findhdlEntry */
static HdlEntry* findhdlEntry (HdlEntry* start, SQLHANDLE hsql) {
  HdlEntry* step = NULL;
  HdlEntry* prev = start;
  db2Entry4();
  for (step = start; step != NULL; step = step->next){
    if (step->hsql == hsql) {
      break;
    }
    prev = step;
  }
  db2Exit4(": %x", prev);
  return prev;
}
