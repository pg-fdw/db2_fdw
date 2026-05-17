#include <postgres.h>
#include <utils/elog.h>
#include <access/xact.h>
#include "db2_fdw.h"

/** eternal variables */
extern bool dml_in_transaction;

/** external prototypes */
extern void         db2EndTransaction         (void* arg, int is_commit, int noerror);
extern void         db2EndSubtransaction      (void* arg, int nest_level, int is_commit);

/** local prototypes */
void db2RegisterCallback   (void* arg);
void db2UnregisterCallback (void* arg);
void transactionCallback   (XactEvent event, void *arg);
void subtransactionCallback(SubXactEvent event, SubTransactionId mySubid, SubTransactionId parentSubid, void* arg);

/* db2RegisterCallback
 * Register a callback for PostgreSQL transaction events.
 */
void db2RegisterCallback (void *arg) {
  db2Entry1("(arg: %x)",arg);
  RegisterXactCallback (transactionCallback, arg);
  RegisterSubXactCallback (subtransactionCallback, arg);
  db2Exit1();
}

/* db2UnregisterCallback
 * Unregister a callback for PostgreSQL transaction events.
 */
void db2UnregisterCallback (void *arg) {
  db2Entry1("(arg: %x)",arg);
  UnregisterXactCallback (transactionCallback, arg);
  UnregisterSubXactCallback (subtransactionCallback, arg);
  db2Exit1();
}

/* transactionCallback
 * Commit or rollback DB2 transactions when appropriate.
 */
void transactionCallback (XactEvent event, void *arg) {
  db2Entry1("(event: %d, arg: %x)", event, arg);
  switch (event) {
    case XACT_EVENT_PRE_COMMIT:
    case XACT_EVENT_PARALLEL_PRE_COMMIT:
      /* remote commit */
      db2EndTransaction (arg, 1, 0);
    break;
    case XACT_EVENT_PRE_PREPARE:
      ereport (ERROR, (errcode (ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION), errmsg ("cannot prepare a transaction that used remote tables")));
    break;
    case XACT_EVENT_COMMIT:
    case XACT_EVENT_PREPARE:
    case XACT_EVENT_PARALLEL_COMMIT:
      /*
       * Commit the remote transaction ignoring errors.
       * In 9.3 or higher, the transaction must already be closed, so this does nothing.
       * In 9.2 or lower, this is ok since nothing can have been modified remotely.
       */
      db2EndTransaction (arg, 1, 1);
    break;
    case XACT_EVENT_ABORT:
    case XACT_EVENT_PARALLEL_ABORT:
      /* remote rollback */
      db2EndTransaction (arg, 0, 1);
    break;
  }
  dml_in_transaction = false;
  db2Exit1();
}

/* subtransactionCallback
 * Set or rollback to DB2 savepoints when appropriate.
 */
void subtransactionCallback (SubXactEvent event, SubTransactionId mySubid, SubTransactionId parentSubid, void *arg) {
    db2Entry1();
  /* rollback to the appropriate savepoint on subtransaction abort */
  if (event == SUBXACT_EVENT_ABORT_SUB || event == SUBXACT_EVENT_PRE_COMMIT_SUB)
    db2EndSubtransaction (arg, GetCurrentTransactionNestLevel (), event == SUBXACT_EVENT_PRE_COMMIT_SUB);
  db2Exit1();
}
