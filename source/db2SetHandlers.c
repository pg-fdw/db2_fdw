#include <postgres.h>
#include <tcop/tcopprot.h>
#include <libpq/pqsignal.h>
#include <optimizer/optimizer.h>
#include <access/heapam.h>
#include "db2_fdw.h"

/** external prototypes */
extern void         db2Cancel                 (void);

/** local prototypes */
void db2SetHandlers(void);
void db2Die        (SIGNAL_ARGS);

/* db2SetHandlers
 * Set signal handler for SIGTERM.
 */
void db2SetHandlers (void) {
  db2Entry5();
  pqsignal (SIGTERM, db2Die);
  db2Exit5();
}

/* db2Die
 * Terminate the current query and prepare backend shutdown.
 * This is a signal handler function.
 */
void db2Die (SIGNAL_ARGS) {
  db2Entry1();
  /* Terminate any running queries.
   * The DB2 sessions will be terminated by exitHook().
   */
  db2Cancel();
  /* Call the original backend shutdown function.
   * If a query was canceled above, an error from DB2 would result.
   * To have the backend report the correct FATAL error instead, we have to call CHECK_FOR_INTERRUPTS() before we report that error;
   * this is done in db2Error_d.
   */
  die (postgres_signal_arg);
  db2Exit1();
}