#include <postgres.h>
#include <nodes/makefuncs.h>
#include "db2_fdw.h"

/** external variables */

/** external prototypes */
extern void         db2EndForeignModifyCommon(EState *estate, ResultRelInfo *rinfo);

/** local prototypes */
void                db2EndForeignModify      (EState* estate, ResultRelInfo* rinfo);

/* db2EndForeignModify
 * Close the currently active DB2 statement.
 */
void db2EndForeignModify (EState* estate, ResultRelInfo* rinfo) {
  db2Entry1();
  db2EndForeignModifyCommon(estate, rinfo);
  db2Exit1();
}

