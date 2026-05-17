#include <postgres.h>
#include <commands/explain.h>
#include "db2_fdw.h"
#include "DB2FdwState.h"

/** external prototypes */
extern void         db2BeginForeignModifyCommon(ModifyTableState* mtstate, ResultRelInfo* rinfo, DB2FdwState* fdw_state, Plan* subplan);
extern DB2FdwState* deserializePlanData        (List* list);

/** local prototypes */
void db2BeginForeignModify (ModifyTableState* mtstate, ResultRelInfo* rinfo, List* fdw_private, int subplan_index, int eflags);

/* db2BeginForeignModify
 * Prepare everything for the DML query:
 * The SQL statement is prepared, the type output functions for the parameters are fetched, and the column numbers of the resjunk attributes are stored in the "pkey" field.
 */
void db2BeginForeignModify (ModifyTableState* mtstate, ResultRelInfo* rinfo, List* fdw_private, int subplan_index, int eflags) {
  DB2FdwState* fdw_state = deserializePlanData (fdw_private);
  Plan        *subplan   = NULL;

  db2Entry1();
  #if PG_VERSION_NUM < 140000
  subplan   = mtstate->mt_plans[subplan_index]->plan;
  #else
  subplan   = outerPlanState(mtstate)->plan;
  #endif

  db2BeginForeignModifyCommon(mtstate, rinfo, fdw_state, subplan);
  db2Exit1();
}