#include <postgres.h>
#if PG_VERSION_NUM >= 140000
#include <nodes/makefuncs.h>
#include "db2_fdw.h"

/** external variables */

/** external prototypes */
extern TupleTableSlot* db2ExecForeignInsert      (EState* estate, ResultRelInfo* rinfo, TupleTableSlot* slot, TupleTableSlot* planSlot);

/** local prototypes */
TupleTableSlot**       db2ExecForeignBatchInsert (EState *estate, ResultRelInfo *rinfo, TupleTableSlot **slots, TupleTableSlot **planSlots, int *numSlots);

/* db2ExecForeignBatchInsert
 * Called when the executor wants to insert multiple rows in one go.
 * For now we just loop and reuse db2ExecForeignInsert for each slot.
 *
 * The executor expects the returned array to point to slots containing the inserted rows (or RETURNING results). We simply reuse the input slots array.
 */
TupleTableSlot ** db2ExecForeignBatchInsert(EState *estate, ResultRelInfo *rinfo, TupleTableSlot **slots, TupleTableSlot **planSlots, int *numSlots) {
  int i;
  db2Entry1();
  /* According to the FDW API, this is *not* used when there is a RETURNING clause, so normally these inserts don't need to produce a result tuple. 
   * However, to be safe and to match the ExecForeignInsert semantics, we just call db2ExecForeignInsert and keep its results in the same slots.
   */
  for (i = 0; i < *numSlots; i++) {
    if (slots[i] == NULL)
      continue;   /* paranoia */
    db2ExecForeignInsert(estate, rinfo, slots[i], planSlots ? planSlots[i] : NULL);
  }
  /* All results are in slots[0..*numSlots - 1]. */
  db2Exit1(": %x", slots);
  return slots;
}
#endif