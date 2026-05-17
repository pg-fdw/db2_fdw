#include <postgres.h>
#include <commands/explain.h>
#include <commands/vacuum.h>
#include <utils/builtins.h>
#include <utils/syscache.h>
#include <optimizer/optimizer.h>
#include <access/heapam.h>
#include "db2_fdw.h"
#include "DB2FdwState.h"
#include "DB2Column.h"

/** external prototypes */
extern DB2FdwState* db2GetFdwState             (Oid foreigntableid, double* sample_percent, bool describe);
extern void         addParam                   (ParamDesc** paramList, DB2Column* db2col, int colnum, int txts);
extern void         checkDataType              (short db2type, int scale, Oid pgtype, const char* tablename, const char* colname);
extern void         appendAsType               (StringInfoData* dest, Oid type);
extern void         db2BeginForeignModifyCommon(ModifyTableState* mtstate, ResultRelInfo* rinfo, DB2FdwState* fdw_state, Plan* subplan);

/** local prototypes */
void                db2BeginForeignInsert      (ModifyTableState* mtstate, ResultRelInfo* rinfo);
DB2FdwState*        db2BuildInsertFdwState     (Relation rel);

void db2BeginForeignInsert(ModifyTableState* mtstate, ResultRelInfo* rinfo) {
  Relation     rel       = rinfo->ri_RelationDesc;
  DB2FdwState* fdw_state = NULL;

  db2Entry1();
  fdw_state = db2BuildInsertFdwState(rel);
  /* subplan is irrelevant for pure INSERT/COPY */
  db2BeginForeignModifyCommon(mtstate, rinfo, fdw_state, NULL);
  db2Exit1();
}

DB2FdwState* db2BuildInsertFdwState(Relation rel) {
  DB2FdwState*    fdwState;
  StringInfoData  sql;
  int             i;
  bool            firstcol;

  db2Entry1();
  /* Same logic as CMD_INSERT branch of db2PlanForeignModify: */
  fdwState = db2GetFdwState(RelationGetRelid(rel), NULL, true);
  initStringInfo(&sql);
  appendStringInfo(&sql, "INSERT INTO %s (", fdwState->db2Table->name);
  firstcol = true;
  for (i = 0; i < fdwState->db2Table->ncols; ++i) {
    if (fdwState->db2Table->cols[i]->pgname == NULL) {
      continue;
    }
    appendStringInfo(&sql, "%s%s", (firstcol) ? "" : ", ", fdwState->db2Table->cols[i]->colName);
    firstcol = false;
  }
  appendStringInfo(&sql, ") VALUES (");
  firstcol = true;
  for (i = 0; i < fdwState->db2Table->ncols; ++i) {
    if (fdwState->db2Table->cols[i]->pgname == NULL)
      continue;
    checkDataType(fdwState->db2Table->cols[i]->colType,
                  fdwState->db2Table->cols[i]->colScale,
                  fdwState->db2Table->cols[i]->pgtype,
                  fdwState->db2Table->pgname,
                  fdwState->db2Table->cols[i]->pgname);
    addParam(&fdwState->paramList, fdwState->db2Table->cols[i], i, 0);
    if (!firstcol)
      appendStringInfo(&sql, ", ");
    else
      firstcol = false;
    appendAsType(&sql, fdwState->db2Table->cols[i]->pgtype);
  }
  appendStringInfo(&sql, ")");
  fdwState->query = sql.data;
  db2Debug(2,"fdwState->query: '%s'",sql.data);
  db2Exit1(": %x",fdwState);
  return fdwState;
}