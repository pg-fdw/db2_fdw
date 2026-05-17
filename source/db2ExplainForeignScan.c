#include <postgres.h>
#include <commands/explain.h>
#if PG_VERSION_NUM >= 180000
#include <commands/explain_state.h>
#include <commands/explain_format.h>
#endif
#include <optimizer/optimizer.h>
#include <access/heapam.h>
#include "db2_fdw.h"
#include "DB2FdwState.h"

/** external prototypes */

/** local prototypes */
       void db2ExplainForeignScan(ForeignScanState* node, ExplainState* es);
static void db2Explain           (void* fdw, ExplainState* es);

/* db2ExplainForeignScan
 * Produce extra output for EXPLAIN:
 * the DB2 query and, if VERBOSE was given, the execution plan.
 */
void db2ExplainForeignScan (ForeignScanState* node, ExplainState* es) {
  DB2FdwState* fdw_state = (DB2FdwState*) node->fdw_state;
  db2Entry1();
  db2Debug2("db2_fdw: explain foreign table scan");
  ExplainPropertyText ("DB2 query", fdw_state->query, es);
  db2Explain (fdw_state, es);
  db2Exit1();
}

/* db2Explain */
static void db2Explain (void* fdw, ExplainState* es) {
  FILE*        fp;
  char         path[1035];
  StringInfoData execution_cmd;
  DB2FdwState* fdw_state = (DB2FdwState*) fdw;
  int          count     = 0;
  int          qlength   = strlen(fdw_state->query);
  char*        tempQuery = NULL;
  char*        src       = fdw_state->query;
  char*        dest      = NULL;

  db2Entry1();
  for (const char* p = src; *p; p++) {
    if (*p == '"') count++;
  }
  tempQuery = db2alloc(qlength+count+1,"tempQuery");
  dest      = tempQuery;
  src       = fdw_state->query;
  while(*src){
    if (*src == '"') {
      *dest++ = '\\';
    }
    *dest++ = *(src);
    src++;
  }
  *dest = '\0';

  initStringInfo(&execution_cmd);
  if (es->verbose) {
    if (strlen(fdw_state->user)) {
      appendStringInfo(&execution_cmd,
                       "db2expln -t -d %s -u %s %s -q \"%s\" ",
                       fdw_state->dbserver,
                       fdw_state->user,
                       fdw_state->password,
                       tempQuery);
    } else {
      appendStringInfo(&execution_cmd,
                       "db2expln -t -d %s -q \"%s\" ",
                       fdw_state->dbserver,
                       tempQuery);
    }
  } else {
    if (strlen(fdw_state->user)) {
      appendStringInfo(&execution_cmd,
                       "db2expln -t -d %s -u %s %s -q \"%s\" |grep -E \"Estimated Cost|Estimated Cardinality\" ",
                       fdw_state->dbserver,
                       fdw_state->user,
                       fdw_state->password,
                       tempQuery);
    } else {
      appendStringInfo(&execution_cmd,
                       "db2expln -t -d %s -q \"%s\" |grep -E \"Estimated Cost|Estimated Cardinality\" ",
                       fdw_state->dbserver,
                       tempQuery);
    }
  }
  db2Debug2("execution_cmd: '%s'", execution_cmd.data);
  /* Open the command for reading. */
  fp = popen(execution_cmd.data, "r");
  if (fp == NULL) {
    int save_errno = errno;
    db2free(tempQuery,"tempQuery");
    pfree(execution_cmd.data);
    ereport(ERROR,
            (errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
             errmsg("db2_fdw: failed to run db2expln"),
             errdetail("popen() failed: %s", strerror(save_errno))));
  }

  /* Read the output a line at a time - output it. */
  while (fgets(path, sizeof(path)-1, fp) != NULL) {
    path[strlen (path) - 1] = '\0';
    ExplainPropertyText ("DB2 plan", path, es);
  }
  /* close */
  pclose(fp);
  db2free(tempQuery,"tempQuery");
  pfree(execution_cmd.data);
  db2Exit1();
}
