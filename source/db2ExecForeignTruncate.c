#include <postgres.h>
#if PG_VERSION_NUM >= 140000
#include <utils/rel.h>
#include <access/xact.h>
#include "db2_fdw.h"
#include "DB2FdwState.h"

/** external prototypes */
extern DB2FdwState* db2GetFdwState       (Oid foreigntableid, double* sample_percent, bool drescribe);
extern DB2Session*  db2GetSession        (const char* connectstring, char* user, char* password, char* jwt_token, const char* nls_lang, int curlevel);
extern int          db2ExecuteTruncate   (DB2Session* session, const char* query);
extern void         db2CloseStatement    (DB2Session* session);

/** local prototypes */
       void         db2ExecForeignTruncate  (List *rels, DropBehavior behavior, bool restart_seqs);
static DB2FdwState* db2BuildTruncateFdwState(Relation rel, bool restart_seqs);

/* ExecForeignTruncate
 *
 * Called once per foreign server. All relations in "rels" must belong
 * to that server.
 */
void db2ExecForeignTruncate(List *rels, DropBehavior behavior, bool restart_seqs) {
  Relation     rel;
  DB2FdwState* fdw_state = NULL;
  ListCell*    lc;

  db2Entry1();
  if (rels != NIL) {
    /* Optionally, you could inspect "behavior" (DROP_CASCADE / DROP_RESTRICT) and try to be clever. 
     * In practice, Db2 won't cascade TRUNCATE through RI anyway, so we just ignore it and let DB2 raise an error if there
     * are incompatible constraints.
     */
    foreach(lc, rels) {
      /** obtain a fdw_state with a DB session per table */
      rel       = (Relation) lfirst(lc);
      fdw_state = db2BuildTruncateFdwState(rel, restart_seqs);

      /** obtain a fdw_state with a DB session per table */
      db2ExecuteTruncate(fdw_state->session,fdw_state->query);

      db2CloseStatement (fdw_state->session);
      db2free(fdw_state->session,"fdw_state->session");
      fdw_state->session = NULL;
    }
  }
  db2Exit1();
}

/* db2BuildTruncateFdwState */
static DB2FdwState* db2BuildTruncateFdwState(Relation rel, bool restart_seqs) {
  DB2FdwState*   fdwState;
  StringInfoData sql;
  char*          identity_clause;
  char*          storage_clause  = "DROP STORAGE";       /* or REUSE STORAGE */
  char*          trigger_clause  = "IGNORE DELETE TRIGGERS";
  db2Entry1();

  /** Map Postgres' RESTART/CONTINUE IDENTITY to Db2's TRUNCATE options. */
  if (restart_seqs)
      identity_clause = "RESTART IDENTITY";
  else
      identity_clause = "CONTINUE IDENTITY";

      /* Same logic as CMD_INSERT branch of db2PlanForeignModify: */
  fdwState = db2GetFdwState(RelationGetRelid(rel), NULL, true);
  initStringInfo(&sql);

  /* Build the TRUNCATE TABLE statement.
   *
   * Example:
   *   TRUNCATE TABLE "SCHEMA"."TAB"
   *     DROP STORAGE
   *     IGNORE DELETE TRIGGERS
   *     RESTART IDENTITY
   *     IMMEDIATE
   *
   * You may want to quote identifiers exactly as Db2 expects them.
   */
  appendStringInfo(&sql, "TRUNCATE TABLE %s %s %s %s IMMEDIATE", fdwState->db2Table->name, storage_clause, trigger_clause, identity_clause);
  fdwState->query = sql.data;
  db2Debug2("fdwState->query: '%s'",sql.data);
  db2Exit1(": %x",fdwState);
  return fdwState;
}
#endif