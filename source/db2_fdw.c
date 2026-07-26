/*-------------------------------------------------------------------------
 * db2_fdw.c
 *   PostgreSQL-related functions for DB2 foreign data wrapper.
 *-------------------------------------------------------------------------
 */
#include <postgres.h>
#include <access/reloptions.h>
#include <catalog/pg_foreign_data_wrapper.h>
#include <catalog/pg_foreign_server.h>
#include <catalog/pg_foreign_table.h>
#include <catalog/pg_user_mapping.h>
#include <commands/explain.h>
#if PG_VERSION_NUM >= 180000
#include <commands/explain_state.h>
#include <commands/explain_format.h>
#endif
#include <miscadmin.h>
#include <storage/ipc.h>
#include <utils/builtins.h>
#include <utils/array.h>
#include <utils/guc.h>
#include <utils/syscache.h>
#include <optimizer/optimizer.h>
#include <access/heapam.h>
#include "db2_fdw.h"
#include "DB2FdwOption.h"

PG_MODULE_MAGIC;

/** SQL functions
 */
extern PGDLLEXPORT Datum db2_fdw_handler       (PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum db2_fdw_validator     (PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum db2_close_connections (PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum db2_diag              (PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1 (db2_fdw_handler);
PG_FUNCTION_INFO_V1 (db2_fdw_validator);
PG_FUNCTION_INFO_V1 (db2_close_connections);
PG_FUNCTION_INFO_V1 (db2_diag);

/** on-load initializer
 */
extern PGDLLEXPORT void _PG_init (void);

/** "true" if DB2 data have been modified in the current transaction.
 */
bool dml_in_transaction = false;

/** Valid options for db2xa_fdw.
 */
DB2FdwOption valid_options[] = {
  {OPT_DBSERVER         , ForeignServerRelationId     , true },
  {OPT_USER             , UserMappingRelationId       , false},
  {OPT_PASSWORD         , UserMappingRelationId       , false},
  {OPT_JWT_TOKEN        , UserMappingRelationId       , false},
  {OPT_SCHEMA           , ForeignTableRelationId      , false},
  {OPT_TABLE            , ForeignTableRelationId      , true },
  {OPT_MAX_LONG         , ForeignTableRelationId      , false},
  {OPT_READONLY         , ForeignTableRelationId      , false},
  {OPT_SAMPLE           , ForeignTableRelationId      , false},
  {OPT_PREFETCH         , ForeignTableRelationId      , false},
  {OPT_FETCHSZ          , ForeignTableRelationId      , false},
  {OPT_KEY              , AttributeRelationId         , false},
  {OPT_DB2TYPE          , AttributeRelationId         , false},
  {OPT_DB2SIZE          , AttributeRelationId         , false},
  {OPT_DB2BYTES         , AttributeRelationId         , false},
  {OPT_DB2CHARS         , AttributeRelationId         , false},
  {OPT_DB2SCALE         , AttributeRelationId         , false},
  {OPT_DB2NULL          , AttributeRelationId         , false},
  {OPT_DB2CCSID         , AttributeRelationId         , false},
  {OPT_BATCH_SIZE       , ForeignServerRelationId     , false},
  {OPT_BATCH_SIZE       , ForeignTableRelationId      , false},
  {OPT_NO_ENCODING_ERROR, ForeignDataWrapperRelationId, false},
  {OPT_NO_ENCODING_ERROR, ForeignTableRelationId      , false},
  {OPT_NO_ENCODING_ERROR, AttributeRelationId         , false}
};

/** Array to hold the type output functions during table modification.
 * It is ok to hold this cache in a static variable because there cannot
 * be more than one foreign table modified at the same time.
 */
regproc* output_funcs;

/** db2_utils
 */
extern DB2Session*      db2GetSession              (const char* connectstring, char* user, char* password, char* jwt_token, int curlevel);
extern void             db2CloseConnections        (void);
extern void             db2ClientVersion           (DB2Session* session, char* version);
extern void             db2ServerVersion           (DB2Session* session, char* version);
extern void             db2GetForeignRelSize       (PlannerInfo* root, RelOptInfo* baserel, Oid foreigntableid);

/** db2_fdw_handler fdwroutines 
 */
extern ForeignScan*     db2GetForeignPlan           (PlannerInfo* root, RelOptInfo* foreignrel, Oid foreigntableid, ForeignPath* best_path, List* tlist, List* scan_clauses , Plan* outer_plan);
extern void             db2GetForeignPaths          (PlannerInfo* root, RelOptInfo* baserel, Oid foreigntableid);
extern void             db2GetForeignUpperPaths     (PlannerInfo *root, UpperRelationKind stage, RelOptInfo *input_rel, RelOptInfo *output_rel, void *extra);
extern void             db2GetForeignJoinPaths      (PlannerInfo* root, RelOptInfo* joinrel, RelOptInfo* outerrel, RelOptInfo* innerrel, JoinType jointype, JoinPathExtraData* extra);
extern bool             db2AnalyzeForeignTable      (Relation relation, AcquireSampleRowsFunc* func, BlockNumber* totalpages);
extern void             db2ExplainForeignScan       (ForeignScanState* node, ExplainState* es);
extern void             db2BeginForeignScan         (ForeignScanState* node, int eflags);
extern TupleTableSlot*  db2IterateForeignScan       (ForeignScanState* node);
extern void             db2EndForeignScan           (ForeignScanState* node);
extern void             db2ReScanForeignScan        (ForeignScanState* node);
extern void             db2AddForeignUpdateTargets  (PlannerInfo* root, Index rtindex, RangeTblEntry* target_rte, Relation target_relation);
extern List*            db2PlanForeignModify        (PlannerInfo* root, ModifyTable* plan, Index resultRelation, int subplan_index);
extern void             db2BeginForeignModify       (ModifyTableState* mtstate, ResultRelInfo* rinfo, List* fdw_private, int subplan_index, int eflags);
extern void             db2BeginForeignInsert       (ModifyTableState* mtstate, ResultRelInfo* rinfo);
extern TupleTableSlot*  db2ExecForeignInsert        (EState* estate, ResultRelInfo* rinfo, TupleTableSlot* slot, TupleTableSlot* planSlot);
extern TupleTableSlot*  db2ExecForeignUpdate        (EState* estate, ResultRelInfo* rinfo, TupleTableSlot* slot, TupleTableSlot* planSlot);
extern TupleTableSlot*  db2ExecForeignDelete        (EState* estate, ResultRelInfo* rinfo, TupleTableSlot* slot, TupleTableSlot* planSlot);
extern void             db2EndForeignModify         (EState* estate, ResultRelInfo* rinfo);
extern void             db2EndForeignInsert         (EState* estate, ResultRelInfo* rinfo);
extern void             db2ExplainForeignModify     (ModifyTableState* mtstate, ResultRelInfo* rinfo, List* fdw_private, int subplan_index, ExplainState* es);
extern int              db2IsForeignRelUpdatable    (Relation rel);
extern List*            db2ImportForeignSchema      (ImportForeignSchemaStmt* stmt, Oid serverOid);
extern bool             db2PlanDirectModify         (PlannerInfo* root, ModifyTable* plan, Index resultRelation, int subplan_index);
extern void             db2BeginDirectModify        (ForeignScanState* node, int eflags);
extern TupleTableSlot*  db2IterateDirectModify      (ForeignScanState* node);
extern void             db2EndDirectModify          (ForeignScanState* node);

extern void             db2ExecForeignTruncate      (List *rels, DropBehavior behavior, bool restart_seqs);
extern TupleTableSlot** db2ExecForeignBatchInsert   (EState *estate, ResultRelInfo *rinfo, TupleTableSlot **slots, TupleTableSlot **planSlots, int *numSlots);
extern int              db2GetForeignModifyBatchSize(ResultRelInfo *rinfo);
/** db2 fdw utilities */
extern void            exitHook                  (int code, Datum arg);


/* Foreign-data wrapper handler function: return a struct with pointers to callback routines.
 */
PGDLLEXPORT Datum db2_fdw_handler (PG_FUNCTION_ARGS) {
  FdwRoutine* fdwroutine    = makeNode (FdwRoutine);

  fdwroutine->GetForeignRelSize         = db2GetForeignRelSize;
  fdwroutine->GetForeignPaths           = db2GetForeignPaths;
  fdwroutine->GetForeignUpperPaths      = db2GetForeignUpperPaths;
  fdwroutine->GetForeignJoinPaths       = db2GetForeignJoinPaths;
  fdwroutine->GetForeignPlan            = db2GetForeignPlan;
  fdwroutine->AnalyzeForeignTable       = db2AnalyzeForeignTable;
  fdwroutine->ExplainForeignScan        = db2ExplainForeignScan;
  fdwroutine->BeginForeignScan          = db2BeginForeignScan;
  fdwroutine->IterateForeignScan        = db2IterateForeignScan;
  fdwroutine->ReScanForeignScan         = db2ReScanForeignScan;
  fdwroutine->EndForeignScan            = db2EndForeignScan;
  fdwroutine->AddForeignUpdateTargets   = db2AddForeignUpdateTargets;
  fdwroutine->PlanForeignModify         = db2PlanForeignModify;
  fdwroutine->BeginForeignModify        = db2BeginForeignModify;
  fdwroutine->ExecForeignInsert         = db2ExecForeignInsert;
  fdwroutine->ExecForeignUpdate         = db2ExecForeignUpdate;
  fdwroutine->ExecForeignDelete         = db2ExecForeignDelete;
  fdwroutine->EndForeignModify          = db2EndForeignModify;
  fdwroutine->ExplainForeignModify      = db2ExplainForeignModify;
  fdwroutine->IsForeignRelUpdatable     = db2IsForeignRelUpdatable;
  fdwroutine->ImportForeignSchema       = db2ImportForeignSchema;
  fdwroutine->BeginForeignInsert        = db2BeginForeignInsert;
  fdwroutine->EndForeignInsert          = db2EndForeignInsert;

  fdwroutine->PlanDirectModify          = db2PlanDirectModify;
  fdwroutine->BeginDirectModify         = db2BeginDirectModify;
  fdwroutine->IterateDirectModify       = db2IterateDirectModify;
  fdwroutine->EndDirectModify           = db2EndDirectModify;

  fdwroutine->ExecForeignTruncate       = db2ExecForeignTruncate;
  fdwroutine->ExecForeignBatchInsert    = db2ExecForeignBatchInsert;
  fdwroutine->GetForeignModifyBatchSize = db2GetForeignModifyBatchSize;

  PG_RETURN_POINTER (fdwroutine);
}

/* db2_fdw_validator
 * Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER, USER MAPPING or FOREIGN TABLE that uses db2_fdw.
 * Raise an ERROR if the option or its value are considered invalid or a required option is missing.
 */
PGDLLEXPORT Datum db2_fdw_validator (PG_FUNCTION_ARGS) {
  List*     options_list               = untransformRelOptions (PG_GETARG_DATUM (0));
  Oid       catalog                    = PG_GETARG_OID (1);
  ListCell* cell                       = NULL;
  bool      option_given[option_count] = { false };
  int       i                          = 0;

  /** Check that only options supported by db2_fdw, and allowed for the
   * current object type, are given.
   */
  foreach (cell, options_list) {
    DefElem* def       = (DefElem *) lfirst (cell);
    bool     opt_found = false;
    /* search for the option in the list of valid options */
    for (i = 0; i < option_count; ++i) {
      if (catalog == valid_options[i].optcontext && strcmp (valid_options[i].optname, def->defname) == 0) {
        opt_found       = true;
        option_given[i] = true;
        break;
      }
    }
    /* option not found, generate error message */
    if (!opt_found) {
      /* generate list of options */
      StringInfoData buf;
      initStringInfo (&buf);
      for (i = 0; i < option_count; ++i) {
        if (catalog == valid_options[i].optcontext)
          appendStringInfo (&buf, "%s%s", (buf.len > 0) ? ", " : "", valid_options[i].optname);
      }
      ereport (ERROR
              , ( errcode(ERRCODE_FDW_INVALID_OPTION_NAME)
                , errmsg ("invalid option \"%s\"", def->defname)
                , errhint("Valid options in this context are: %s", buf.data)
                )
              );
    }
    /* check valid values for "readonly", "key" and "no_encoding_error" */
    if (strcmp (def->defname, OPT_READONLY         ) == 0 
    ||  strcmp (def->defname, OPT_KEY              ) == 0  
    ||  strcmp (def->defname, OPT_NO_ENCODING_ERROR) == 0) {
      char *val = STRVAL(def->arg);
      if (pg_strcasecmp (val, "on"  ) != 0 && pg_strcasecmp (val, "off"  ) != 0
      &&  pg_strcasecmp (val, "yes" ) != 0 && pg_strcasecmp (val, "no"   ) != 0
      &&  pg_strcasecmp (val, "true") != 0 && pg_strcasecmp (val, "false") != 0) {
        ereport ( ERROR
                , ( errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE)
                  , errmsg ("invalid value for option \"%s\"", def->defname)
                  , errhint("Valid values in this context are: on/yes/true or off/no/false")
                  )
                );
      }
    }
    /* check valid values for "table" and "schema" */
    if (strcmp (def->defname, OPT_TABLE) == 0 || strcmp (def->defname, OPT_SCHEMA) == 0) {
      char *val = STRVAL(def->arg);
      if (strchr (val, '"') != NULL)
        ereport ( ERROR
                , ( errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE)
                  , errmsg ("invalid value for option \"%s\"", def->defname)
                  , errhint("Double quotes are not allowed in names.")
                  )
                );
    }
    /* check valid values for column options */
    /* check valid values for max_long */
    if (strcmp (def->defname, OPT_DB2TYPE ) == 0 
    ||  strcmp (def->defname, OPT_DB2NULL ) == 0
    ||  strcmp (def->defname, OPT_DB2SIZE ) == 0
    ||  strcmp (def->defname, OPT_DB2BYTES) == 0
    ||  strcmp (def->defname, OPT_DB2CHARS) == 0
    ||  strcmp (def->defname, OPT_DB2SCALE) == 0
    ||  strcmp (def->defname, OPT_DB2CCSID) == 0
    ||  strcmp (def->defname, OPT_MAX_LONG) == 0) {
      char *val = STRVAL(def->arg);
      char *endptr;
      long  lvalue = strtol (val, &endptr, 0);
      if (val[0] == '\0' || *endptr != '\0' || lvalue < LONG_MIN || lvalue > LONG_MAX)
        ereport (ERROR
                , ( errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE)
                  , errmsg ("invalid value for option \"%s\"", def->defname)
                  , errhint("Valid values in this context are integers between 1 and 1073741823.")
                  )
                );
    }
    /* check valid values for "sample_percent" */
    if (strcmp (def->defname, OPT_SAMPLE) == 0) {
      char *val = STRVAL(def->arg);
      char *endptr;
      double sample_percent;
      errno = 0;
      sample_percent = strtod (val, &endptr);
      if (val[0] == '\0' || *endptr != '\0' || errno != 0 || sample_percent < 0.000001 || sample_percent > 100.0)
        ereport ( ERROR
                , ( errcode( ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE)
                  , errmsg ("invalid value for option \"%s\"", def->defname)
                  , errhint("Valid values in this context are numbers between 0.000001 and 100.")
                  )
                );
    }
    /* check valid values for "prefetch" */
    if (strcmp (def->defname, OPT_PREFETCH) == 0) {
      char *val = STRVAL(def->arg);
      char *endptr;
      unsigned long prefetch = strtol (val, &endptr, 0);
      if (val[0] == '\0' || *endptr != '\0' || prefetch < 0 || prefetch > DB2_MAX_ATTR_PREFETCH_NROWS)
        ereport ( ERROR
                , ( errcode (ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE)
                  , errmsg ("invalid value for option \"%s\"", def->defname)
                  , errhint ("Valid values in this context are integers between 0 and %d.", DB2_MAX_ATTR_PREFETCH_NROWS)
                  )
                );
    }
    /* check valid values for "fetchsize" */
    if (strcmp (def->defname, OPT_FETCHSZ) == 0) {
      char *val = STRVAL(def->arg);
      char *endptr;
      unsigned long fetchsz = strtol (val, &endptr, 0);
      if (val[0] == '\0' || *endptr != '\0' || fetchsz < 0 || fetchsz > DB2_MAX_ATTR_ROW_ARRAY_SIZE)
        ereport ( ERROR
                , ( errcode (ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE)
                  , errmsg ("invalid value for option \"%s\"", def->defname)
                  , errhint ("Valid values in this context are integers between 1 and %d.", DB2_MAX_ATTR_ROW_ARRAY_SIZE)
                  )
                );
    }
    /* check valid values for "batchsz" */
    if (strcmp (def->defname, OPT_BATCH_SIZE) == 0) {
      char *val = STRVAL(def->arg);
      char *endptr;
      unsigned long batchsz = strtol (val, &endptr, 0);
      if (val[0] == '\0' || *endptr != '\0' || batchsz < 0)
        ereport ( ERROR
                , ( errcode (ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE)
                  , errmsg ("invalid value for option \"%s\"", def->defname)
                  , errhint ("Valid values in this context are integers greater than 0.")
                  )
                );
    }
  }
  /* check that all required options have been given */
  for (i = 0; i < option_count; ++i) {
    if (catalog == valid_options[i].optcontext && valid_options[i].optrequired && !option_given[i]) {
      ereport ( ERROR
              , ( errcode (ERRCODE_FDW_OPTION_NAME_NOT_FOUND)
                , errmsg ("missing required option \"%s\""
                , valid_options[i].optname)
                )
              );
    }
  }

  /* For UserMapping: check that either (user AND password) OR jwt_token is provided */
  if (catalog == UserMappingRelationId) {
    bool has_user = false;
    bool has_password = false;
    bool has_jwt_token = false;

    for (i = 0; i < option_count; ++i) {
      if (option_given[i] && catalog == valid_options[i].optcontext) {
        if (strcmp(valid_options[i].optname, OPT_USER) == 0)
          has_user = true;
        if (strcmp(valid_options[i].optname, OPT_PASSWORD) == 0)
          has_password = true;
        if (strcmp(valid_options[i].optname, OPT_JWT_TOKEN) == 0)
          has_jwt_token = true;
      }
    }

    /* Validate authentication method */
    if (has_jwt_token && (has_user || has_password)) {
      ereport ( ERROR
              , ( errcode (ERRCODE_FDW_INVALID_OPTION_NAME)
                , errmsg ("cannot specify both jwt_token and user/password authentication")
                , errhint ("Use either jwt_token OR user and password, not both.")
                )
              );
    }

    if (!has_jwt_token && (!has_user || !has_password)) {
      ereport ( ERROR
              , ( errcode (ERRCODE_FDW_OPTION_NAME_NOT_FOUND)
                , errmsg ("authentication credentials missing")
                , errhint ("Specify either jwt_token OR both user and password.")
                )
              );
    }
  }

  PG_RETURN_VOID ();
}

/* db2_close_connections
 * Close all open DB2 connections.
 */
PGDLLEXPORT Datum db2_close_connections (PG_FUNCTION_ARGS) {
  if (dml_in_transaction)
    ereport ( ERROR
            , ( errcode (ERRCODE_ACTIVE_SQL_TRANSACTION)
              , errmsg ("connections with an active transaction cannot be closed")
              , errhint ("The transaction that modified DB2 data must be closed first.")
              )
            );
  elog (DEBUG1, "db2_fdw: close all DB2 connections");
  db2CloseConnections ();
  PG_RETURN_VOID ();
}

/* db2_diag
 * Get the DB2 client version.
 * If a non-NULL argument is supplied, it must be a foreign server name.
 * In this case, the remote server version is returned as well.
 */
PGDLLEXPORT Datum db2_diag (PG_FUNCTION_ARGS) {
  Oid            srvId     = InvalidOid;
  char*          pgversion = NULL;
  StringInfoData version;

  /* Get the PostgreSQL server version.
   * We cannot use PG_VERSION because that would give the version against which
   * db2_fdw was compiled, not the version it is running with.
   */
  pgversion = GetConfigOptionByName ("server_version", NULL, false);

  initStringInfo (&version);
  appendStringInfo (&version, "db2_fdw %s, PostgreSQL %s", DB2_FDW_VERSION, pgversion);

  if (PG_ARGISNULL (0)) {
    /* display some important DB2 environment variables */
    static const char *const db2_env[] = { "DB2INSTANCE", "DB2_HOME", "DB2LIB", NULL };
    int i;
    for (i = 0; db2_env[i] != NULL; ++i) {
      char *val = getenv (db2_env[i]);
      if (val != NULL)
        appendStringInfo (&version, ", %s=%s", db2_env[i], val);
    }
  } else {
    /* get the server version only if a non-null argument was given */
    DB2Session*         session = NULL;
    HeapTuple           tup;
    Relation            rel;
    Name                srvname = PG_GETARG_NAME (0);
    ForeignServer*      server;
    UserMapping*        mapping;
    ForeignDataWrapper* wrapper;
    List*               options;
    ListCell*           cell;
    char*               user      = NULL;
    char*               password  = NULL;
    char*               dbserver  = NULL;
    char*               jwt_token = NULL;
    char srv_version[256];
    char cli_version[256];

    /* look up foreign server with this name */
    rel = table_open (ForeignServerRelationId, AccessShareLock);
    tup = SearchSysCacheCopy1 (FOREIGNSERVERNAME, NameGetDatum (srvname));
    if (!HeapTupleIsValid (tup)) {
      ereport (ERROR
              , ( errcode(ERRCODE_UNDEFINED_OBJECT)
                , errmsg ("server \"%s\" does not exist", NameStr(*srvname))
                )
              );
    }
    srvId = ((Form_pg_foreign_server)GETSTRUCT(tup))->oid;
    table_close (rel, AccessShareLock);

    /* get the foreign server, the user mapping and the FDW */
    server  = GetForeignServer (srvId);
    mapping = GetUserMapping (GetUserId (), srvId);
    wrapper = GetForeignDataWrapper (server->fdwid);
    /* get all options for these objects */
    options = wrapper->options;
    options = list_concat (options, server->options);
    options = list_concat (options, mapping->options);
    foreach (cell, options) {
      DefElem *def = (DefElem *) lfirst (cell);
      dbserver  = (strcmp (def->defname, OPT_DBSERVER)  == 0) ? STRVAL(def->arg) : dbserver;
      user      = (strcmp (def->defname, OPT_USER)      == 0) ? STRVAL(def->arg) : user;
      password  = (strcmp (def->defname, OPT_PASSWORD)  == 0) ? STRVAL(def->arg) : password;
      jwt_token = (strcmp (def->defname, OPT_JWT_TOKEN) == 0) ? STRVAL(def->arg) : jwt_token;
    }
    /* connect to DB2 database */
    session = db2GetSession (dbserver, user, password, jwt_token, 1);
    /* get the client version */

    db2ClientVersion (session, cli_version);
    appendStringInfo (&version, ", DB2 client %s", cli_version);
    /* get the server version */
    db2ServerVersion (session, srv_version);
    appendStringInfo (&version, ", DB2 server %s", srv_version);
    /* release the session (connection will be cached) */
    /* db2free (session);*/
  }
  PG_RETURN_TEXT_P (cstring_to_text (version.data));
}

/* _PG_init
 * Library load-time initalization.
 * Sets exitHook() callback for backend shutdown.
 */
void _PG_init (void) {
  char*       pgversion     = NULL;
  int         min_pgversion = PG_SUPPORTED_MIN_VERSION / 10000;
  int         i_pgversion   = min_pgversion;

  /* Get the PostgreSQL server version.
   * We cannot use PG_VERSION because that would give the version against which
   * db2_fdw was compiled, not the version it is running with.
   */
  pgversion     = GetConfigOptionByName ("server_version", NULL, false);
  if (pgversion != NULL) {
    char majorversion[3];
    majorversion[0] = pgversion[0];
    majorversion[1] = pgversion[1];
    majorversion[2] = '\0';
    i_pgversion     = atoi(majorversion);
  }

  if (i_pgversion >= min_pgversion) {
    /* register an exit hook */
    on_proc_exit (&exitHook, PointerGetDatum (NULL));
  } else {
    ereport (ERROR
            , ( errcode(ERRCODE_INSUFFICIENT_RESOURCES)
              , errmsg ("You are running a PG version %s which is not supported.", pgversion)
              , errhint("You need at least PG version %d or higher.", min_pgversion)
              )
            );
  }
}
