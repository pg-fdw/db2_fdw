#include <postgres.h>
#include <catalog/pg_collation.h>
#include <miscadmin.h>
#include <utils/formatting.h>
#include <optimizer/optimizer.h>
#include <access/heapam.h>
#include "db2_fdw.h"

/** external prototypes */
extern DB2Session*    db2GetSession             (const char* connectstring, char* user, char* password, char* jwt_token, int curlevel);
extern short          c2dbType                  (short fcType);
extern char**         getForeignSchemaList      (DB2Session* session, char* schema);
extern char**         getForeignTableList       (DB2Session* session, char* schema, char* importtype);
extern DB2Table*      describeForeignTable      (DB2Session* session, char* schema, char* tabname);
extern bool           optionIsTrue              (const char* value);

/** local prototypes */
       List*          db2ImportForeignSchema    (ImportForeignSchemaStmt* stmt, Oid serverOid);
static char*          resolveForeignSchema      (DB2Session* session, char* name);
static int            resolveForeignTable       (char** tablist, int ntabs, char* name, char* schema);
static bool           equalIgnoreCase           (char* a, char* b);
static char*          fold_case                 (char* name, fold_t foldcase);
static void           generateForeignTableCreate(StringInfo buf, char* servername, char* local_schema, char* remote_schema, DB2Table* db2Table, fold_t foldcase, bool readonly);
static ForeignServer* getOptions                (Oid serverOid, List** options);

/* db2ImportForeignSchema
 * Returns a List of CREATE FOREIGN TABLE statements.
 */
List* db2ImportForeignSchema (ImportForeignSchemaStmt* stmt, Oid serverOid) {
  char*               user       = NULL;
  char*               password   = NULL;
  char*               jwt_token  = NULL;
  char*               dbserver   = NULL;
  List*               options    = NULL;
  ListCell*           cell       = NULL;
  DB2Session*         session    = NULL;
  fold_t              foldcase   = CASE_SMART;
  StringInfoData      buf;
  bool                readonly   = false;
  char*               importtype = NULL;
  char*               remote_schema = NULL;
  List*               result     = NIL;
  ForeignServer*      server     = NULL;

  db2Entry1();
  /* process the server options */
  server = getOptions (serverOid, &options);
  foreach (cell, options) {
    DefElem *def = (DefElem *) lfirst (cell);
    db2Debug2("option: '%s'", def->defname);
    dbserver  = (strcmp (def->defname, OPT_DBSERVER)  == 0) ? STRVAL(def->arg) : dbserver;
    user      = (strcmp (def->defname, OPT_USER)      == 0) ? STRVAL(def->arg) : user;
    password  = (strcmp (def->defname, OPT_PASSWORD)  == 0) ? STRVAL(def->arg) : password;
    jwt_token = (strcmp (def->defname, OPT_JWT_TOKEN) == 0) ? STRVAL(def->arg) : jwt_token;
  }

  /* process the options of the IMPORT FOREIGN SCHEMA command */
  foreach (cell, stmt->options) {
    DefElem *def = (DefElem *) lfirst (cell);
    db2Debug2("option: '%s'", def->defname);
    if (strcmp (def->defname, "case") == 0) {
      char *s = STRVAL(def->arg);
      if (strcmp (s, "keep") == 0)
        foldcase = CASE_KEEP;
      else if (strcmp (s, "lower") == 0)
        foldcase = CASE_LOWER;
      else if (strcmp (s, "smart") == 0)
        foldcase = CASE_SMART;
      else
        ereport (ERROR, ( errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE), errmsg("invalid value for option \"%s\"", def->defname), errhint("Valid values in this context are: %s", "keep, lower, smart")));
      continue;
    } else if (strcmp (def->defname, "readonly") == 0) {
      char *s = STRVAL(def->arg);
      if (pg_strcasecmp (s, "on") == 0 || pg_strcasecmp (s, "yes") == 0 || pg_strcasecmp (s, "true") == 0 || pg_strcasecmp (s, "off") == 0 || pg_strcasecmp (s, "no") == 0 || pg_strcasecmp (s, "false") == 0)
        readonly = optionIsTrue(s);
      else
        ereport (ERROR, (errcode (ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE), errmsg ("invalid value for option \"%s\"", def->defname),errhint ("Valid values in this context are: %s", "on, yes, true, off, no, false")));
      continue;
    } else if (strcmp (def->defname, "importtype") == 0 ) {
      char *s = STRVAL(def->arg);
      /* the value ends up in the catalog query, so only use the fixed strings, in the upper case of SYSCAT.TABLES.TYPE */
      if (pg_strcasecmp (s, "T") == 0)
        importtype = "T";
      else if (pg_strcasecmp (s, "V") == 0)
        importtype = "V";
      else
        ereport (ERROR, (errcode (ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE), errmsg ("invalid value for option \"%s\"", def->defname),errhint ("Valid values in this context are: %s", "T, V")));
      continue;
    } else
    ereport (ERROR, (errcode (ERRCODE_FDW_INVALID_OPTION_NAME), errmsg ("invalid option \"%s\"", def->defname), errhint ("Valid options in this context are: %s", "case, readonly, importtype")));
  }

  /* connect to DB2 database */
  session = db2GetSession (dbserver, user, password, jwt_token, 1);

  db2Debug2("stmt->list_type    : %d", stmt->list_type);
  db2Debug2("stmt->local_schema : %s", stmt->local_schema);
  db2Debug2("stmt->remote_schema: %s", stmt->remote_schema);
  db2Debug2("stmt->server_name  : %s", stmt->server_name);
  db2Debug2("stmt->table_list   : %s", stmt->table_list);
  db2Debug2("stmt->type         : %d", stmt->type);

  initStringInfo(&buf);
  remote_schema = resolveForeignSchema (session, stmt->remote_schema);
  if (remote_schema != NULL) {
    char**          tablist   = getForeignTableList(session, remote_schema, importtype);
    int             ntabs     = 0;
    bool*           listed    = NULL;
    char**          localname = NULL;

    while (tablist[ntabs] != NULL)
      ntabs++;
    listed    = (bool*)  db2alloc (sizeof (bool)  * (ntabs + 1), "listed");
    localname = (char**) db2alloc (sizeof (char*) * (ntabs + 1), "localname");
    for (int i = 0; i < ntabs; i++) {
      listed[i]    = false;
      localname[i] = fold_case (tablist[i], foldcase);
    }

    /* table i is imported unless LIMIT TO does not list it or EXCEPT does */
#define IMPORTED(i) ((stmt->list_type != FDW_IMPORT_SCHEMA_LIMIT_TO || listed[i]) && (stmt->list_type != FDW_IMPORT_SCHEMA_EXCEPT || !listed[i]))
    if (stmt->list_type != FDW_IMPORT_SCHEMA_ALL) {
      foreach (cell, stmt->table_list) {
        RangeVar* rVar = lfirst(cell);
        int       idx  = -1;
        db2Debug2("rVar             :  %x ", rVar);
        if (rVar == NULL || rVar->relname == NULL)
          continue;
        idx = resolveForeignTable (tablist, ntabs, rVar->relname, remote_schema);
        /*
         * IMPORTANT: PostgreSQL applies LIMIT TO / EXCEPT once more to the result, comparing the table list
         * against the names of the created foreign tables. So we normalize rVar->relname in-place to the
         * local name of the DB2 table it resolved to, so that PostgreSQL's filtering and ours agree.
         */
        if (idx >= 0) {
          listed[idx]   = true;
          rVar->relname = db2strdup (localname[idx], "rVar->relname");
        } else {
          rVar->relname = fold_case (rVar->relname, foldcase);
        }
      }
    }

    for (int i = 0; i < ntabs; i++) {
      DB2Table* db2Table = NULL;
      if (!IMPORTED(i))
        continue;
      /* case folding can map different DB2 tables to the same local name; fail with a clear message instead of "relation already exists" */
      for (int j = 0; j < ntabs; j++) {
        if (j == i || strcmp (localname[i], localname[j]) != 0)
          continue;
        if (IMPORTED(j) && j < i)
          ereport (ERROR, ( errcode(ERRCODE_DUPLICATE_TABLE)
                          , errmsg("DB2 tables \"%s\".\"%s\" and \"%s\".\"%s\" would both be imported as \"%s\"", remote_schema, tablist[j], remote_schema, tablist[i], localname[i])
                          , errhint("Use the IMPORT option case 'keep' to keep the DB2 names, or LIMIT TO / EXCEPT to import only one of them.")));
        /* PostgreSQL's own EXCEPT filter compares local names, so it would silently drop this table as well */
        if (!IMPORTED(j) && stmt->list_type == FDW_IMPORT_SCHEMA_EXCEPT)
          ereport (ERROR, ( errcode(ERRCODE_DUPLICATE_TABLE)
                          , errmsg("DB2 table \"%s\".\"%s\" would be imported as \"%s\", the same name as the excluded DB2 table \"%s\"", remote_schema, tablist[i], localname[i], tablist[j])
                          , errhint("Use the IMPORT option case 'keep' to keep the DB2 names.")));
      }
      db2Table = describeForeignTable(session, remote_schema, tablist[i]);
      if (db2Table != NULL) {
        generateForeignTableCreate(&buf, server->servername, stmt->local_schema, remote_schema, db2Table, foldcase, readonly);
        db2Debug2("pg fdw table ddl: '%s'",buf.data);
        result = lappend (result, db2strdup (buf.data,"buf.data"));
        resetStringInfo (&buf);
      }
    }
#undef IMPORTED
    db2free (listed,"listed");
    db2free (localname,"localname");
    db2free (tablist,"tablist");
  }
  db2Exit1(": %d", list_length(result));
  return result;
}

/* resolveForeignSchema
 * Returns the catalog name of the DB2 schema the IMPORT refers to, or NULL if there is none.
 * DB2 names are case sensitive: BETA, "beta" and "Beta" can exist side by side. An exact match wins;
 * otherwise the name is matched ignoring case, so that the unquoted IMPORT FOREIGN SCHEMA beta (which PostgreSQL
 * folds to lower case) finds BETA, but only if that is unambiguous.
 */
static char* resolveForeignSchema (DB2Session* session, char* name) {
  char**         schemas = getForeignSchemaList (session, name);
  char*          result  = NULL;
  int            n       = 0;

  db2Entry1("(name: '%s')", name);
  for (n = 0; schemas[n] != NULL; n++) {
    if (strcmp (schemas[n], name) == 0)
      result = schemas[n];
  }
  if (result == NULL && n == 1)
    result = schemas[0];
  if (result == NULL && n > 1) {
    StringInfoData candidates;
    initStringInfo (&candidates);
    for (int i = 0; i < n; i++)
      appendStringInfo (&candidates, "%s\"%s\"", (i > 0) ? ", " : "", schemas[i]);
    ereport (ERROR, ( errcode(ERRCODE_FDW_SCHEMA_NOT_FOUND)
                    , errmsg("remote schema \"%s\" is ambiguous", name)
                    , errdetail("DB2 schemas that differ only in case: %s", candidates.data)
                    , errhint("Write the schema name in double quotes, with the exact case of the DB2 schema.")));
  }
  db2Exit1(": %s", (result == NULL) ? "NULL" : result);
  return result;
}

/* resolveForeignTable
 * Returns the index in tablist of the DB2 table a LIMIT TO / EXCEPT entry refers to, or -1 if there is none.
 * Same rule as for the schema: an exact match wins, otherwise the name is matched ignoring case if that is unambiguous.
 */
static int resolveForeignTable (char** tablist, int ntabs, char* name, char* schema) {
  int result = -1;
  int nmatch = 0;

  db2Entry1("(name: '%s')", name);
  for (int i = 0; i < ntabs && result < 0; i++) {
    if (strcmp (tablist[i], name) == 0)
      result = i;
  }
  if (result < 0) {
    StringInfoData candidates;
    initStringInfo (&candidates);
    for (int i = 0; i < ntabs; i++) {
      if (equalIgnoreCase (tablist[i], name)) {
        appendStringInfo (&candidates, "%s\"%s\"", (nmatch > 0) ? ", " : "", tablist[i]);
        result = i;
        nmatch++;
      }
    }
    if (nmatch > 1)
      ereport (ERROR, ( errcode(ERRCODE_FDW_TABLE_NOT_FOUND)
                      , errmsg("remote table \"%s\" in LIMIT TO / EXCEPT is ambiguous", name)
                      , errdetail("DB2 tables in schema \"%s\" that differ only in case: %s", schema, candidates.data)
                      , errhint("Write the table name in double quotes, with the exact case of the DB2 table.")));
    pfree (candidates.data);
  }
  db2Exit1(": %d", result);
  return result;
}

/* equalIgnoreCase
 * Compares two names like DB2's UPPER(a) = UPPER(b).
 */
static bool equalIgnoreCase (char* a, char* b) {
  char* ua     = str_toupper (a, strlen (a), DEFAULT_COLLATION_OID);
  char* ub     = str_toupper (b, strlen (b), DEFAULT_COLLATION_OID);
  bool  result = (strcmp (ua, ub) == 0);
  pfree (ua);
  pfree (ub);
  return result;
}

/* fold_case
 * Returns a dup'ed string that is the case-folded first argument.
 */
static char* fold_case (char *name, fold_t foldcase) {
  char* result = NULL;
  db2Entry4("(name: '%s', foldcase: %d)", name, foldcase);
  if (foldcase == CASE_KEEP) {
    result = db2strdup (name,"result");
  } else {
    if (foldcase == CASE_LOWER) {
      result = str_tolower (name, strlen (name), DEFAULT_COLLATION_OID);
    } else {
      if (foldcase == CASE_SMART) {
        char *upstr = str_toupper (name, strlen (name), DEFAULT_COLLATION_OID);
        /* fold case only if it does not contain lower case characters */
        if (strcmp (upstr, name) == 0)
          result = str_tolower (name, strlen (name), DEFAULT_COLLATION_OID);
        else
          result = db2strdup (name,"result");
      }
    }
  }
  if (result == NULL) {
     elog (ERROR, "impossible case folding type %d", foldcase);
  }
  db2Exit4(": '%s'", result);
  return result;
}

static void generateForeignTableCreate(StringInfo buf, char* servername, char* local_schema, char* remote_schema, DB2Table* db2Table, fold_t foldcase, bool readonly) {
  StringInfoData  coldef;
  char*           foldedname;
  bool            firstcol = true;

  db2Entry4();
  initStringInfo(&coldef);
  foldedname = fold_case (db2Table->name, foldcase);
  appendStringInfo( buf
                  , "CREATE FOREIGN TABLE \"%s\".\"%s\" ("
                  , local_schema
                  , foldedname
                  );
  db2free (foldedname,"foldedname");
  for (int i = 0; i < db2Table->ncols; i++) {
    appendStringInfo(buf, (firstcol) ? "" : ", ");

    /* column name */
    foldedname = fold_case (db2Table->cols[i]->colName, foldcase);
    appendStringInfo (buf, "\"%s\" ", foldedname);
    db2free (foldedname,"foldedname");

    // check charlen is not 0; set it to 1 in that case
    db2Table->cols[i]->colSize = db2Table->cols[i]->colSize == 0 ? 1 : db2Table->cols[i]->colSize;
    /* data type */
    switch (c2dbType(db2Table->cols[i]->colType)) {
      case DB2_CHAR:
        appendStringInfo (buf, "character(%ld)", db2Table->cols[i]->colSize);
        break;
      case DB2_VARCHAR:
        appendStringInfo (buf, "character varying(%ld)", db2Table->cols[i]->colSize);
        break;
      case DB2_LONGVARCHAR:
      case DB2_CLOB:
      case DB2_VARGRAPHIC:
      case DB2_GRAPHIC:
      case DB2_DBCLOB:
        appendStringInfo (buf, "text");
        break;
      case DB2_SMALLINT:
        appendStringInfo (buf, "smallint");
        break;
      case DB2_INTEGER:
        appendStringInfo (buf, "integer");
        break;
      case DB2_BIGINT:
        appendStringInfo (buf, "bigint");
        break;
      case DB2_BOOLEAN:
        appendStringInfo (buf, "boolean");
        break;
      case DB2_NUMERIC:
        appendStringInfo (buf, "numeric(%ld,%d)", db2Table->cols[i]->colSize, db2Table->cols[i]->colScale);
        break;
      case DB2_DECIMAL:
        appendStringInfo (buf, "decimal(%ld,%d)", db2Table->cols[i]->colSize, db2Table->cols[i]->colScale);
        break;
      case DB2_DOUBLE:
        appendStringInfo (buf, "double precision");
        break;
      case DB2_DECFLOAT:
        /*
         * DB2 DECFLOAT is a decimal floating-point type with either 16 or
         * 34 decimal digits of precision and no fixed scale.  Mapping it to
         * PostgreSQL float(p) is lossy: PostgreSQL interprets float(1..24) as
         * real, so the former colSize cap at 8 reduced every imported
         * DECFLOAT column to about six decimal digits of precision.
         *
         * An unconstrained numeric preserves both DECFLOAT precisions and
         * their variable scale.  A typmod such as numeric(34) would imply a
         * scale of zero and would therefore be incorrect for fractional
         * DECFLOAT values.
         */
        appendStringInfo (buf, "numeric");
        break;
      case DB2_FLOAT:
        /*
         * DB2 FLOAT without an explicit precision is double precision.  Even
         * for FLOAT(p) with p <= 24, importing into PostgreSQL double
         * precision is a lossless widening conversion.  PostgreSQL float(8)
         * means real/float4 (the argument is binary precision, not bytes), so
         * the former mapping silently rounded values with more than about six
         * significant decimal digits.
         */
        appendStringInfo (buf, "double precision");
        break;
      case DB2_REAL:
        appendStringInfo (buf, "real");
        break;
      case DB2_XML:
        appendStringInfo (buf, "xml");
        break;
      case DB2_BINARY:
      case DB2_VARBINARY:
      case DB2_LONGVARBINARY:
      case DB2_BLOB:
        appendStringInfo (buf, "bytea");
        break;
      case DB2_TYPE_DATE:
        appendStringInfo (buf, "date");
        break;
      case DB2_TYPE_TIMESTAMP:
        appendStringInfo (buf, "timestamp(%d)", (db2Table->cols[i]->colScale > 6) ? 6 : db2Table->cols[i]->colScale);
        break;
      case DB2_TYPE_TIMESTAMP_WITH_TIMEZONE:
        appendStringInfo (buf, "timestamp(%d) with time zone", (db2Table->cols[i]->colScale > 6) ? 6 : db2Table->cols[i]->colScale);
        break;
      case DB2_TYPE_TIME:
        appendStringInfo (buf, "time(%d)", (db2Table->cols[i]->colScale > 6) ? 6 : db2Table->cols[i]->colScale);
        break;
      default:
        elog (DEBUG2, "column \"%s\" of table \"%s\" has an untranslatable data type", db2Table->cols[i]->colName, db2Table->name);
        appendStringInfo (buf, "text");
        break;
    }
    appendStringInfo (buf, " OPTIONS (");
    appendStringInfo (buf,   "%s '%d'" , OPT_DB2TYPE , db2Table->cols[i]->colType);
    appendStringInfo (buf, ", %s '%ld'", OPT_DB2SIZE , db2Table->cols[i]->colSize);
    appendStringInfo (buf, ", %s '%ld'", OPT_DB2BYTES, db2Table->cols[i]->colBytes);
    appendStringInfo (buf, ", %s '%ld'", OPT_DB2CHARS, db2Table->cols[i]->colChars);
    appendStringInfo (buf, ", %s '%d'" , OPT_DB2SCALE, db2Table->cols[i]->colScale);
    appendStringInfo (buf, ", %s '%d'" , OPT_DB2NULL , db2Table->cols[i]->colNulls);
    appendStringInfo (buf, ", %s '%d'" , OPT_DB2CCSID, db2Table->cols[i]->colCodepage);
    /* part of the primary key */
    if (db2Table->cols[i]->colPrimKeyPart)
      appendStringInfo (buf, ", %s 'true'", OPT_KEY);
    appendStringInfo (buf, ")");

    /* not nullable */
    if (!db2Table->cols[i]->colNulls)
      appendStringInfo (buf, " NOT NULL");
    firstcol = false;
  }
  appendStringInfo( buf
                  , ") SERVER \"%s\" OPTIONS (schema '%s', table '%s'"
                  , servername
                  , remote_schema
                  , db2Table->name
                  );
  if (readonly) {
    appendStringInfo (buf, ", readonly 'true'");
  }
  appendStringInfo (buf, ")");
  db2Exit4(": %s", buf->data);
}

/* getOptions
 * Fetch the options for an db2_fdw foreign table.
 * Returns a union of the options of the foreign data wrapper, the foreign server, the user mapping and the foreign table, in that order. 
 * Column options are ignored.
 */
static ForeignServer* getOptions (Oid serverOid, List** options) {
  ForeignDataWrapper* wrapper = NULL;
  ForeignServer*      server  = NULL;
  UserMapping*        mapping = NULL;

  db2Entry4();
  /* get the foreign server, the user mapping and the FDW */
  server  = GetForeignServer      (serverOid);
  mapping = GetUserMapping        (GetUserId (), serverOid);
  if (server != NULL)
    wrapper = GetForeignDataWrapper (server->fdwid);

  /* get all options for these objects */
  *options = NIL;
  if (wrapper != NULL)
    *options = list_concat (*options, wrapper->options) ;
  if (server != NULL)
    *options = list_concat (*options, server->options);
  if (mapping != NULL)
    *options = list_concat (*options, mapping->options);
  db2Exit4(": %x", server);
  return server;
}
