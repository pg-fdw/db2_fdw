#include <postgres.h>
#include <catalog/pg_collation.h>
#include <miscadmin.h>
#include <utils/formatting.h>
#include <optimizer/optimizer.h>
#include <access/heapam.h>
#include "db2_fdw.h"

/** external prototypes */
extern DB2Session*    db2GetSession             (const char* connectstring, char* user, char* password, char* jwt_token, const char* nls_lang, int curlevel);
extern char*          guessNlsLang              (char* nls_lang);
extern short          c2dbType                  (short fcType);
extern bool           isForeignSchema           (DB2Session* session, char* schema);
extern char**         getForeignTableList       (DB2Session* session, char* schema, int list_type, char* table_list);
extern DB2Table*      describeForeignTable      (DB2Session* session, char* schema, char* tabname);
extern bool           optionIsTrue              (const char* value);

/** local prototypes */
       List*          db2ImportForeignSchema    (ImportForeignSchemaStmt* stmt, Oid serverOid);
static char*          fold_case                 (char* name, fold_t foldcase);
static void           generateForeignTableCreate(StringInfo buf, char* servername, char* local_schema, char* remote_schema, DB2Table* db2Table, fold_t foldcase, bool readonly);
static ForeignServer* getOptions                (Oid serverOid, List** options);

/* db2ImportForeignSchema
 * Returns a List of CREATE FOREIGN TABLE statements.
 */
List* db2ImportForeignSchema (ImportForeignSchemaStmt* stmt, Oid serverOid) {
  char*               nls_lang  = NULL;
  char*               user      = NULL;
  char*               password  = NULL;
  char*               jwt_token = NULL;
  char*               dbserver  = NULL;
  List*               options   = NULL;
  ListCell*           cell      = NULL;
  DB2Session*         session   = NULL;
  fold_t              foldcase  = CASE_SMART;
  StringInfoData      buf;
  bool                readonly  = false;
  List*               result    = NIL;
  ForeignServer*      server    = NULL;

  db2Entry1();
  /* process the server options */
  server = getOptions (serverOid, &options);
  foreach (cell, options) {
    DefElem *def = (DefElem *) lfirst (cell);
    db2Debug2("option: '%s'", def->defname);
    nls_lang  = (strcmp (def->defname, OPT_NLS_LANG)  == 0) ? STRVAL(def->arg) : nls_lang;
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
      if (pg_strcasecmp (s, "on") != 0 || pg_strcasecmp (s, "yes") != 0 || pg_strcasecmp (s, "true") != 0 || pg_strcasecmp (s, "off") != 0 || pg_strcasecmp (s, "no") != 0 || pg_strcasecmp (s, "false") != 0)
        readonly = optionIsTrue(s);
      else
        ereport (ERROR, (errcode (ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE), errmsg ("invalid value for option \"%s\"", def->defname),errhint ("Valid values in this context are: %s", "on, yes, true, off, no, false")));
      continue;
    }
    ereport (ERROR, (errcode (ERRCODE_FDW_INVALID_OPTION_NAME), errmsg ("invalid option \"%s\"", def->defname), errhint ("Valid options in this context are: %s", "case, readonly")));
  }

  /* guess a good NLS_LANG environment setting */
  nls_lang = guessNlsLang (nls_lang);

  /* connect to DB2 database */
  session = db2GetSession (dbserver, user, password, jwt_token, nls_lang, 1);

  db2Debug2("stmt->list_type    : %d", stmt->list_type);
  db2Debug2("stmt->local_schema : %s", stmt->local_schema);
  db2Debug2("stmt->remote_schema: %s", stmt->remote_schema);
  db2Debug2("stmt->server_name  : %s", stmt->server_name);
  db2Debug2("stmt->table_list   : %s", stmt->table_list);
  db2Debug2("stmt->type         : %d", stmt->type);

  if (isForeignSchema (session, stmt->remote_schema)) {
    StringInfoData  tblist;
    char**          tablist  = NULL;
    initStringInfo(&buf);
    initStringInfo(&tblist);

    if (stmt->list_type != FDW_IMPORT_SCHEMA_ALL) {
      foreach (cell, stmt->table_list) {
        RangeVar* rVar      = lfirst(cell);
        char*     uppername = NULL;
        char*     folded    = NULL;
        db2Debug2("rVar             :  %x ", rVar);
        if (rVar == NULL || rVar->relname == NULL)
          continue;

        /*
         * IMPORTANT: the table list in LIMIT TO / EXCEPT is compared against the
         * names that will be created in PostgreSQL after case folding.
         *
         * So we normalize rVar->relname in-place using fold_case() so that
         * PostgreSQL's LIMIT/EXCEPT filtering and our generated CREATE FOREIGN
         * TABLE statements agree.
         *
         * For the DB2-side query filter we use an uppercased version of the name
         * and the DB2 query itself compares with UPPER(T.TABNAME), making the
         * matching case-insensitive.
         */
        uppername = str_toupper (rVar->relname, strlen (rVar->relname), DEFAULT_COLLATION_OID);
        if (tblist.len != 0) {
          appendStringInfo(&tblist,",'%s'", uppername);
        } else {
          appendStringInfo(&tblist,"'%s'", uppername);
        }
        db2free (uppername,"uppername");

        folded = fold_case (rVar->relname, foldcase);
        rVar->relname = folded;

      }
      db2Debug2("import table_list: %s",tblist.data);
    }
    tablist  = getForeignTableList(session, stmt->remote_schema, stmt->list_type, tblist.data);
    db2free (tblist.data,"tblist.data");
    for (int i = 0; tablist[i] != NULL; i++) {
      DB2Table* db2Table = describeForeignTable(session, stmt->remote_schema, tablist[i]);
      if (db2Table != NULL) {
        generateForeignTableCreate(&buf, server->servername, stmt->local_schema, stmt->remote_schema, db2Table, foldcase, readonly);
        db2Debug2("pg fdw table ddl: '%s'",buf.data);
        result = lappend (result, db2strdup (buf.data,"buf.data"));
        resetStringInfo (&buf);
      }
    }
    db2free (tablist,"tablist");
  }
  db2Exit1(": %d", list_length(result));
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
      case DB2_FLOAT:
        db2Table->cols[i]->colSize = (db2Table->cols[i]->colSize > 8) ? 8 : db2Table->cols[i]->colSize;
        appendStringInfo (buf, "float(%ld)", db2Table->cols[i]->colSize);
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