#include <postgres.h>
#include <catalog/pg_collation.h>
#include <foreign/foreign.h>
#include <miscadmin.h>
#include <utils/formatting.h>
#include <nodes/pathnodes.h>
#include <optimizer/optimizer.h>
#include <access/heapam.h>
#include "db2_fdw.h"

/** external prototypes */
extern DB2Session*  db2GetSession             (const char* connectstring, char* user, char* password, char* jwt_token, const char* nls_lang, int curlevel);
extern int          db2GetImportColumn        (DB2Session* session, char* stmt, char* table_list, int list_type, char* tabname, char* colname, short* colType, size_t* colLen, short* typescale, short* nullable, int* key, int* cp);
extern char*        guessNlsLang              (char* nls_lang);
extern void         db2Debug1                 (const char* message, ...);
extern void         db2Debug2                 (const char* message, ...);
extern short        c2dbType                  (short fcType);
extern void         db2free                   (void* p);
extern char*        db2strdup                 (const char* source);

/** local prototypes */
List* db2ImportForeignSchema(ImportForeignSchemaStmt* stmt, Oid serverOid);
char* fold_case             (char* name, fold_t foldcase);

/** db2ImportForeignSchema
 *   Returns a List of CREATE FOREIGN TABLE statements.
 */
List* db2ImportForeignSchema (ImportForeignSchemaStmt* stmt, Oid serverOid) {
  ForeignServer*      server;
  UserMapping*        mapping;
  ForeignDataWrapper* wrapper;
  char                tabname   [TABLE_NAME_LEN] = { '\0' };
  char                colname   [COLUMN_NAME_LEN] = { '\0' };
  char                oldtabname[TABLE_NAME_LEN] = { '\0' };
  char*               foldedname;
  char*               nls_lang  = NULL;
  char*               user      = NULL;
  char*               password  = NULL;
  char*               jwt_token = NULL;
  char*               dbserver  = NULL;
  short               colType;
  size_t              colSize;
  short               colScale;
  short               colNulls;
  int                 key;
  int                 cp;
  int                 rc;
  List*               options;
  List*               result    = NIL;
  ListCell*           cell;
  DB2Session*         session;
  fold_t              foldcase  = CASE_SMART;
  StringInfoData      buf;
  StringInfoData      tblist;
  bool                readonly  = false;
  bool                firstcol  = true;
  db2Debug1("> db2ImportForeignSchema");
  
  /* get the foreign server, the user mapping and the FDW */
  server  = GetForeignServer      (serverOid);
  mapping = GetUserMapping        (GetUserId (), serverOid);
  wrapper = GetForeignDataWrapper (server->fdwid);

  /* get all options for these objects */
  options = wrapper->options;
  options = list_concat (options, server->options);
  options = list_concat (options, mapping->options);

  foreach (cell, options) {
    DefElem *def = (DefElem *) lfirst (cell);
    if (strcmp (def->defname, OPT_NLS_LANG) == 0)
      nls_lang = STRVAL(def->arg);
    if (strcmp (def->defname, OPT_DBSERVER) == 0)
      dbserver = STRVAL(def->arg);
    if (strcmp (def->defname, OPT_USER) == 0)
      user = STRVAL(def->arg);
    if (strcmp (def->defname, OPT_PASSWORD) == 0)
      password = STRVAL(def->arg);
    if (strcmp (def->defname, OPT_JWT_TOKEN) == 0)
      jwt_token = STRVAL(def->arg);
  }

  /* process the options of the IMPORT FOREIGN SCHEMA command */
  foreach (cell, stmt->options) {
    DefElem *def = (DefElem *) lfirst (cell);
    db2Debug2("  option: '%s'", def->defname);
    if (strcmp (def->defname, "case") == 0) {
      char *s = STRVAL(def->arg);
      if (strcmp (s, "keep") == 0)
        foldcase = CASE_KEEP;
      else if (strcmp (s, "lower") == 0)
        foldcase = CASE_LOWER;
      else if (strcmp (s, "smart") == 0)
        foldcase = CASE_SMART;
      else
        ereport (ERROR
                , ( errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE)
                  , errmsg("invalid value for option \"%s\"", def->defname)
                  , errhint("Valid values in this context are: %s", "keep, lower, smart")
                  )
                );
      continue;
    } else if (strcmp (def->defname, "readonly") == 0) {
      char *s = STRVAL(def->arg);
      if (pg_strcasecmp (s, "on") == 0 || pg_strcasecmp (s, "yes") == 0 || pg_strcasecmp (s, "true") == 0)
        readonly = true;
      else if (pg_strcasecmp (s, "off") == 0 || pg_strcasecmp (s, "no") == 0 || pg_strcasecmp (s, "false") == 0)
        readonly = false;
      else
        ereport (ERROR, (errcode (ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE), errmsg ("invalid value for option \"%s\"", def->defname)));
      continue;
    }
    ereport (ERROR, (errcode (ERRCODE_FDW_INVALID_OPTION_NAME), errmsg ("invalid option \"%s\"", def->defname), errhint ("Valid options in this context are: %s", "case, readonly")));
  }

  /* guess a good NLS_LANG environment setting */
  nls_lang = guessNlsLang (nls_lang);

  /* connect to DB2 database */
  session = db2GetSession (dbserver, user, password, jwt_token, nls_lang, 1);

  initStringInfo (&buf);
  db2Debug2("  stmt->list_type    : %d  ",stmt->list_type);
  db2Debug2("  stmt->local_schema : '%s'",stmt->local_schema);
  db2Debug2("  stmt->remote_schema: '%s'",stmt->remote_schema);
  db2Debug2("  stmt->server_name  : '%s'",stmt->server_name);
  db2Debug2("  stmt->tabel_list   : '%x'",stmt->table_list);
  db2Debug2("  stmt->type         : %d  ",stmt->type);

  initStringInfo (&tblist);
  if (stmt->list_type != FDW_IMPORT_SCHEMA_ALL) {
    foreach (cell, stmt->table_list) {
      RangeVar* rVar = lfirst(cell);
      char* uppername;
      char* folded;
      db2Debug2("  rVar             :  %x ", rVar);
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
      db2free (uppername);

      folded = fold_case (rVar->relname, foldcase);
      rVar->relname = folded;
    }
  }
  db2Debug2("  import table_list: '%s'",tblist.data);
  do {
    /* get the next column definition */
    rc = db2GetImportColumn (session, stmt->remote_schema, tblist.data, stmt->list_type, tabname, colname, &colType, &colSize, &colScale, &colNulls, &key, &cp);

    if (rc == -1) {
      /* remote schema does not exist, issue a warning */
      ereport (ERROR,(errcode(ERRCODE_FDW_SCHEMA_NOT_FOUND)
                     ,errmsg ("remote schema \"%s\" does not exist", stmt->remote_schema)
                     ,errhint ("Enclose the schema name in double quotes to prevent case folding.")
                     )
              );
      return NIL;
    }

    if ((rc == 0 && oldtabname[0] != '\0') || (rc == 1 && oldtabname[0] != '\0' && strcmp (tabname, oldtabname))) {
      /* finish previous CREATE FOREIGN TABLE statement */
      appendStringInfo (&buf, ") SERVER \"%s\" OPTIONS (schema '%s', table '%s'", server->servername, stmt->remote_schema, oldtabname);
      if (readonly) {
        appendStringInfo (&buf, ", readonly 'true'");
      }
      appendStringInfo (&buf, ")");
      db2Debug2 ("  pg fdw table ddl: '%s'",buf.data);
      result = lappend (result, db2strdup (buf.data));
      db2Debug2("result(%d):%x",list_length(result), result);
    }

    if (rc == 1 && (oldtabname[0] == '\0' || strcmp (tabname, oldtabname))) {
      /* start a new CREATE FOREIGN TABLE statement */
      resetStringInfo (&buf);
      foldedname = fold_case (tabname, foldcase);
      appendStringInfo (&buf, "CREATE FOREIGN TABLE \"%s\".\"%s\" (", stmt->local_schema, foldedname);
      db2free (foldedname);

      firstcol = true;
      strncpy (oldtabname, tabname, sizeof(oldtabname));
    }

    if (rc == 1) {
      /** Add a column definition. */
      if (firstcol)
        firstcol = false;
      else
        appendStringInfo (&buf, ", ");

      /* column name */
      foldedname = fold_case (colname, foldcase);
      appendStringInfo (&buf, "\"%s\" ", foldedname);
      db2free (foldedname);

      // check charlen is not 0; set it to 1 in that case
      colSize = colSize == 0 ? 1 : colSize;
      /* data type */
      switch (c2dbType(colType)) {
        case DB2_CHAR:
          appendStringInfo (&buf, "character(%ld)", colSize);
          break;
        case DB2_VARCHAR:
          appendStringInfo (&buf, "character varying(%ld)", colSize);
          break;
        case DB2_LONGVARCHAR:
        case DB2_CLOB:
        case DB2_VARGRAPHIC:
        case DB2_GRAPHIC:
        case DB2_DBCLOB:
          appendStringInfo (&buf, "text");
          break;
        case DB2_SMALLINT:
          appendStringInfo (&buf, "smallint");
          break;
        case DB2_INTEGER:
          appendStringInfo (&buf, "integer");
          break;
        case DB2_BIGINT:
          appendStringInfo (&buf, "bigint");
          break;
        case DB2_BOOLEAN:
          appendStringInfo (&buf, "boolean");
          break;
        case DB2_NUMERIC:
          appendStringInfo (&buf, "numeric(%ld,%d)", colSize, colScale);
          break;
        case DB2_DECIMAL:
          appendStringInfo (&buf, "decimal(%ld,%d)", colSize, colScale);
          break;
        case DB2_DOUBLE:
          appendStringInfo (&buf, "double precision");
          break;
        case DB2_DECFLOAT:
        case DB2_FLOAT:
          colSize = (colSize > 8) ? 8 : colSize;
          appendStringInfo (&buf, "float(%ld)", colSize);
          break;
        case DB2_REAL:
          appendStringInfo (&buf, "real");
          break;
        case DB2_XML:
          appendStringInfo (&buf, "xml");
          break;
        case DB2_BINARY:
        case DB2_VARBINARY:
        case DB2_LONGVARBINARY:
        case DB2_BLOB:
          appendStringInfo (&buf, "bytea");
          break;
        case DB2_TYPE_DATE:
          appendStringInfo (&buf, "date");
          break;
        case DB2_TYPE_TIMESTAMP:
          appendStringInfo (&buf, "timestamp(%d)", (colScale > 6) ? 6 : colScale);
          break;
        case DB2_TYPE_TIMESTAMP_WITH_TIMEZONE:
          appendStringInfo (&buf, "timestamp(%d) with time zone", (colScale > 6) ? 6 : colScale);
          break;
        case DB2_TYPE_TIME:
          appendStringInfo (&buf, "time(%d)", (colScale > 6) ? 6 : colScale);
          break;
        default:
          elog (DEBUG2, "column \"%s\" of table \"%s\" has an untranslatable data type", colname, tabname);
          appendStringInfo (&buf, "text");
          break;
      }
      /* part of the primary key */
      if (key)
        appendStringInfo (&buf, " OPTIONS (key 'true')");
      /* not nullable */
      if (!colNulls)
        appendStringInfo (&buf, " NOT NULL");
    }
  } while (rc == 1);
  db2Debug2("result(%d):%x",list_length(result), result);
  db2Debug1("< db2ImportForeignSchema : result(%d):%x",list_length(result), result);
  return result;
}

/* fold_case
 * Returns a dup'ed string that is the case-folded first argument.
 */
char* fold_case (char *name, fold_t foldcase) {
  char* result = NULL;
  db2Debug1("> fold_case");
  if (foldcase == CASE_KEEP) {
    result = db2strdup (name);
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
          result = db2strdup (name);
      }
    }
  }
  if (result == NULL) {
     elog (ERROR, "impossible case folding type %d", foldcase);
  }
  db2Debug1("< fold_case - returns: '%s'", result);
  return result;
}
