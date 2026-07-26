#include <postgres.h>
#include <sqlcli.h>
#include <access/heapam.h>
#if PG_VERSION_NUM < 140000
#include <access/xact.h>
#endif
#include <catalog/pg_collation.h>
#include <miscadmin.h>
#include <utils/formatting.h>
#include <utils/lsyscache.h>
#include "db2_fdw.h"
#include "DB2FdwState.h"
#include "DB2FdwDirectModifyState.h"

/** external prototypes */
extern bool         optionIsTrue  (const char* value);
extern DB2Session*  db2GetSession (const char* connectstring, char* user, char* password, char* jwt_token, int curlevel);
extern DB2Table*    db2Describe   (DB2Session* session, char* schema, char* table, char* pgname, char* noencerr, char* batchsz);
extern char*        db2CopyText   (const char* string, int size, int quote);
extern char*        c2name        (short fcType);

/** local prototypes */
       DB2FdwState*             db2GetFdwState            (Oid foreigntableid, double* sample_percent, bool describe);
       DB2FdwDirectModifyState* db2GetFdwDirectModifyState(Oid foreigntableid, double* sample_percent, bool describe);
static DB2Table*                describeForeignTable      (Oid foreigntableid, char* schema, char* table, char* pgname, char* noencerr, char* batchsz);
static void                     getColumnData             (DB2Table* db2Table, Oid foreigntableid);
static void                     getOptions                (Oid foreigntableid, List** options);


/* db2GetFdwState
 * Construct an DB2FdwState from the options of the foreign table.
 * Establish an DB2 connection and get a description of the remote table.
 * "sample_percent" is set from the foreign table options.
 * "sample_percent" can be NULL, in that case it is not set.
 */
DB2FdwState* db2GetFdwState (Oid foreigntableid, double* sample_percent, bool describe) {
  DB2FdwState* fdwState    = db2alloc(sizeof (DB2FdwState),"DB2FdwState* fdwState");
  char*        pgtablename = get_rel_name (foreigntableid);
  List*        options     = NIL;
  ListCell*    cell        = NULL;
  char*        schema      = NULL;
  char*        table       = NULL;
  char*        sample      = NULL;
  char*        prefetch    = NULL;
  char*        fetchsz     = NULL;
  char*        noencerr    = NULL;
  char*        batchsz     = NULL;

  db2Entry1();
  /* Get all relevant options from the foreign table, the user mapping, the foreign server and the foreign data wrapper. */
  getOptions (foreigntableid, &options);

  foreach (cell, options) {
    DefElem *def = (DefElem *) lfirst (cell);
    fdwState->dbserver  = (strcmp (def->defname, OPT_DBSERVER)          == 0) ? STRVAL(def->arg) : fdwState->dbserver;
    fdwState->user      = (strcmp (def->defname, OPT_USER)              == 0) ? STRVAL(def->arg) : fdwState->user;
    fdwState->password  = (strcmp (def->defname, OPT_PASSWORD)          == 0) ? STRVAL(def->arg) : fdwState->password;
    fdwState->jwt_token = (strcmp (def->defname, OPT_JWT_TOKEN)         == 0) ? STRVAL(def->arg) : fdwState->jwt_token;
    schema              = (strcmp (def->defname, OPT_SCHEMA)            == 0) ? STRVAL(def->arg) : schema;
    table               = (strcmp (def->defname, OPT_TABLE)             == 0) ? STRVAL(def->arg) : table;
    sample              = (strcmp (def->defname, OPT_SAMPLE)            == 0) ? STRVAL(def->arg) : sample;
    prefetch            = (strcmp (def->defname, OPT_PREFETCH)          == 0) ? STRVAL(def->arg) : prefetch;
    fetchsz             = (strcmp (def->defname, OPT_FETCHSZ)           == 0) ? STRVAL(def->arg) : fetchsz;
    noencerr            = (strcmp (def->defname, OPT_NO_ENCODING_ERROR) == 0) ? STRVAL(def->arg) : noencerr;
    batchsz             = (strcmp (def->defname, OPT_BATCH_SIZE)        == 0) ? STRVAL(def->arg) : batchsz;
  }

  /* convert "sample_percent" to double */
  if (sample_percent != NULL) {
    if (sample == NULL)
      *sample_percent = DEFAULT_SAMPLE_PERCENT;
    else
      *sample_percent = strtod (sample, NULL);
  }
  /* convert "prefetch" to number (or use default) */
  fdwState->prefetch   = (prefetch == NULL) ? DEFAULT_PREFETCH : (unsigned long) strtoul (prefetch, NULL, 0);

  /* convert "fetchsize" to number (or use default) */
  fdwState->fetch_size = (fetchsz == NULL) ? DEFAULT_FETCHSZ : (int) strtol (fetchsz, NULL, 0);

    /* check if options are ok */
  if (table == NULL) {
    ereport (ERROR, (errcode (ERRCODE_FDW_OPTION_NAME_NOT_FOUND), errmsg ("required option \"%s\" in foreign table \"%s\" missing", OPT_TABLE, pgtablename)));
  }

  if (describe) {
    fdwState->db2Table = describeForeignTable(foreigntableid, schema, table, pgtablename, noencerr, batchsz);
    if (fdwState->db2Table == NULL) {
      /* connect to DB2 database */
      fdwState->session = db2GetSession (fdwState->dbserver, fdwState->user, fdwState->password, fdwState->jwt_token, GetCurrentTransactionNestLevel () );
      /* get remote table description */
      fdwState->db2Table = db2Describe (fdwState->session, schema, table, pgtablename, noencerr, batchsz);
      /* add PostgreSQL data to table description */
      getColumnData (fdwState->db2Table, foreigntableid);
    }
  }

  db2Exit1(": %x", fdwState);
  return fdwState;
}

/* db2GetFdwDirectModifyState
 * Construct an DB2FdwState from the options of the foreign table.
 * Establish an DB2 connection and get a description of the remote table.
 * "sample_percent" is set from the foreign table options.
 * "sample_percent" can be NULL, in that case it is not set.
 */
DB2FdwDirectModifyState* db2GetFdwDirectModifyState (Oid foreigntableid, double* sample_percent, bool describe) {
  DB2FdwDirectModifyState* fdwState    = db2alloc(sizeof (DB2FdwDirectModifyState),"DB2FdwDirectModifyState* fdwState");
  char*        pgtablename = get_rel_name (foreigntableid);
  List*        options     = NIL;
  ListCell*    cell        = NULL;
  char*        schema      = NULL;
  char*        table       = NULL;
  char*        sample      = NULL;
  char*        prefetch    = NULL;
  char*        fetchsz     = NULL;
  char*        noencerr    = NULL;
  char*        batchsz     = NULL;

  db2Entry1();
  /* Get all relevant options from the foreign table, the user mapping, the foreign server and the foreign data wrapper. */
  getOptions (foreigntableid, &options);

  foreach (cell, options) {
    DefElem *def = (DefElem *) lfirst (cell);
    fdwState->dbserver  = (strcmp (def->defname, OPT_DBSERVER)          == 0) ? STRVAL(def->arg) : fdwState->dbserver;
    fdwState->user      = (strcmp (def->defname, OPT_USER)              == 0) ? STRVAL(def->arg) : fdwState->user;
    fdwState->password  = (strcmp (def->defname, OPT_PASSWORD)          == 0) ? STRVAL(def->arg) : fdwState->password;
    fdwState->jwt_token = (strcmp (def->defname, OPT_JWT_TOKEN)         == 0) ? STRVAL(def->arg) : fdwState->jwt_token;
    schema              = (strcmp (def->defname, OPT_SCHEMA)            == 0) ? STRVAL(def->arg) : schema;
    table               = (strcmp (def->defname, OPT_TABLE)             == 0) ? STRVAL(def->arg) : table;
    sample              = (strcmp (def->defname, OPT_SAMPLE)            == 0) ? STRVAL(def->arg) : sample;
    prefetch            = (strcmp (def->defname, OPT_PREFETCH)          == 0) ? STRVAL(def->arg) : prefetch;
    fetchsz             = (strcmp (def->defname, OPT_FETCHSZ)           == 0) ? STRVAL(def->arg) : fetchsz;
    noencerr            = (strcmp (def->defname, OPT_NO_ENCODING_ERROR) == 0) ? STRVAL(def->arg) : noencerr;
    batchsz             = (strcmp (def->defname, OPT_BATCH_SIZE)        == 0) ? STRVAL(def->arg) : batchsz;
  }

  /* convert "sample_percent" to double */
  if (sample_percent != NULL) {
    if (sample == NULL)
      *sample_percent = DEFAULT_SAMPLE_PERCENT;
    else
      *sample_percent = strtod (sample, NULL);
  }
  /* convert "prefetch" to number (or use default) */
  fdwState->prefetch   = (prefetch == NULL) ? DEFAULT_PREFETCH : (unsigned long) strtoul (prefetch, NULL, 0);

  /* convert "fetchsize" to number (or use default) */
  fdwState->fetch_size = (fetchsz == NULL) ? DEFAULT_FETCHSZ : (int) strtol (fetchsz, NULL, 0);

    /* check if options are ok */
  if (table == NULL) {
    ereport (ERROR, (errcode (ERRCODE_FDW_OPTION_NAME_NOT_FOUND), errmsg ("required option \"%s\" in foreign table \"%s\" missing", OPT_TABLE, pgtablename)));
  }

  if (describe) {
    fdwState->db2Table = describeForeignTable(foreigntableid, schema, table, pgtablename, noencerr, batchsz);
  }
  fdwState->session  = db2GetSession (fdwState->dbserver, fdwState->user, fdwState->password, fdwState->jwt_token, GetCurrentTransactionNestLevel () );

  db2Exit1();
  return fdwState;
}


static DB2Table* describeForeignTable (Oid foreigntableid, char* schema, char* table, char* pgname, char* noencerr, char* batchsz) {
  DB2Table* db2Table = NULL;
  char*     qtable    = NULL;
  char*     qschema   = NULL;
  char*     tablename = NULL;
  Relation  rel;
  TupleDesc tupdesc;
  int       length    = 0;

  db2Entry2();

  db2Table = (DB2Table*)db2alloc(sizeof (DB2Table),"DB2Table* db2_table");
  /* get a complete quoted table name */
  qtable = db2CopyText (table, strlen (table), 1);
  length = strlen (qtable);
  if (schema != NULL) {
    qschema = db2CopyText (schema, strlen (schema), 1);
    length += strlen (qschema) + 1;
  }
  tablename = db2alloc (length + 1,"db2Table->name");
  tablename[0] = '\0';		/* empty */
  if (schema != NULL) {
    strncat (tablename, qschema, length);
    strncat (tablename, ".", length);
  }
  strncat (tablename, qtable,length);
  db2free (qtable,"qtable");
  if (schema != NULL)
    db2free (qschema,"qschema");

  db2Table->name = tablename;
  db2Debug3("table description");
  db2Debug3("db2Table->name    : '%s'", db2Table->name);
  db2Table->pgname = pgname;
  db2Debug3("db2Table->pgname  : '%s'", db2Table->pgname);

  db2Table->batchsz = DEFAULT_BATCHSZ;
  if (batchsz != NULL) {
    char* end;
    db2Table->batchsz = strtol(batchsz,&end,10);
    db2Debug3("db2Table->batchsz : %d", db2Table->batchsz);
  }

  rel = table_open (foreigntableid, NoLock);
  tupdesc = rel->rd_att;

  db2Table->npgcols = tupdesc->natts;
  db2Debug3("db2Table->npgcols : %d", db2Table->npgcols);
  db2Table->ncols   = tupdesc->natts;
  db2Debug3("db2Table->ncols   : %d", db2Table->ncols);
  db2Table->cols    = (DB2Column**)  db2alloc(sizeof (DB2Column*) * db2Table->ncols,"DB2Columns* db2Table->cols(%d)",db2Table->ncols);
  db2Debug3("db2Table->cols    : %x", db2Table->cols);

  /* loop through foreign table columns */
  for (int i = 0, cidx = 0; i < tupdesc->natts; ++i) {
    Form_pg_attribute att_tuple = TupleDescAttr (tupdesc, i);
    List*             options   = NIL;
    ListCell*         option    = NULL;

    /* ignore dropped columns */
    if (att_tuple->attisdropped) {
      continue;
    }
    /* get PostgreSQL column number and type */
    if (cidx <= db2Table->ncols) {
      int           bin_size  = 0;
      int           charlen   = 0;
      unsigned int  colSize   = 0;
      short         scale     = 0;
      bool          db2type_set = false;
      bool          db2size_set = false;
      bool          db2bytes_set = false;
      bool          db2chars_set = false;
      bool          db2scale_set = false;
      bool          db2nulls_set = false;
      bool          db2codepage_set = false;

      db2Table->cols[cidx]           = (DB2Column*) db2alloc (sizeof (DB2Column),"DB2Column db2Table->cols[%d]",cidx);
      db2Table->cols[cidx]->used     = 0;
      db2Table->cols[cidx]->pgattnum = att_tuple->attnum;
      db2Table->cols[cidx]->pgtype   = att_tuple->atttypid;
      db2Table->cols[cidx]->pgtypmod = att_tuple->atttypmod;
      db2Table->cols[cidx]->pgname   = db2strdup (NameStr(att_tuple->attname),"db2Table->cols[%d]->pgname",cidx);
      db2Table->cols[cidx]->colName  = db2CopyText ( str_toupper(db2Table->cols[cidx]->pgname, strlen(db2Table->cols[cidx]->pgname), DEFAULT_COLLATION_OID)
                                                   , strlen(db2Table->cols[cidx]->pgname)
                                                   , 1
                                                   );
      /* loop through column options */
      options = GetForeignColumnOptions (foreigntableid, att_tuple->attnum);
      if (noencerr != NULL) {
        db2Table->cols[cidx]->noencerr = optionIsTrue(noencerr) ? NO_ENC_ERR_TRUE : NO_ENC_ERR_FALSE;
      } else {
        db2Table->cols[cidx]->noencerr = NO_ENC_ERR_NULL;
      }
      foreach (option, options) {
        DefElem* def = (DefElem*) lfirst (option);
        if (strcmp (def->defname, OPT_KEY) == 0) {
          db2Table->cols[cidx]->pkey     = optionIsTrue ((STRVAL(def->arg))) ? 1 : 0;                             /* is it the "key" option and is it set to "true" ? */
          db2Table->cols[cidx]->colPrimKeyPart  = db2Table->cols[cidx]->pkey;
        } else if (strcmp (def->defname, OPT_NO_ENCODING_ERROR) == 0) {
          db2Table->cols[cidx]->noencerr = optionIsTrue((STRVAL(def->arg))) ? NO_ENC_ERR_TRUE : NO_ENC_ERR_FALSE; /* is it the "no_encoding_error" option set         */
        } else if (strcmp (def->defname, OPT_DB2TYPE) == 0) {
          db2type_set = true;
          db2Table->cols[cidx]->colType = (short)strtol(STRVAL(def->arg), NULL, 10);
        } else if (strcmp (def->defname, OPT_DB2SIZE) == 0) {
          db2size_set = true;
          db2Table->cols[cidx]->colSize = (size_t)strtol(STRVAL(def->arg), NULL, 10);
        } else if (strcmp (def->defname, OPT_DB2BYTES) == 0) {
          db2bytes_set = true;
          db2Table->cols[cidx]->colBytes = (size_t)strtol(STRVAL(def->arg), NULL, 10);
        } else if (strcmp (def->defname, OPT_DB2CHARS) == 0) {
          db2chars_set = true;
          db2Table->cols[cidx]->colChars = (size_t)strtol(STRVAL(def->arg), NULL, 10);
        } else if (strcmp (def->defname, OPT_DB2SCALE) == 0) {
          db2scale_set = true;
          db2Table->cols[cidx]->colScale = (short)strtol(STRVAL(def->arg), NULL, 10);
        } else if (strcmp (def->defname, OPT_DB2NULL) == 0) {
          db2nulls_set = true;
          db2Table->cols[cidx]->colNulls = (short)strtol(STRVAL(def->arg), NULL, 10);
        } else if (strcmp (def->defname, OPT_DB2CCSID) == 0) {
          db2codepage_set = true;
          db2Table->cols[cidx]->colCodepage = (int)strtol(STRVAL(def->arg), NULL, 10);
        }
      }
      if (!db2type_set || !db2size_set || !db2bytes_set || !db2chars_set || !db2scale_set || !db2nulls_set || !db2codepage_set) {
        db2Debug2("INFO: column %d - %s without required options, discarding db2Table", cidx, db2Table->cols[cidx]->pgname);
        db2free (db2Table,"db2Table");
        db2Table = NULL;
        break;
      }
      bin_size  = db2Table->cols[cidx]->colBytes;
      charlen   = db2Table->cols[cidx]->colChars;
      colSize   = db2Table->cols[cidx]->colSize;
      scale     = db2Table->cols[cidx]->colScale;
      // val_size berechnen
      /* determine db2Type and length to allocate */
      switch (db2Table->cols[cidx]->colType) {
        case SQL_CHAR:
        case SQL_VARCHAR:
        case SQL_LONGVARCHAR:
          db2Table->cols[cidx]->val_size = bin_size + 1;
        break;
        case SQL_BLOB:
        case SQL_CLOB:
          db2Table->cols[cidx]->val_size = bin_size + 1;
        break;
        case SQL_GRAPHIC:
        case SQL_VARGRAPHIC:
        case SQL_LONGVARGRAPHIC:
        case SQL_WCHAR:
        case SQL_WVARCHAR:
        case SQL_WLONGVARCHAR:
        case SQL_DBCLOB:
          db2Table->cols[cidx]->val_size = bin_size + 1;
        break;
        case SQL_BOOLEAN:
          db2Table->cols[cidx]->val_size = bin_size + 1;
        break;
        case SQL_INTEGER:
        case SQL_SMALLINT:
            db2Table->cols[cidx]->val_size = charlen + 2;
        break;
        case SQL_NUMERIC:
        case SQL_DECIMAL:
          if (db2Table->cols[cidx]->colScale == 0)
            db2Table->cols[cidx]->val_size = bin_size;
          else
            db2Table->cols[cidx]->val_size = (scale > colSize ? scale : colSize) + 5;
        break;
        case SQL_REAL:
        case SQL_DOUBLE:
        case SQL_FLOAT:
        case SQL_DECFLOAT:
          db2Table->cols[cidx]->val_size = 24 + 1;
        break;
        case SQL_TYPE_DATE:
        case SQL_TYPE_TIME:
        case SQL_TYPE_TIMESTAMP:
        case SQL_TYPE_TIMESTAMP_WITH_TIMEZONE:
          db2Table->cols[cidx]->val_size = colSize + 1;
        break;
        case SQL_BIGINT:
          db2Table->cols[cidx]->val_size = 24;
        break;
        case SQL_XML:
          db2Table->cols[cidx]->val_size = LOB_CHUNK_SIZE + 1;
        break;
        case SQL_BINARY:
        case SQL_VARBINARY:
        case SQL_LONGVARBINARY:
          db2Table->cols[cidx]->val_size = bin_size;
        break;
        default:
        break;
      }
      db2Debug3("db2Table->cols >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
      db2Debug3("db2Table->cols[%d] : %x" , cidx, db2Table->cols[cidx]);
      db2Debug3("db2Table->cols[%d]->colName        : %s" , cidx, db2Table->cols[cidx]->colName);
      db2Debug3("db2Table->cols[%d]->colType        : %d - (%s)" , cidx, db2Table->cols[cidx]->colType,c2name(db2Table->cols[cidx]->colType));
      db2Debug3("db2Table->cols[%d]->colSize        : %ld", cidx, db2Table->cols[cidx]->colSize);
      db2Debug3("db2Table->cols[%d]->colScale       : %d" , cidx, db2Table->cols[cidx]->colScale);
      db2Debug3("db2Table->cols[%d]->colNulls       : %d" , cidx, db2Table->cols[cidx]->colNulls);
      db2Debug3("db2Table->cols[%d]->colChars       : %ld", cidx, db2Table->cols[cidx]->colChars);
      db2Debug3("db2Table->cols[%d]->colBytes       : %ld", cidx, db2Table->cols[cidx]->colBytes);
      db2Debug3("db2Table->cols[%d]->colPrimKeyPart : %d" , cidx, db2Table->cols[cidx]->colPrimKeyPart);
      db2Debug3("db2Table->cols[%d]->colCodepage    : %d" , cidx, db2Table->cols[cidx]->colCodepage);
      db2Debug3("db2Table->cols[%d]->pgrelid        : %d" , cidx, db2Table->cols[cidx]->pgrelid);
      db2Debug3("db2Table->cols[%d]->pgname         : %s" , cidx, db2Table->cols[cidx]->pgname);
      db2Debug3("db2Table->cols[%d]->pgattnum       : %d" , cidx, db2Table->cols[cidx]->pgattnum);
      db2Debug3("db2Table->cols[%d]->pgtype         : %d" , cidx, db2Table->cols[cidx]->pgtype);
      db2Debug3("db2Table->cols[%d]->pgtypmod       : %d" , cidx, db2Table->cols[cidx]->pgtypmod);
      db2Debug3("db2Table->cols[%d]->used           : %d" , cidx, db2Table->cols[cidx]->used);
      db2Debug3("db2Table->cols[%d]->pkey           : %d" , cidx, db2Table->cols[cidx]->pkey);
      db2Debug3("db2Table->cols[%d]->val_size       : %ld", cidx, db2Table->cols[cidx]->val_size);
      db2Debug3("db2Table->cols[%d]->noencerr       : %d" , cidx, db2Table->cols[cidx]->noencerr);
    }
    ++cidx;
  }

  table_close (rel, NoLock);
  db2Exit2(": %x", db2Table);
  return db2Table;
}

/* getColumnData
 * Get PostgreSQL column name and number, data type and data type modifier.
 * Set db2Table->npgcols.
 * For PostgreSQL 9.2 and better, find the primary key columns and mark them in db2Table.
 */
static void getColumnData (DB2Table* db2Table, Oid foreigntableid) {
  Relation rel;
  TupleDesc tupdesc;
  int i, index;

  db2Entry4();
  rel = table_open (foreigntableid, NoLock);
  tupdesc = rel->rd_att;

  /* number of PostgreSQL columns */
  db2Table->npgcols = tupdesc->natts;

  /* loop through foreign table columns */
  index = 0;  
  for (i = 0; i < tupdesc->natts; ++i) {
    Form_pg_attribute att_tuple = TupleDescAttr (tupdesc, i);
    List*             options;
    ListCell*         option;

    /* ignore dropped columns */
    if (att_tuple->attisdropped)
      continue;

    ++index;
    /* get PostgreSQL column number and type */
    if (index <= db2Table->ncols) {
      db2Table->cols[index - 1]->pgattnum = att_tuple->attnum;
      db2Table->cols[index - 1]->pgtype   = att_tuple->atttypid;
      db2Table->cols[index - 1]->pgtypmod = att_tuple->atttypmod;
      db2Table->cols[index - 1]->pgname   = db2strdup (NameStr(att_tuple->attname),"db2Table->cols[%d - 1]->pgname", index);
    }

    /* loop through column options */
    options = GetForeignColumnOptions (foreigntableid, att_tuple->attnum);
    foreach (option, options) {
      DefElem* def = (DefElem*) lfirst (option);
      /* is it the "key" option and is it set to "true" ? */
      if (strcmp (def->defname, OPT_KEY) == 0 && optionIsTrue ((STRVAL(def->arg)))) {
        /* mark the column as primary key column */
        db2Table->cols[index - 1]->pkey           = 1;
      }
      /* is it the "no_encoding_error" option set */
      if (strcmp (def->defname, OPT_NO_ENCODING_ERROR) == 0) {
        db2Table->cols[index - 1]->noencerr = optionIsTrue((STRVAL(def->arg))) ? NO_ENC_ERR_TRUE : NO_ENC_ERR_FALSE; 
      }
    }
  }

  table_close (rel, NoLock);
  db2Exit4();
}

/* getOptions
 * Fetch the options for an db2_fdw foreign table.
 * Returns a union of the options of the foreign data wrapper, the foreign server, the user mapping and the foreign table, in that order. 
 * Column options are ignored.
 */
static void getOptions (Oid foreigntableid, List** options) {
  ForeignDataWrapper* wrapper = NULL;
  ForeignServer*      server  = NULL;
  UserMapping*        mapping = NULL;
  ForeignTable*       table   = NULL;

  db2Entry4();
  /** Gather all data for the foreign table. */
  table = GetForeignTable(foreigntableid);
  if (table != NULL) {
    server  = GetForeignServer(table->serverid);
    mapping = GetUserMapping(GetUserId(), table->serverid);
    if (server != NULL) {
      wrapper = GetForeignDataWrapper(server->fdwid);
    } else {
        db2Debug5("unable to GetForeignServer: %d", table->serverid);
    }
    /* later options override earlier ones */
    *options = NIL;
    if (wrapper != NULL)
      *options = list_concat(*options, wrapper->options);
    else
      db2Debug5("unable to get wrapper options");
    if (server != NULL)
      *options = list_concat(*options, server->options);
    else
      db2Debug5("unable to get server options");
    if (mapping != NULL)
      *options = list_concat(*options, mapping->options);
    else
      db2Debug5("unable to get mapping options");
    *options = list_concat(*options, table->options);
  } else {
    db2Debug5("unable to GetForeignTable: %d",foreigntableid);
  }
  db2Exit4();
}