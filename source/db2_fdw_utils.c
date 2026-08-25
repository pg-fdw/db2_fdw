#include <postgres.h>
#include <catalog/dependency.h>
#include <catalog/pg_type.h>
#include <mb/pg_wchar.h>
#include <miscadmin.h>
#include <optimizer/optimizer.h>
#include <optimizer/paths.h>
#include <utils/builtins.h>
#include <utils/float.h>
#include <utils/guc.h>
#include <utils/hsearch.h>
#include <utils/inval.h>
#include <utils/syscache.h>
#include "db2_fdw.h"
#include "DB2FdwState.h"

/* Hash table for caching the results of shippability lookups */
static HTAB* ShippableCacheHash = NULL;

/* Hash key for shippability lookups.
 * We include the FDW server OID because decisions may differ per-server.
 * Otherwise, objects are identified by their (local!) OID and catalog OID.
 */
typedef struct {
  /* XXX we assume this struct contains no padding bytes  */
  Oid objid;    /* function/operator/type OID             */
  Oid classid;  /* OID of its catalog (pg_proc, etc)      */
  Oid serverid; /* FDW server we are concerned with       */
} ShippableCacheKey;

typedef struct {
  ShippableCacheKey key;        /* hash key - must be first */
  bool              shippable;
} ShippableCacheEntry;


/** external prototypes */
extern void         db2GetLob                  (DB2Session* session, DB2ResultColumn* column, char** value, long* value_len);
extern void         db2Shutdown                (void);
extern short        c2dbType                   (short fcType);

/** local prototypes */
bool                optionIsTrue               (const char *value);
int                 guessDb2ClientCodepage     (void);
void                exitHook                   (int code, Datum arg);
void                convertTuple               (DB2Session* session, DB2ResultColumn* reslist, DB2TupleIndexMode index_mode, int natts, Datum* values, bool* nulls);
void                reset_transmission_modes   (int nestlevel);
int                 set_transmission_modes     (void);
bool                is_builtin                 (Oid objectId);
bool                is_shippable               (Oid objectId, Oid classId, DB2FdwState* fpinfo);
static void         InvalidateShippableCacheCbk(Datum arg, int cacheid, uint32 hashvalue);
static void         InitializeShippableCache   (void);
static bool         lookup_shippable           (Oid objectId, Oid classId, DB2FdwState* fpinfo);

/* optionIsTrue
 * Returns true if the string is "true", "on" or "yes".
 */
bool optionIsTrue (const char* value) {
  bool result = false;
  db2Entry4("(value: '%s')", value);
  result = (pg_strcasecmp (value, "on") == 0 || pg_strcasecmp (value, "yes") == 0 || pg_strcasecmp (value, "true") == 0);
  db2Exit4(": '%s'",((result) ? "true" : "false"));
  return result;
}

/* guessDb2ClientCodepage
 * Return the DB2 CCSID matching PostgreSQL's server_encoding, so the DB2
 * CLI connection's SQL_ATTR_CLIENT_CODEPAGE can be set to it explicitly.
 *
 * Without this, DB2 CLI negotiates its client codepage from ambient
 * OS/db2cli.ini configuration, which has no guaranteed relationship to
 * server_encoding: CLOB/CHAR data gets transcoded by the CLI driver to
 * whatever that ambient codepage happens to be, not to what PostgreSQL
 * actually expects, silently corrupting non-ASCII data on the way in.
 *
 * Returns 0 (caller should skip setting the attribute, leaving today's
 * ambient-codepage behavior in place) if server_encoding has no
 * confidently-known CCSID equivalent.
 */
int guessDb2ClientCodepage (void) {
  char* server_encoding = NULL;
  int   ccsid           = 0;

  db2Entry4();
  server_encoding = db2strdup (GetConfigOption ("server_encoding", false, true),"server_encoding");
  if (strcmp (server_encoding, "UTF8") == 0)
    ccsid = 1208;
  else if (strcmp (server_encoding, "ISO_8859_5") == 0)
    ccsid = 915;
  else if (strcmp (server_encoding, "ISO_8859_6") == 0)
    ccsid = 1089;
  else if (strcmp (server_encoding, "ISO_8859_7") == 0)
    ccsid = 813;
  else if (strcmp (server_encoding, "ISO_8859_8") == 0)
    ccsid = 916;
  else if (strcmp (server_encoding, "LATIN1") == 0)
    ccsid = 819;
  else if (strcmp (server_encoding, "LATIN2") == 0)
    ccsid = 912;
  else if (strcmp (server_encoding, "LATIN3") == 0)
    ccsid = 913;
  else if (strcmp (server_encoding, "LATIN4") == 0)
    ccsid = 914;
  else if (strcmp (server_encoding, "LATIN5") == 0)
    ccsid = 920;
  else if (strcmp (server_encoding, "LATIN9") == 0)
    ccsid = 923;
  else if (strcmp (server_encoding, "WIN866") == 0)
    ccsid = 866;
  else if (strcmp (server_encoding, "WIN1250") == 0)
    ccsid = 1250;
  else if (strcmp (server_encoding, "WIN1251") == 0)
    ccsid = 1251;
  else if (strcmp (server_encoding, "WIN1252") == 0)
    ccsid = 1252;
  else if (strcmp (server_encoding, "WIN1253") == 0)
    ccsid = 1253;
  else if (strcmp (server_encoding, "WIN1254") == 0)
    ccsid = 1254;
  else if (strcmp (server_encoding, "WIN1255") == 0)
    ccsid = 1255;
  else if (strcmp (server_encoding, "WIN1256") == 0)
    ccsid = 1256;
  else if (strcmp (server_encoding, "WIN1257") == 0)
    ccsid = 1257;
  else if (strcmp (server_encoding, "WIN1258") == 0)
    ccsid = 1258;
  else {
    ereport (WARNING,(errcode (ERRCODE_WARNING)
                    ,errmsg ("no known DB2 CCSID for database encoding \"%s\"", server_encoding)
                    ,errdetail ("The DB2 CLI connection's client codepage will be left at its ambient default, which may not match this encoding.")
                    ,errhint ("CLOB and character data containing non-ASCII characters may be transcoded incorrectly.")
                    )
            );
  }
  db2free (server_encoding,"server_encoding");
  db2Exit4(": %d", ccsid);
  return ccsid;
}

/* exitHook
 * Close all DB2 connections on process exit.
 */
void exitHook (int code, Datum arg) {
  db2Entry4();
  db2Shutdown ();
  db2Exit4();
}

/* convertTuple
 * Convert a result row from DB2 stored in db2Table into arrays of values and null indicators.
 */
void convertTuple (DB2Session* session, DB2ResultColumn* reslist, DB2TupleIndexMode index_mode, int natts, Datum* values, bool* nulls) {
  char*                value          = NULL;
  long                 value_len      = 0;
  int                  j              = 0;
  DB2ResultColumn*     res            = NULL;

  db2Entry4();
  db2Debug5("natts: %d", natts);
  db2Debug5("tuple index mode: %s",
            index_mode == DB2_TUPLE_INDEX_RESULT ? "result" : "attribute");

  // initialize all columns to NULL
  for (j = 0; j < natts; j++) {
    nulls[j]  = true;
    values[j] = PointerGetDatum (NULL);
  }

  for (res = reslist; res; res = res->next) {
    /*
     * Never infer the slot layout from natts.  A SELECT that returns every
     * base column in a different order has natts == npgcols, while its
     * fdw_scan_tlist (and therefore its TupleTableSlot) still follows result
     * order.  Using pgattnum in that case stores by-value data in by-reference
     * attributes (or vice versa), and PostgreSQL later dereferences an invalid
     * Datum while materializing the slot.
     */
    j = (index_mode == DB2_TUPLE_INDEX_RESULT ? res->resnum : res->pgattnum) - 1;

    /*
     * Result metadata is assembled by the planner and crosses a serialization
     * boundary before execution.  Never trust an invalid attribute/result
     * number here: writing outside the TupleSlot arrays terminates the
     * PostgreSQL backend instead of producing a normal SQL error.
     */
    if (j < 0 || j >= natts) {
      ereport(ERROR,
              (errcode(ERRCODE_FDW_ERROR),
               errmsg("db2_fdw result column index out of range"),
               errdetail("Result column %d maps to tuple index %d, but the tuple has %d attributes.",
                         res->resnum, j, natts)));
    }

    db2Debug5("start processing column %d of %d: values index = %d", res->resnum, natts, j);
    db2Debug5("res->pgname   : %s"  ,res->pgname  );
    db2Debug5("res->pgattnum : %d"  ,res->pgattnum);
    db2Debug5("res->pgtype   : %d"  ,res->pgtype  );
    db2Debug5("res->pgtypmod : %d"  ,res->pgtypmod);
    db2Debug5("res->val      : %s"  ,res->val ? res->val : "(null)");
    db2Debug5("res->val_len  : %ld"  ,(long) res->val_len );
    db2Debug5("res->val_null : %ld"  ,(long) res->val_null);

    if (res->val_null >= 0) {
      short db2Type = 0;
      /* from here on, we can assume columns to be NOT NULL */
      nulls[j] = false;
  
      /* get the data and its length */
      switch(c2dbType(res->colType)) {
        case DB2_BLOB:
        case DB2_CLOB: {
          db2Debug5("DB2_BLOB or DB2CLOB");
          /* for LOBs, get the actual LOB contents (allocated), truncated if desired */
          db2GetLob (session, res, &value, &value_len);
        }
        break;
        case DB2_LONGVARBINARY: {
          db2Debug5("DB2_LONGBINARY datatypes");
          /* for LONG and LONG RAW, the first 4 bytes contain the length */
          value_len = *((int32*) res->val);
          /* the rest is the actual data */
          value = res->val;
          /* terminating zero byte (needed for LONGs) */
          value[value_len] = '\0';
        }
        break;
        case DB2_FLOAT:
        case DB2_DECIMAL:
        case DB2_SMALLINT:
        case DB2_INTEGER:
        case DB2_REAL:
        case DB2_DECFLOAT:
        case DB2_DOUBLE: {
          char* tmp_value = NULL;
  
          db2Debug5("DB2_FLOAT, DECIMAL, SMALLINT, INTEGER, REAL, DECFLOAT, DOUBLE");
          value     = res->val;
          value_len = res->val_len;
          if (value == NULL || res->val_size == 0) {
            ereport(ERROR,
                    (errcode(ERRCODE_FDW_ERROR),
                     errmsg("db2_fdw received a non-NULL result without a value buffer")));
          }
          if (value_len == 0)
            value_len = strnlen(value, res->val_size);
          if ((size_t) value_len >= res->val_size)
            value_len = res->val_size - 1;
          value[value_len] = '\0';
          tmp_value = memchr(value, ',', value_len);
          if(tmp_value != NULL) {
            *tmp_value = '.';
          }
        }
        break;
        default: {
          db2Debug5("should be string based values");
          /* for other data types, db2Table contains the results */
          value     = res->val;
          value_len = res->val_len;
          if (value == NULL || res->val_size == 0) {
            ereport(ERROR,
                    (errcode(ERRCODE_FDW_ERROR),
                     errmsg("db2_fdw received a non-NULL result without a value buffer")));
          }
          if (value_len == 0)
            value_len = strnlen(value, res->val_size);
          if ((size_t) value_len >= res->val_size)
            value_len = res->val_size - 1;
          value[value_len] = '\0';
        }
        break;
      }
      db2Debug5("value         : %s"  , value);
      db2Debug5("value_len     : %ld" , value_len);
      /* fill the TupleSlot with the data (after conversion if necessary) */
      if (res->pgtype == BYTEAOID) {
        /* binary columns are not converted */
        bytea* result = (bytea*) db2alloc (value_len + VARHDRSZ,"result");
        memcpy (VARDATA (result), value, value_len);
        SET_VARSIZE (result, value_len + VARHDRSZ);
  
        values[j] = PointerGetDatum (result);
      } else {
        regproc   typinput;
        HeapTuple tuple;
        Datum     dat;
        db2Debug5("pgtype: %d",res->pgtype);
        /* find the appropriate conversion function */
        tuple = SearchSysCache1 (TYPEOID, ObjectIdGetDatum (res->pgtype));
        if (!HeapTupleIsValid (tuple)) {
          elog (ERROR, "cache lookup failed for type %u", res->pgtype);
        }
        typinput = ((Form_pg_type) GETSTRUCT (tuple))->typinput;
        ReleaseSysCache (tuple);
        dat = CStringGetDatum (value);
        db2Debug5("CStringGetDatum(%s): %d",value, dat);
  
        /* for string types, check that the data are in the database encoding */
        if (res->pgtype == BPCHAROID || res->pgtype == VARCHAROID || res->pgtype == TEXTOID) {
          db2Debug5("pg_verify_mbstr");
          (void) pg_verify_mbstr (GetDatabaseEncoding(), value, value_len, res->noencerr == NO_ENC_ERR_TRUE);
        }
        /* call the type input function */
        switch (res->pgtype) {
          case BPCHAROID:
          case VARCHAROID:
          case TIMESTAMPOID:
          case TIMESTAMPTZOID:
          case TIMEOID:
          case TIMETZOID:
          case INTERVALOID:
          case NUMERICOID:
            /* these functions require the type modifier */
            values[j] = OidFunctionCall3 (typinput, dat, ObjectIdGetDatum (InvalidOid), Int32GetDatum (res->pgtypmod));
            db2Debug5("OidFunctionCall3 : values[%d]: %d", j, values[j]);
            break;
          default:
            /* the others don't */
            values[j] = OidFunctionCall1 (typinput, dat);
            db2Debug5("OidFunctionCall1 : values[%d]: %d", j, values[j]);
        }
      }
      /* release the data buffer for LOBs */
      db2Type = c2dbType(res->colType);
      if (db2Type == DB2_BLOB || db2Type == DB2_CLOB) {
        if (value != NULL) {
          db2free (value,"value");
        } else {
          db2Debug5("not freeing value, since it is null");
        }
      }
    } else {
      db2Debug5("column %d is NULL", res->resnum);
    }
  }
  db2Exit4();
}

/* Undo the effects of set_transmission_modes(). */
void reset_transmission_modes(int nestlevel) {
  db2Entry4();
  AtEOXact_GUC(true, nestlevel);
  db2Exit4();
}

/* Force assorted GUC parameters to settings that ensure that we'll output data values in a form that is unambiguous to the remote server.
 *
 * This is rather expensive and annoying to do once per row, but there's little choice if we want to be sure values are transmitted accurately;
 * we can't leave the settings in place between rows for fear of affecting user-visible computations.
 *
 * We use the equivalent of a function SET option to allow the settings to persist only until the caller calls reset_transmission_modes().  If an
 * error is thrown in between, guc.c will take care of undoing the settings.
 *
 * The return value is the nestlevel that must be passed to reset_transmission_modes() to undo things.
 */
int set_transmission_modes(void) {
  int nestlevel = NewGUCNestLevel();

  db2Entry4();
  /* The values set here should match what pg_dump does.  See also configure_remote_session in connection.c. */
  if (DateStyle != USE_ISO_DATES)
    (void) set_config_option("datestyle", "ISO", PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SAVE, true, 0, false);
  if (IntervalStyle != INTSTYLE_POSTGRES)
    (void) set_config_option("intervalstyle", "postgres", PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SAVE, true, 0, false);
  if (extra_float_digits < 3)
    (void) set_config_option("extra_float_digits", "3", PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SAVE, true, 0, false);

  /*
   * In addition force restrictive search_path, in case there are any
   * regproc or similar constants to be printed.
   */
  (void) set_config_option("search_path", "pg_catalog", PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SAVE, true, 0, false);

  db2Exit4(": %d", nestlevel);
  return nestlevel;
}

/* Return true if given object is one of PostgreSQL's built-in objects.
 *
 * We use FirstGenbkiObjectId as the cutoff, so that we only consider objects with hand-assigned OIDs to be "built in", not for instance any
 * function or type defined in the information_schema.
 *
 * Our constraints for dealing with types are tighter than they are for functions or operators: we want to accept only types that are in pg_catalog,
 * else deparse_type_name might incorrectly fail to schema-qualify their names.
 * Thus we must exclude information_schema types.
 *
 * XXX there is a problem with this, which is that the set of built-in objects expands over time.
 * Something that is built-in to us might not
 * be known to the remote server, if it's of an older version.
 * But keeping track of that would be a huge exercise.
 */
bool is_builtin (Oid objectId) {
  bool isBuiltin = (objectId < FirstGenbkiObjectId);
  db2Entry4();
  db2Exit4(": %s", (isBuiltin) ? "true": "false");
  return isBuiltin;
}

/* is_shippable
 * Is this object (function/operator/type) shippable to foreign server?
 */
bool is_shippable (Oid objectId, Oid classId, DB2FdwState* fpinfo) {
  ShippableCacheKey     key;
  ShippableCacheEntry*  entry     = NULL;
  bool                  shippable = false; 

  db2Entry4();
  /* Built-in objects are presumed shippable. */
  if (is_builtin(objectId)) {
    shippable = true;
  } else {
    /* Otherwise, give up if user hasn't specified any shippable extensions. */
    if (fpinfo->shippable_extensions == NIL) {
      shippable = false;
    } else {
      /* Initialize cache if first time through. */
      if (!ShippableCacheHash) {
        InitializeShippableCache();
      }

      /* Set up cache hash key */
      key.objid     = objectId;
      key.classid   = classId;
      key.serverid  = fpinfo->fserver->serverid;

      /* See if we already cached the result. */
      entry = (ShippableCacheEntry*) hash_search(ShippableCacheHash, &key, HASH_FIND, NULL);
      if (!entry) {
        /* Not found in cache, so perform shippability lookup. */
        shippable = lookup_shippable(objectId, classId, fpinfo);

        /* Don't create a new hash entry until *after* we have the shippable result in hand, as the underlying catalog lookups might trigger a
        * cache invalidation.
        */
        entry = (ShippableCacheEntry*) hash_search(ShippableCacheHash, &key, HASH_ENTER, NULL);
        entry->shippable = shippable;
      } else {
        db2Debug4("no shippable cache entry (%x) found shippable is set to false", entry);
        shippable = false;
      }
    }
  }
  db2Exit4(": %s", shippable ? "true" : "false");
  return shippable;
}

/* Flush cache entries when pg_foreign_server is updated.
 *
 * We do this because of the possibility of ALTER SERVER being used to change a server's extensions option.
 * We do not currently bother to check whether objects' extension membership changes once a shippability decision has been
 * made for them, however.
 */
static void InvalidateShippableCacheCbk(Datum arg, int cacheid, uint32 hashvalue) {
  HASH_SEQ_STATUS       status;
  ShippableCacheEntry*  entry;

  db2Entry5();
  /* In principle we could flush only cache entries relating to the pg_foreign_server entry being outdated; but that would be more
   * complicated, and it's probably not worth the trouble.
   * So for now, just flush all entries.
   */
  hash_seq_init(&status, ShippableCacheHash);
  while ((entry = (ShippableCacheEntry *) hash_seq_search(&status)) != NULL) {
    if (hash_search(ShippableCacheHash, &entry->key, HASH_REMOVE, NULL) == NULL)
      elog(ERROR, "hash table corrupted");
  }
  db2Exit5();
}

/* Initialize the backend-lifespan cache of shippability decisions. */
static void InitializeShippableCache(void) {
  HASHCTL ctl;

  db2Entry5();
  /* Create the hash table. */
  ctl.keysize   = sizeof(ShippableCacheKey);
  ctl.entrysize = sizeof(ShippableCacheEntry);
  ShippableCacheHash = hash_create("Shippability cache", 256, &ctl, HASH_ELEM | HASH_BLOBS);

  /* Set up invalidation callback on pg_foreign_server. */
  CacheRegisterSyscacheCallback(FOREIGNSERVEROID, InvalidateShippableCacheCbk, (Datum) 0);
  db2Exit5();
}

/* Returns true if given object (operator/function/type) is shippable according to the server options.
 *
 * Right now "shippability" is exclusively a function of whether the object belongs to an extension declared by the user.
 * In the future we could additionally have a list of functions/operators declared one at a time.
 */
static bool lookup_shippable(Oid objectId, Oid classId, DB2FdwState* fpinfo) {
  Oid   extensionOid = 0;
  bool  isValid       = false;

  db2Entry5();
  /* Is object a member of some extension?  (Note: this is a fairly expensive lookup, which is why we try to cache the results.) */
  extensionOid = getExtensionOfObject(classId, objectId);

  /* If so, is that extension in fpinfo->shippable_extensions? */
  isValid = (OidIsValid(extensionOid) && list_member_oid(fpinfo->shippable_extensions, extensionOid));
  db2Exit5(": %s", (isValid) ? "true" : "false");
  return false;
}
