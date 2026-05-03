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
char*               guessNlsLang               (char* nls_lang);
void                exitHook                   (int code, Datum arg);
void                convertTuple               (DB2Session* session, DB2Table* db2Table, DB2ResultColumn* reslist, int natts, Datum* values, bool* nulls);
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

/* guessNlsLang
 * If nls_lang is not NULL, return "NLS_LANG=<nls_lang>".
 * Otherwise, return a good guess for DB2's NLS_LANG.
 */
char* guessNlsLang (char *nls_lang) {
  char*          server_encoding = NULL;
  char*          lc_messages     = NULL;
  char*          language        = "AMERICAN_AMERICA";
  char*          charset         = NULL;
  StringInfoData buf;

  db2Entry4("(nls_lang: %s)", nls_lang);
  initStringInfo (&buf);
  if (nls_lang == NULL) {
    server_encoding = db2strdup (GetConfigOption ("server_encoding", false, true),"server_encoding");
    /* find an DB2 client character set that matches the database encoding */
    if (strcmp (server_encoding, "UTF8") == 0)
      charset = "AL32UTF8";
    else if (strcmp (server_encoding, "EUC_JP") == 0)
      charset = "JA16EUC";
    else if (strcmp (server_encoding, "EUC_JIS_2004") == 0)
      charset = "JA16SJIS";
    else if (strcmp (server_encoding, "EUC_TW") == 0)
      charset = "ZHT32EUC";
    else if (strcmp (server_encoding, "ISO_8859_5") == 0)
      charset = "CL8ISO8859P5";
    else if (strcmp (server_encoding, "ISO_8859_6") == 0)
      charset = "AR8ISO8859P6";
    else if (strcmp (server_encoding, "ISO_8859_7") == 0)
      charset = "EL8ISO8859P7";
    else if (strcmp (server_encoding, "ISO_8859_8") == 0)
      charset = "IW8ISO8859P8";
    else if (strcmp (server_encoding, "KOI8R") == 0)
      charset = "CL8KOI8R";
    else if (strcmp (server_encoding, "KOI8U") == 0)
      charset = "CL8KOI8U";
    else if (strcmp (server_encoding, "LATIN1") == 0)
      charset = "WE8ISO8859P1";
    else if (strcmp (server_encoding, "LATIN2") == 0)
      charset = "EE8ISO8859P2";
    else if (strcmp (server_encoding, "LATIN3") == 0)
      charset = "SE8ISO8859P3";
    else if (strcmp (server_encoding, "LATIN4") == 0)
      charset = "NEE8ISO8859P4";
    else if (strcmp (server_encoding, "LATIN5") == 0)
      charset = "WE8ISO8859P9";
    else if (strcmp (server_encoding, "LATIN6") == 0)
      charset = "NE8ISO8859P10";
    else if (strcmp (server_encoding, "LATIN7") == 0)
      charset = "BLT8ISO8859P13";
    else if (strcmp (server_encoding, "LATIN8") == 0)
      charset = "CEL8ISO8859P14";
    else if (strcmp (server_encoding, "LATIN9") == 0)
      charset = "WE8ISO8859P15";
    else if (strcmp (server_encoding, "WIN866") == 0)
      charset = "RU8PC866";
    else if (strcmp (server_encoding, "WIN1250") == 0)
      charset = "EE8MSWIN1250";
    else if (strcmp (server_encoding, "WIN1251") == 0)
      charset = "CL8MSWIN1251";
    else if (strcmp (server_encoding, "WIN1252") == 0)
      charset = "WE8MSWIN1252";
    else if (strcmp (server_encoding, "WIN1253") == 0)
      charset = "EL8MSWIN1253";
    else if (strcmp (server_encoding, "WIN1254") == 0)
      charset = "TR8MSWIN1254";
    else if (strcmp (server_encoding, "WIN1255") == 0)
      charset = "IW8MSWIN1255";
    else if (strcmp (server_encoding, "WIN1256") == 0)
      charset = "AR8MSWIN1256";
    else if (strcmp (server_encoding, "WIN1257") == 0)
      charset = "BLT8MSWIN1257";
    else if (strcmp (server_encoding, "WIN1258") == 0)
      charset = "VN8MSWIN1258";
    else {
      /* warn if we have to resort to 7-bit ASCII */
      charset = "US7ASCII";
      ereport (WARNING,(errcode (ERRCODE_WARNING)
                      ,errmsg ("no DB2 character set for database encoding \"%s\"", server_encoding)
                      ,errdetail ("All but ASCII characters will be lost.")
                      ,errhint ("You can set the option \"%s\" on the foreign data wrapper to force an DB2 character set.", OPT_NLS_LANG)
                      )
              );
    }
    db2free(server_encoding,"server_encoding");
    lc_messages = db2strdup (GetConfigOption ("lc_messages", false, true),"lc_messages");
    /* try to guess those for which there is a backend translation */
    if (strncmp (lc_messages, "de_", 3) == 0 || pg_strncasecmp (lc_messages, "german", 6) == 0)
      language = "GERMAN_GERMANY";
    if (strncmp (lc_messages, "es_", 3) == 0 || pg_strncasecmp (lc_messages, "spanish", 7) == 0)
      language = "SPANISH_SPAIN";
    if (strncmp (lc_messages, "fr_", 3) == 0 || pg_strncasecmp (lc_messages, "french", 6) == 0)
      language = "FRENCH_FRANCE";
    if (strncmp (lc_messages, "in_", 3) == 0 || pg_strncasecmp (lc_messages, "indonesian", 10) == 0)
      language = "INDONESIAN_INDONESIA";
    if (strncmp (lc_messages, "it_", 3) == 0 || pg_strncasecmp (lc_messages, "italian", 7) == 0)
      language = "ITALIAN_ITALY";
    if (strncmp (lc_messages, "ja_", 3) == 0 || pg_strncasecmp (lc_messages, "japanese", 8) == 0)
      language = "JAPANESE_JAPAN";
    if (strncmp (lc_messages, "pt_", 3) == 0 || pg_strncasecmp (lc_messages, "portuguese", 10) == 0)
      language = "BRAZILIAN PORTUGUESE_BRAZIL";
    if (strncmp (lc_messages, "ru_", 3) == 0 || pg_strncasecmp (lc_messages, "russian", 7) == 0)
      language = "RUSSIAN_RUSSIA";
    if (strncmp (lc_messages, "tr_", 3) == 0 || pg_strncasecmp (lc_messages, "turkish", 7) == 0)
      language = "TURKISH_TURKEY";
    if (strncmp (lc_messages, "zh_CN", 5) == 0 || pg_strncasecmp (lc_messages, "chinese-simplified", 18) == 0)
      language = "SIMPLIFIED CHINESE_CHINA";
    if (strncmp (lc_messages, "zh_TW", 5) == 0 || pg_strncasecmp (lc_messages, "chinese-traditional", 19) == 0)
      language = "TRADITIONAL CHINESE_TAIWAN";
    appendStringInfo (&buf, "NLS_LANG=%s.%s", language, charset);
    db2free(lc_messages,"lc_messages");
  } else {
    appendStringInfo (&buf, "NLS_LANG=%s", nls_lang);
  }
  db2Exit4(": %s", buf.data);
  return buf.data;
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
void convertTuple (DB2Session* session, DB2Table* db2Table, DB2ResultColumn* reslist, int natts, Datum* values, bool* nulls) {
  char*                value          = NULL;
  long                 value_len      = 0;
  int                  j              = 0;
  DB2ResultColumn*     res            = NULL;
  bool                 isSimpleSelect = false;

  db2Entry4();
  db2Debug5("natts: %d", natts);

  /* assign result values */
  isSimpleSelect = (db2Table && natts == db2Table->npgcols);
  db2Debug5("isSimpleSelect: %s", isSimpleSelect ? "true": "false");

  // initialize all columns to NULL
  for (j = 0; j < natts; j++) {
    nulls[j]  = true;
    values[j] = PointerGetDatum (NULL);
  }

  for (res = reslist; res; res = res->next) {
    j = ((isSimpleSelect) ? res->pgattnum : res->resnum) - 1;
    db2Debug5("start processing column %d of %d: values index = %d", res->resnum, natts, j);
    db2Debug5("res->pgname   : %s"  ,res->pgname  );
    db2Debug5("res->pgattnum : %d"  ,res->pgattnum);
    db2Debug5("res->pgtype   : %d"  ,res->pgtype  );
    db2Debug5("res->pgtypmod : %d"  ,res->pgtypmod);
    db2Debug5("res->val      : %s"  ,res->val     );
    db2Debug5("res->val_len  : %d"  ,res->val_len );
    db2Debug5("res->val_null : %d"  ,res->val_null);

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
          value_len = (value_len == 0) ? strlen(value) : value_len;
          tmp_value = value;
          if((tmp_value = strchr(value,',')) != NULL) {
            *tmp_value = '.';
          }
        }
        break;
        default: {
          db2Debug5("should be string based values");
          /* for other data types, db2Table contains the results */
          value     = res->val;
          value_len = res->val_len;
          value_len = (value_len == 0) ? strlen(value) : value_len;
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