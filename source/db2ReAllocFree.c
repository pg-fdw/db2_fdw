#include <postgres.h>
#include "db2_fdw.h"

/*+ external prototypes */

/** local prototypes */

/* db2Alloc
 * Expose palloc0() to DB2 functions.
 */
void* db2Alloc (size_t size,const char* message, ...) {
  void*   memory = palloc0(size);

  if (db2IsLogEnabled(DB2DEBUG5)) {
    char    cBuffer[4000];
    va_list arg_marker;
    va_start(arg_marker, message);
    vsnprintf(cBuffer, sizeof(cBuffer), message, arg_marker);
    db2Debug5("++ %s: %x: %d bytes - %s", cBuffer, memory, size, memory);
    va_end  (arg_marker);
  }
  return memory;
}

/* db2ReAlloc
 * Expose repalloc() to DB2 functions.
 */
void* db2ReAlloc (size_t size, void* p, const char* message, ...) {
  void*   memory = repalloc(p, size);

  if (db2IsLogEnabled(DB2DEBUG5)) {
    char    cBuffer[4000];
    va_list arg_marker;
    va_start(arg_marker, message);
    vsnprintf(cBuffer, sizeof(cBuffer), message, arg_marker);
    db2Debug5("++ %s: %x: %d bytes - %x", cBuffer, memory, size, p);
    va_end  (arg_marker);
  }
  return memory;
}

/* db2Free
 * Expose pfree() to DB2 functions.
 */
void db2Free (void* p, const char* message, ...) {
  if (db2IsLogEnabled(DB2DEBUG5) && p != NULL) {
    char    cBuffer[4000];
    va_list arg_marker;
    va_start(arg_marker, message);
    vsnprintf(cBuffer, sizeof(cBuffer), message, arg_marker);
    db2Debug5("-- %s: %x", cBuffer, p);
    va_end  (arg_marker);
  }
  if (p != NULL) {
    pfree (p);
  }
}

/* db2StrDup
 * Expose pstrdup() to DB2 functions.
 */
char* db2StrDup(const char* source, const char* message, ...) {
  char*   target = NULL;
  if (source != NULL && source[0] != '\0') {
    target = pstrdup(source);
  }
  if (db2IsLogEnabled(DB2DEBUG5) && source != NULL && source[0] != '\0') {
    char    cBuffer[4000];
    va_list arg_marker;
    va_start(arg_marker, message);
    vsnprintf(cBuffer, sizeof(cBuffer), message, arg_marker);
    db2Debug5("++ %s: %x: dup'ed string from %x content source: '%s' target: '%s'",cBuffer, target, source, source, target);
   va_end  (arg_marker);
  }
  return target;
}