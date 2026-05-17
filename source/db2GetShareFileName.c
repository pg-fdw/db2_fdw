#include <postgres.h>
#include <miscadmin.h>
#include <optimizer/optimizer.h>
#include <access/heapam.h>
#include <string.h>
#include "db2_fdw.h"

/** external prototypes */

/** local prototypes */
char* db2GetShareFileName(const char *relativename);

/* db2GetShareFileName
 * Returns the allocated absolute path of a file in the "share" directory.
 */
char* db2GetShareFileName (const char *relativename) {
  char  share_path[MAXPGPATH];
  char* result = NULL;

  db2Entry1();
  get_share_path(my_exec_path, share_path);
  result = db2alloc(MAXPGPATH,"result[%d]",MAXPGPATH);
  snprintf(result, MAXPGPATH, "%s/%s", share_path, relativename);
  db2Exit1(": %s",result);
  return result;
}
