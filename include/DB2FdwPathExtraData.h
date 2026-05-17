#ifndef DB2FDWPATHEXTRADATA_H
#define DB2FDWPATHEXTRADATA_H

#include <nodes/pathnodes.h>

/** Struct for extra information passed to estimate_path_cost_size()
 * 
 *  @author Thomas Muenz
 *  @since  18.1.1
 */

typedef struct DB2FdwPathExtraData {
  PathTarget* target;
  bool        has_final_sort;
  bool        has_limit;
  double      limit_tuples;
  int64       count_est;
  int64       offset_est;
} DB2FdwPathExtraData;

#endif