#ifndef DB2FDWSTATE_H
#define DB2FDWSTATE_H

#include <foreign/foreign.h>
#include <nodes/pathnodes.h>
#include "ParamDesc.h"
#include "DB2ResultColumn.h"

/** DB2FdwState
 *  FDW-specific information for RelOptInfo.fdw_private and ForeignScanState.fdw_state.
 *  The same structure is used to hold information for query planning and execution.
 *  The structure is initialized during query planning and passed on to the execution
 *  step serialized as a List (see serializePlanData and deserializePlanData).
 *  For DML statements, the scan stage and the modify stage both hold an
 *  DB2FdwState, and the latter is initialized by copying the former (see copyPlanData).
 * 
 *  FDW-specific planner information kept in RelOptInfo.fdw_private for a postgres_fdw foreign table.
 *  For a baserel, this struct is created by postgresGetForeignRelSize, although some fields are not filled till later.
 *  postgresGetForeignJoinPaths creates it for a joinrel, and postgresGetForeignUpperPaths creates it for an upperrel.
 *
 *  @author Ing Wolfgang Brandl
 *  @since  1.0
 */
typedef struct db2FdwState {
  // DB2 Section
  char*               dbserver;               // DB2 connect string
  char*               user;                   // DB2 username
  char*               password;               // DB2 password
  char*               jwt_token;              // JWT token for authentication (alternative to user/password)
  char*               nls_lang;               // DB2 locale information
  DB2Session*         session;                // encapsulates the active DB2 session
  char*               query;                  // query we issue against DB2
  List*               params;                 // list of parameters needed for the query
  ParamDesc*          paramList;              // description of parameters needed for the query
  DB2ResultColumn*    resultList;             // list of result columns for the query
  DB2Table*           db2Table;               // description of the remote DB2 table
  unsigned long       rowcount;               // rows already read from DB2
  MemoryContext       temp_cxt;               // short-lived memory for data modification
  unsigned long       prefetch;               // number of rows to prefetch (SQL_ATTR_PREFETCH_NROWS 0-1024)
  char*               order_clause;           // for sort-pushdown
  char*               where_clause;           // deparsed where clause
  // attributes taken over from PgFdwRelationInfo
  bool                pushdown_safe;          // True: relation can be pushed down. Always true for simple foreign scan.
  // All entries in these lists should have RestrictInfo wrappers; that improves efficiency of selectivity and cost estimation.
  List*               retrieved_attr;         // Retrieved Attributes List.
  List*               remote_conds;           // Restriction clauses, safe to pushdown subsets.
  List*               local_conds;            // Restriction clauses, unsafe to pushdown subsets.
  List*               final_remote_exprs;     // Actual remote restriction clauses for scan (sans RestrictInfos)
  Bitmapset*          attrs_used;             // Bitmap of attr numbers we need to fetch from the remote server.
  bool                qp_is_pushdown_safe;    // True means that the query_pathkeys is safe to push down
  QualCost            local_conds_cost;       // Cost of local_conds.
  Selectivity         local_conds_sel;        // Selectivity of local_conds.
  Selectivity         joinclause_sel;         // Selectivity of join conditions
  double              rows;                   // Estimated size and cost for a scan, join, or grouping/aggregation.
  int                 width;                  // Estimated size and cost for a scan, join, or grouping/aggregation.
  int                 disabled_nodes;         // Estimated size and cost for a scan, join, or grouping/aggregation.
  Cost                startup_cost;           // Estimated size and cost for a scan, join, or grouping/aggregation.
  Cost                total_cost;             // Estimated size and cost for a scan, join, or grouping/aggregation.
// These are only used by estimate_path_cost_size().
  double              retrieved_rows;         // Estimated number of rows fetched from the foreign server
  Cost                rel_startup_cost;       // Estimated costs excluding costs for transferring those rows from the foreign server.
  Cost                rel_total_cost;         // Estimated costs excluding costs for transferring those rows from the foreign server.
// Options extracted from catalogs.
  bool                use_remote_estimate;
  Cost                fdw_startup_cost;
  Cost                fdw_tuple_cost;
  List*               shippable_extensions;   // OIDs of shippable extensions
  bool                async_capable;
  ForeignTable*       ftable;                 // Cached catalog foreign table information
  ForeignServer*      fserver;                // Cached catalog foreign server information
  UserMapping*        fuser;                  // only set in use_remote_estimate mode
  int                 fetch_size;             // fetch size for this remote table (SQL_ATTR_ROW_ARRAY_SIZE 1 - 32767)
  /* Name of the relation, for use while EXPLAINing ForeignScan.  It is used for join and upper relations but is set for all relations.
   * For a base relation, this is really just the RT index as a string; we convert that while producing EXPLAIN output.
   * For join and upper relations, the name indicates which base foreign tables are included and the join type or aggregation type used.
   */
  char*               relation_name;
  RelOptInfo*         outerrel;               // Join information
  RelOptInfo*         innerrel;
  JoinType            jointype;
  List*               joinclauses;            // joinclauses contains only JOIN/ON conditions for an outer join 
  UpperRelationKind   stage;                  // Upper relation information
  List*               grouped_tlist;          // Grouping information
// Subquery information
  bool                make_outerrel_subquery; // do we deparse outerrel as a subquery?
  bool                make_innerrel_subquery; // do we deparse innerrel as a subquery?
  Relids              lower_subquery_rels;    // all relids appearing in lower subqueries
  Relids              hidden_subquery_rels;   // unreferrabl relids from upper relations, used internally for equivalence member search
  int                 relation_index;         // Index of the relation.  It is used to create an alias to a subquery representing the relation.
} DB2FdwState;
#endif