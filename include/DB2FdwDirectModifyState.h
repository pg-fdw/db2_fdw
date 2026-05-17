#ifndef DB2FDWDIRECTMODIFYSTATE_H
#define DB2FDWDIRECTMODIFYSTATE_H

#include <foreign/foreign.h>
#include <foreign/fdwapi.h>
#include <funcapi.h>
#include "ParamDesc.h"
#include "DB2ResultColumn.h"

// Execution state of a foreign scan that modifies a foreign table directly. 
typedef struct db2FdwDirectModifyState {
  // DB2 Section
  char*               dbserver;               // DB2 connect string
  char*               user;                   // DB2 username
  char*               password;               // DB2 password
  char*               jwt_token;              // JWT token for authentication (alternative to user/password)
  char*               nls_lang;               // DB2 locale information
  DB2Session*         session;                // encapsulates the active DB2 session
  ParamDesc*          paramList;              // description of parameters needed for the query
  DB2ResultColumn*    resultList;             // list of result columns for the query
  DB2Table*           db2Table;               // description of the remote DB2 table
  unsigned long       prefetch;               // number of rows to prefetch (SQL_ATTR_PREFETCH_NROWS 0-1024)
  int                 fetch_size;             // fetch size for this remote table (SQL_ATTR_ROW_ARRAY_SIZE 1 - 32767)
  Relation	          rel;		  	            // relcache entry for the foreign table 
  AttInMetadata*      attinmeta;      	      // attribute datatype conversion metadata 
  // extracted fdw_private data 
  char*               query;			            // text of UPDATE/DELETE command 
  bool		            has_returning;	        // is there a RETURNING clause? 
  List*               retrieved_attrs;	      // attr numbers retrieved by RETURNING 
  bool		            set_processed;	        // do we set the command es_processed? 
  // for remote query execution 
  int			            numParams;		          // number of parameters passed to query 
  FmgrInfo*           param_flinfo;	          // output conversion functions for them 
  List*               param_exprs;	          // executable expressions for param values 
  const char**        param_values;	          // textual values of query parameters 
  // for storing result tuples 
  //  PGresult*       result;			              // result for query 
  int			            num_tuples;		          // # of result tuples 
  int			            next_tuple;		          // index of next one to return 
  Relation            resultRel;		          // relcache entry for the target relation 
  AttrNumber*         attnoMap;		            // array of attnums of input user columns 
  AttrNumber	        ctidAttno;		          // attnum of input ctid column 
  AttrNumber	        oidAttno;		            // attnum of input oid column 
  bool		            hasSystemCols;	        // are there system columns of resultRel? 
  // working memory context 
  MemoryContext       temp_cxt;		            // context for per-tuple temporary data 
} DB2FdwDirectModifyState;

/* Similarly, this enum describes what's kept in the fdw_private list for
 * a ForeignScan node that modifies a foreign table directly.  We store:
 *
 * 1) UPDATE/DELETE statement text to be sent to the remote server
 * 2) Boolean flag showing if the remote query has a RETURNING clause
 * 3) Integer list of attribute numbers retrieved by RETURNING, if any
 * 4) Boolean flag showing if we set the command es_processed
 */
enum FdwDirectModifyPrivateIndex {
  // SQL statement to execute remotely (as a String node) 
  FdwDirectModifyPrivateUpdateSql,
  // has-returning flag (as a Boolean node) 
  FdwDirectModifyPrivateHasReturning,
  // Integer list of attribute numbers retrieved by RETURNING 
  FdwDirectModifyPrivateRetrievedAttrs,
  // set-processed flag (as a Boolean node) 
  FdwDirectModifyPrivateSetProcessed,
};

#endif