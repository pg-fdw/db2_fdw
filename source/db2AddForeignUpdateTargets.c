#include <postgres.h>
#include <nodes/makefuncs.h>
#include <optimizer/appendinfo.h>
#include <optimizer/optimizer.h>
#include <access/heapam.h>
#include <utils/lsyscache.h>
#include "db2_fdw.h"

/** external prototypes */
extern bool         optionIsTrue              (const char* value);

/** local prototypes */
void db2AddForeignUpdateTargets(PlannerInfo* root, Index rtindex, RangeTblEntry* target_rte, Relation target_relation);

/* db2AddForeignUpdateTargets
 * Add the primary key columns as resjunk entries.
 */
void db2AddForeignUpdateTargets (PlannerInfo* root, Index rtindex,RangeTblEntry* target_rte, Relation target_relation){
  Oid       relid   = RelationGetRelid (target_relation);
  TupleDesc tupdesc = target_relation->rd_att;
  int       i       = 0;
  bool      has_key = false;
  db2Entry1();
  db2Debug2("add target columns for update on %d - %s", relid, get_rel_name(relid));
  /* loop through all columns of the foreign table */
  for (i = 0; i < tupdesc->natts; ++i) {
    Form_pg_attribute att     = TupleDescAttr (tupdesc, i);
    AttrNumber        attrno  = att->attnum;
    List*             options = NIL;
    ListCell*         option  = NULL;

    if (att->attisdropped)
      continue;

    /* look for the "key" option on this column */
    options = GetForeignColumnOptions (relid, attrno);
    foreach (option, options) {
      DefElem* def = (DefElem*) lfirst (option);
      /* if "key" is set, add a resjunk for this column */
      if (strcmp (def->defname, OPT_KEY) == 0) {
        if (optionIsTrue (STRVAL(def->arg))) {
          Var*  var           = NULL;

          /* Build a Var referencing the PK column of the target RTE */
          var = makeVar( rtindex
                       , attrno
                       , att->atttypid
                       , att->atttypmod
                       , att->attcollation
                       , 0
                       );
          db2Debug2("create var rtindex: %d, attrno: %d, typid: %d, typmod: %d, collation: %d",rtindex,attrno,att->atttypid,att->atttypmod,att->attcollation);
          /* Register it as a required row-identity column. The name becomes the resjunk column name in the plan. */
          add_row_identity_var(root, var, rtindex, NameStr(att->attname));
          db2Debug2("add resjunk column %s: %d", NameStr(att->attname), rtindex);
          has_key = true;
        }
      }
    }
  }
  if (!has_key) {
    ereport ( ERROR
            , ( errcode   (ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION)
              , errmsg    ("no primary key column specified for foreign DB2 table")
              , errdetail ("For UPDATE or DELETE, at least one foreign table column must be marked as primary key column.")
              , errhint   ("Set the option \"%s\" on the columns that belong to the primary key.", OPT_KEY)
              )
            );
  }
  db2Exit1();
}
