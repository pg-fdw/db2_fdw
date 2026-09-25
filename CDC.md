AUDIT LOG Muss zuerst konfiguriert und aktiviert werden!


To query your IBM Db2 audit logs dynamically via SELECT statements, you need to set up the official IBM target tables and automate the flow from archiving to extracting and finally loading the data using an ADMIN_CMD load routine. [1] 
Below is the structured, end-to-end pipeline to accomplish this.
------------------------------
## Step 1: Create the Standard Db2 Audit Tables
IBM provides a pre-packaged DDL script containing the precise schema definitions required for audit log extraction. [2] 
Run this command from your database server's terminal (ensure you are connected to the database and have an 8K tablespace available): [2] 

db2 +o -tf $DB2INSTANCE/sqllib/misc/db2audit.ddl

This creates separate tracking tables in the default schema (or a specified one) corresponding to the audit categories: EXECUTE, CONTEXT, CHECKING, OBJMAINT, SECMAINT, SYSADMIN, VALIDATE, and AUDIT. [2, 3] 
------------------------------
## Step 2: The Core SQL Extraction Routine
Because SYSPROC.AUDIT_DELIM_EXTRACT writes raw .del files directly to the file system, you cannot SELECT from them until they are ingested into the tables created in Step 1. [1, 4] 
The following automated compound SQL block loops through your archived logs, extracts them, and loads them directly into the database tables. [1, 5] 

    BEGIN
      DECLARE v_archive_dir VARCHAR(1024) DEFAULT '/home/db2inst1/audit_archive';
      DECLARE v_extract_dir VARCHAR(1024) DEFAULT '/home/db2inst1/audit_extract';
      DECLARE v_load_cmd    VARCHAR(2048);

      -- 1. Archive the active log file (Generates a timestamped binary file)
      CALL SYSPROC.AUDIT_ARCHIVE(v_archive_dir, -2);

      -- 2. Process each archived file found in the directory
      FOR log_row AS 
        SELECT FILENAME FROM TABLE(SYSPROC.AUDIT_LIST_LOGS(v_archive_dir, -2)) AS T
      DO
        -- 3. Extract binary logs into flat .del files inside the extract directory
        CALL SYSPROC.AUDIT_DELIM_EXTRACT('"', v_extract_dir, v_archive_dir, log_row.FILENAME, NULL);
        
        -- 4. Dynamically load the extracted category logs into your target tables.
        -- (Repeat this block or loop through for other active categories like CONTEXT, CHECKING, etc.)
        SET v_load_cmd = 'LOAD FROM ' || v_extract_dir || '/execute.del OF DEL INSERT INTO EXECUTE';
        CALL SYSPROC.ADMIN_CMD(v_load_cmd);
        
        SET v_load_cmd = 'LOAD FROM ' || v_extract_dir || '/objmaint.del OF DEL INSERT INTO OBJMAINT';
        CALL SYSPROC.ADMIN_CMD(v_load_cmd);

      END FOR;END @

## Parameter Breakdown for Stored Procedures:

   1. SYSPROC.AUDIT_ARCHIVE(directory, member)
   * directory: Path where binary logs are saved.
      * -2: Directs the routine to aggregate logs across all database members. [6] 
   2. SYSPROC.AUDIT_DELIM_EXTRACT(delimiter, target_dir, source_dir, filename, category)
   * '"' : Standard double-quote text delimiter.
      * category: Set to NULL to extract all captured audit event types at once. [7, 8] 
   
------------------------------
## Step 3: Run Your Queries
Once the data is populated via the routine above, you can execute standard SQL queries directly against your audit tables: [9] 

-- Query failed authorization attemptsSELECT TIMESTAMP, USERID, OBJECTSCHEMA, OBJECTNAME FROM CHECKING WHERE STATUS = -551; -- Db2 Authorization failure code
-- Review executed SQL statement textsSELECT TIMESTAMP, USERID, STMT_TEXT FROM EXECUTE ORDER BY TIMESTAMP DESC;

To refine this automation, let me know:

* 
* Would you like this packaged into a permanent Stored Procedure that you can trigger via a cron job or administrative task scheduler?
* Which specific audit categories (e.g., EXECUTE, CONTEXT, SECMAINT) are you primarily focusing on tracking?
* 

[1] [https://www.ibm.com](https://www.ibm.com/docs/en/db2/11.1.0?topic=facility-storage-analysis-audit-logs)
[2] [https://www.ibm.com](https://www.ibm.com/docs/en/db2/11.5.x?topic=logs-creating-tables-db2-audit-data)
[3] [https://www.ibm.com](https://www.ibm.com/docs/en/db2/11.5.x?topic=logs-creating-tables-db2-audit-data)
[4] [https://www.ibm.com](https://www.ibm.com/docs/en/db2/12.1.x?topic=logs-audit-archive-extract-stored-procedures)
[5] [https://www.ibm.com](https://www.ibm.com/docs/en/db2/11.1.0?topic=facility-storage-analysis-audit-logs)
[6] [https://www.ibm.com](https://www.ibm.com/docs/en/db2/11.1.0?topic=procedures-audit-archive-archive-audit-log-file)
[7] [https://www.ibm.com](https://www.ibm.com/docs/en/db2-big-sql/7.1.0?topic=auditing-loading-analyzing-log-data-from-database-tables)
[8] [https://www.ibm.com](https://www.ibm.com/docs/en/db2/11.1.0?topic=procedures-audit-delim-extract-performs-extract-delimited-file)
[9] [https://www.ibm.com](https://www.ibm.com/docs/hu/SSEPGG_11.1.0/com.ibm.db2.luw.apdv.sample.doc/doc/admin_scripts/s-audit_UNIX-db2.html?view=kc)

------------------------------
## Step 4: Manual Test in local test environment

    db2audit configure datapath /home/db2inst1/sqllib/security/auditdata archivepath /home/db2inst1/audit_archive
    db2 connect to sample
    db2 "CREATE AUDIT POLICY LOGALL CATEGORIES ALL STATUS BOTH ERROR TYPE AUDIT"
    db2 "AUDIT DATABASE USING POLICY LOGALL"
    db2 "CALL SYSPROC.AUDIT_DELIM_EXTRACT(NULL, NULL, NULL, NULL, NULL)"
    db2 "CALL SYSPROC.ADMIN_CMD('LOAD FROM /home/db2inst1/audit_extract/audit.del OF DEL MODIFIED BY DELPRIORITYCHAR LOBSINFILE INSERT INTO DB2INST1.AUDIT');"
    db2 "CALL SYSPROC.ADMIN_CMD('LOAD FROM /home/db2inst1/audit_extract/checking.del OF DEL MODIFIED BY DELPRIORITYCHAR LOBSINFILE INSERT INTO DB2INST1.CHECKING');"
    db2 "CALL SYSPROC.ADMIN_CMD('LOAD FROM /home/db2inst1/audit_extract/context.del OF DEL MODIFIED BY DELPRIORITYCHAR LOBSINFILE INSERT INTO DB2INST1.CONTEXT');"
    db2 "CALL SYSPROC.ADMIN_CMD('LOAD FROM /home/db2inst1/audit_extract/execute.del OF DEL MODIFIED BY DELPRIORITYCHAR LOBSINFILE INSERT INTO DB2INST1.EXECUTE');"
    db2 "CALL SYSPROC.ADMIN_CMD('LOAD FROM /home/db2inst1/audit_extract/secmaint.del OF DEL MODIFIED BY DELPRIORITYCHAR LOBSINFILE INSERT INTO DB2INST1.SECMAINT');"
    db2 "CALL SYSPROC.ADMIN_CMD('LOAD FROM /home/db2inst1/audit_extract/objmaint.del OF DEL MODIFIED BY DELPRIORITYCHAR LOBSINFILE INSERT INTO DB2INST1.OBJMAINT');"
    db2 "CALL SYSPROC.ADMIN_CMD('LOAD FROM /home/db2inst1/audit_extract/validate.del OF DEL MODIFIED BY DELPRIORITYCHAR LOBSINFILE INSERT INTO DB2INST1.VALIDATE');"
    db2 "AUDIT DATABASE REMOVE POLICY"
    db2 "DROP AUDIT POLICY LOGALL"
    db2audit stop
    db2audit configure reset