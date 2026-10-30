--
-- db2_fdw sync – DB2-Seite (Beispiel: DB2INST1.EMPLOYEE aus der SAMPLE-Datenbank)
--
-- Wird normalerweise von db2_fdw_sync_db2_ddl('employee') erzeugt.
-- Ausführen mit:  db2 -td@ -f db2_side_employee.sql
--
-- Prinzip: Das Journal enthält nur den Primärschlüssel und die Operation.
-- Den aktuellen Zeileninhalt liest der Sync später direkt aus der Quelltabelle.
-- Dadurch bleibt das Journal schmal, und mehrfache Änderungen derselben Zeile
-- kollabieren beim Sync automatisch auf den letzten Stand.
--

CREATE SCHEMA DB2FDW@

-- 1. Journaltabelle: PK-Spalten mit exakt denselben Typen wie in der Quelle
CREATE TABLE DB2FDW.J_EMPLOYEE AS (SELECT EMPNO FROM DB2INST1.EMPLOYEE) WITH NO DATA@

ALTER TABLE DB2FDW.J_EMPLOYEE
  ADD COLUMN SEQ    BIGINT    NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 CACHE 100)
  ADD COLUMN OP     CHAR(1)   NOT NULL
  ADD COLUMN CHG_TS TIMESTAMP NOT NULL DEFAULT CURRENT TIMESTAMP@

ALTER TABLE DB2FDW.J_EMPLOYEE ADD PRIMARY KEY (SEQ)@

-- 2. Statement-Trigger mit Transition Tables:
--    ein INSERT ... SELECT pro DML-Statement statt eines Inserts pro Zeile.
CREATE OR REPLACE TRIGGER DB2FDW.T_EMPLOYEE_I
  AFTER INSERT ON DB2INST1.EMPLOYEE
  REFERENCING NEW TABLE AS NT
  FOR EACH STATEMENT
  INSERT INTO DB2FDW.J_EMPLOYEE (OP, EMPNO) SELECT 'I', EMPNO FROM NT@

CREATE OR REPLACE TRIGGER DB2FDW.T_EMPLOYEE_U
  AFTER UPDATE ON DB2INST1.EMPLOYEE
  REFERENCING OLD TABLE AS OT NEW TABLE AS NT
  FOR EACH STATEMENT
BEGIN ATOMIC
  INSERT INTO DB2FDW.J_EMPLOYEE (OP, EMPNO) SELECT 'U', EMPNO FROM NT;
  -- PK-Änderung: alter Schlüssel verschwindet -> als Delete protokollieren
  INSERT INTO DB2FDW.J_EMPLOYEE (OP, EMPNO)
    SELECT 'D', O.EMPNO FROM OT O
    WHERE NOT EXISTS (SELECT 1 FROM NT N WHERE N.EMPNO = O.EMPNO);
END@

CREATE OR REPLACE TRIGGER DB2FDW.T_EMPLOYEE_D
  AFTER DELETE ON DB2INST1.EMPLOYEE
  REFERENCING OLD TABLE AS OT
  FOR EACH STATEMENT
  INSERT INTO DB2FDW.J_EMPLOYEE (OP, EMPNO) SELECT 'D', EMPNO FROM OT@

-- 3. Rechte für den Benutzer aus dem USER MAPPING
--    (SELECT auf Quelle + Journal, DELETE auf Journal zum Quittieren)
GRANT SELECT, DELETE ON DB2FDW.J_EMPLOYEE TO USER PGSYNC@

-- Hinweis Isolation:
--   Der Sync darf keine unkommittierten Journalzeilen sehen. Mit
--   CUR_COMMIT = ON (Default bei neuen Datenbanken ab 9.7) liefert CS nur
--   kommittierte Zeilen, ohne auf Sperren zu warten. Prüfen mit:
--     db2 get db cfg for SAMPLE | grep -i CUR_COMMIT
--
-- Hinweis TRUNCATE / LOAD:
--   Beide lösen keine Trigger aus. Danach ist ein Full Resync nötig
--   (CALL db2_fdw_sync_initial_load(...) auf PostgreSQL-Seite).
