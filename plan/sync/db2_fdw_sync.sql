/*-------------------------------------------------------------------------
 * db2_fdw_sync.sql – SKIZZE
 *
 * Inkrementelle Synchronisation einer DB2-Tabelle in eine lokale
 * PostgreSQL-Spiegeltabelle über ein trigger-basiertes Journal in DB2.
 *
 * Gedacht als Ergänzung zu sql/db2_fdw--18.3.0.sql (bzw. als Upgrade-Skript
 * db2_fdw--18.3.0--18.4.0.sql). Reines SQL/PL/pgSQL, kein neuer C-Code nötig:
 * Alle Zugriffe auf DB2 laufen über die vorhandenen Foreign-Table-Pfade
 * (Join-, ORDER BY/LIMIT- und Direct-Modify-Pushdown).
 *
 * Ablauf eines Batches (at-least-once + idempotentes Anwenden):
 *   1. Journal-Batch lesen          (DB2: nur kommittierte Zeilen sichtbar)
 *   2. aktuellen Zustand holen      (DB2: Join Quelle x Journal, gepusht)
 *   3. Upsert in die Spiegeltabelle (lokal)
 *   4. Deletes in der Spiegeltabelle(lokal)
 *   5. verarbeitete SEQs merken + COMMIT
 *   6. SEQs im DB2-Journal löschen  (eigene Transaktion, wiederholbar)
 *-------------------------------------------------------------------------
 */

/* ---------- Katalog ---------------------------------------------------- */

CREATE TABLE db2_fdw_sync_config (
  mirror         regclass    PRIMARY KEY,           -- lokale Spiegeltabelle
  source         regclass    NOT NULL,              -- Foreign Table (Daten)
  journal        regclass    NOT NULL,              -- Foreign Table (DB2-Journal)
  pk_cols        name[]      NOT NULL,              -- Spalten mit Option key 'true'
  batch_size     integer     NOT NULL DEFAULT 10000 CHECK (batch_size > 0),
  last_sync      timestamptz,
  last_seq       bigint,
  rows_upserted  bigint      NOT NULL DEFAULT 0,
  rows_deleted   bigint      NOT NULL DEFAULT 0
);

-- lokal angewandte, in DB2 noch nicht quittierte Journal-Einträge
CREATE TABLE db2_fdw_sync_pending (
  mirror  regclass NOT NULL REFERENCES db2_fdw_sync_config ON DELETE CASCADE,
  seq     bigint   NOT NULL,
  PRIMARY KEY (mirror, seq)
);

SELECT pg_catalog.pg_extension_config_dump('db2_fdw_sync_config' , '');
SELECT pg_catalog.pg_extension_config_dump('db2_fdw_sync_pending', '');

/* ---------- Hilfsfunktionen -------------------------------------------- */

-- DB2-Bezeichner immer quoten (Groß-/Kleinschreibung bleibt erhalten)
CREATE FUNCTION db2_fdw_quote(text) RETURNS text
LANGUAGE sql IMMUTABLE STRICT AS
$$ SELECT '"' || replace($1, '"', '""') || '"' $$;

-- PG-Spaltenname -> DB2-Spaltenname (entspricht IMPORT-Option case 'smart')
CREATE FUNCTION db2_fdw_remote_name(name) RETURNS text
LANGUAGE sql IMMUTABLE STRICT AS
$$ SELECT CASE WHEN $1 = lower($1) THEN upper($1) ELSE $1 END $$;

-- Schlüsselspalten einer Foreign Table (Spaltenoption key = true/yes/on)
CREATE FUNCTION db2_fdw_key_columns(ft regclass) RETURNS name[]
LANGUAGE sql STABLE AS
$$
  SELECT array_agg(a.attname ORDER BY a.attnum)
  FROM   pg_attribute a
  WHERE  a.attrelid = ft AND a.attnum > 0 AND NOT a.attisdropped
    AND  EXISTS (SELECT 1
                 FROM   pg_options_to_table(a.attfdwoptions) o
                 WHERE  o.option_name = 'key'
                   AND  lower(o.option_value) IN ('true', 'yes', 'on', '1'))
$$;

/* ---------- DB2-DDL erzeugen ------------------------------------------- */

CREATE FUNCTION db2_fdw_sync_db2_ddl(
    source          regclass,
    journal_schema  text DEFAULT 'DB2FDW',
    grantee         text DEFAULT NULL)
RETURNS text
LANGUAGE plpgsql STABLE AS
$$
DECLARE
  pk       name[] := db2_fdw_key_columns(source);
  rschema  text;
  rtable   text;
  src      text;
  jt       text;
  trg      text;
  k_list   text;   -- "A", "B"
  o_list   text;   -- O."A", O."B"
  no_match text;   -- N."A" = O."A" AND ...
BEGIN
  IF pk IS NULL THEN
    RAISE EXCEPTION 'foreign table % has no column with option key ''true''', source
      USING HINT = 'ALTER FOREIGN TABLE ... ALTER COLUMN ... OPTIONS (ADD key ''true'')';
  END IF;

  SELECT max(o.option_value) FILTER (WHERE o.option_name = 'schema'),
         max(o.option_value) FILTER (WHERE o.option_name = 'table')
    INTO rschema, rtable
  FROM   pg_foreign_table ft, pg_options_to_table(ft.ftoptions) o
  WHERE  ft.ftrelid = source;

  src := coalesce(db2_fdw_quote(rschema) || '.', '') || db2_fdw_quote(rtable);
  jt  := db2_fdw_quote(journal_schema) || '.' || db2_fdw_quote('J_' || rtable);
  trg := replace(journal_schema, '"', '""') || '"."T_' || replace(rtable, '"', '""');  -- Rest der Quotes im Template

  SELECT string_agg(db2_fdw_quote(r), ', '),
         string_agg('O.' || db2_fdw_quote(r), ', '),
         string_agg(format('N.%1$s = O.%1$s', db2_fdw_quote(r)), ' AND ')
    INTO k_list, o_list, no_match
  FROM (SELECT db2_fdw_remote_name(k) AS r FROM unnest(pk) k) x;

  RETURN format($ddl$
-- generiert von db2_fdw_sync_db2_ddl(%1$s) – ausführen mit: db2 -td@ -f <datei>
-- einmalig:  CREATE SCHEMA %2$s@

CREATE TABLE %3$s AS (SELECT %4$s FROM %5$s) WITH NO DATA@

ALTER TABLE %3$s
  ADD COLUMN SEQ    BIGINT    NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 CACHE 100)
  ADD COLUMN OP     CHAR(1)   NOT NULL
  ADD COLUMN CHG_TS TIMESTAMP NOT NULL DEFAULT CURRENT TIMESTAMP@

ALTER TABLE %3$s ADD PRIMARY KEY (SEQ)@

CREATE OR REPLACE TRIGGER "%6$s_I"
  AFTER INSERT ON %5$s REFERENCING NEW TABLE AS NT FOR EACH STATEMENT
  INSERT INTO %3$s (OP, %4$s) SELECT 'I', %4$s FROM NT@

CREATE OR REPLACE TRIGGER "%6$s_U"
  AFTER UPDATE ON %5$s REFERENCING OLD TABLE AS OT NEW TABLE AS NT FOR EACH STATEMENT
BEGIN ATOMIC
  INSERT INTO %3$s (OP, %4$s) SELECT 'U', %4$s FROM NT;
  INSERT INTO %3$s (OP, %4$s) SELECT 'D', %7$s FROM OT O
    WHERE NOT EXISTS (SELECT 1 FROM NT N WHERE %8$s);
END@

CREATE OR REPLACE TRIGGER "%6$s_D"
  AFTER DELETE ON %5$s REFERENCING OLD TABLE AS OT FOR EACH STATEMENT
  INSERT INTO %3$s (OP, %4$s) SELECT 'D', %4$s FROM OT@
%9$s$ddl$,
    source, db2_fdw_quote(journal_schema), jt, k_list, src, trg, o_list, no_match,
    CASE WHEN grantee IS NULL THEN ''
         ELSE format(E'\nGRANT SELECT, DELETE ON %s TO USER %s@\n', jt, db2_fdw_quote(grantee)) END);
END
$$;

/* ---------- Einrichtung ------------------------------------------------ */

-- journal: per IMPORT FOREIGN SCHEMA angelegte Foreign Table auf DB2FDW.J_<TAB>
CREATE FUNCTION db2_fdw_sync_setup(
    source      regclass,
    journal     regclass,
    mirror      text    DEFAULT NULL,       -- Default: <source>_mirror im selben Schema
    batch_size  integer DEFAULT 10000)
RETURNS regclass
LANGUAGE plpgsql AS
$$
DECLARE
  pk       name[] := db2_fdw_key_columns(source);
  nsp      name;
  rel      name;
  m        regclass;
BEGIN
  IF pk IS NULL THEN
    RAISE EXCEPTION 'foreign table % has no column with option key ''true''', source;
  END IF;

  SELECT n.nspname, c.relname INTO nsp, rel
  FROM   pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE  c.oid = source;

  mirror := coalesce(mirror, format('%I.%I', nsp, rel || '_mirror'));

  EXECUTE format('CREATE TABLE %s (LIKE %s INCLUDING DEFAULTS)', mirror, source);
  m := mirror::regclass;
  EXECUTE format('ALTER TABLE %s ADD PRIMARY KEY (%s)', m,
                 (SELECT string_agg(quote_ident(k), ', ') FROM unnest(pk) k));

  INSERT INTO db2_fdw_sync_config (mirror, source, journal, pk_cols, batch_size)
  VALUES (m, source, journal, pk, batch_size);

  RETURN m;
END
$$;

-- Vollabgleich (Erstbefüllung, nach TRUNCATE/LOAD in DB2, nach Störungen).
-- Die Trigger in DB2 müssen VORHER aktiv sein; ältere Journal-Einträge
-- werden beim nächsten Sync einfach noch einmal (idempotent) angewandt.
CREATE PROCEDURE db2_fdw_sync_initial_load(p_mirror regclass)
LANGUAGE plpgsql AS
$$
DECLARE
  c     db2_fdw_sync_config;
  cols  text;
  n     bigint;
BEGIN
  SELECT * INTO STRICT c FROM db2_fdw_sync_config WHERE mirror = p_mirror;

  SELECT string_agg(quote_ident(attname), ', ' ORDER BY attnum) INTO cols
  FROM   pg_attribute WHERE attrelid = c.mirror AND attnum > 0 AND NOT attisdropped;

  EXECUTE format('TRUNCATE %s', c.mirror);
  EXECUTE format('INSERT INTO %s (%s) SELECT %s FROM %s', c.mirror, cols, cols, c.source);
  GET DIAGNOSTICS n = ROW_COUNT;

  UPDATE db2_fdw_sync_config
     SET last_sync = now(), rows_upserted = rows_upserted + n
   WHERE mirror = p_mirror;
END
$$;

/* ---------- Quittieren (Journal in DB2 aufräumen) ----------------------- */

CREATE PROCEDURE db2_fdw_sync_ack(p_mirror regclass, p_chunk integer DEFAULT 1000)
LANGUAGE plpgsql AS
$$
DECLARE
  c     db2_fdw_sync_config;
  seqs  bigint[];
BEGIN
  SELECT * INTO STRICT c FROM db2_fdw_sync_config WHERE mirror = p_mirror;
  LOOP
    SELECT array_agg(seq) INTO seqs
    FROM  (SELECT seq FROM db2_fdw_sync_pending
           WHERE mirror = p_mirror ORDER BY seq LIMIT p_chunk) s;
    EXIT WHEN seqs IS NULL;

    -- Direct Modify: DELETE FROM DB2FDW.J_x WHERE SEQ IN (...) läuft in DB2
    EXECUTE format('DELETE FROM %s WHERE seq = ANY ($1)', c.journal) USING seqs;
    DELETE FROM db2_fdw_sync_pending WHERE mirror = p_mirror AND seq = ANY (seqs);
    -- db2_fdw committet DB2 in PRE_COMMIT, danach PostgreSQL. Scheitert der
    -- lokale Commit, bleiben die SEQs pending -> erneutes DELETE ist ein No-op.
    COMMIT;
  END LOOP;
END
$$;

/* ---------- Synchronisation -------------------------------------------- */

-- Aufruf nur auf oberster Ebene (CALL, nicht in BEGIN ... COMMIT), z. B. via pg_cron.
-- Achtung: kein "SET search_path" an der Prozedur – das verbietet COMMIT.
CREATE PROCEDURE db2_fdw_sync(p_mirror regclass, p_max_batches integer DEFAULT 100)
LANGUAGE plpgsql AS
$$
DECLARE
  c         db2_fdw_sync_config;
  pk        text;   -- "a", "b"
  pk_j      text;   -- j."a", j."b"
  pk_s      text;   -- s."a", s."b"
  on_sj     text;   -- s."a" = j."a" AND ...
  on_mb     text;   -- m."a" = b."a" AND ...
  on_xb     text;   -- x."a" = b."a" AND ...
  cols      text;
  upd       text;
  n_batch   bigint;
  n_up      bigint;
  n_del     bigint;
  lo        bigint;
  hi        bigint;
  i         integer := 0;
BEGIN
  SELECT * INTO STRICT c FROM db2_fdw_sync_config WHERE mirror = p_mirror;

  SELECT string_agg(quote_ident(k), ', '),
         string_agg('j.' || quote_ident(k), ', '),
         string_agg('s.' || quote_ident(k), ', '),
         string_agg(format('s.%1$I = j.%1$I', k), ' AND '),
         string_agg(format('m.%1$I = b.%1$I', k), ' AND '),
         string_agg(format('x.%1$I = b.%1$I', k), ' AND ')
    INTO pk, pk_j, pk_s, on_sj, on_mb, on_xb
  FROM unnest(c.pk_cols) k;

  SELECT string_agg(quote_ident(attname), ', ' ORDER BY attnum),
         string_agg(format('%1$I = EXCLUDED.%1$I', attname), ', ' ORDER BY attnum)
           FILTER (WHERE attname <> ALL (c.pk_cols))
    INTO cols, upd
  FROM pg_attribute
  WHERE attrelid = c.mirror AND attnum > 0 AND NOT attisdropped;

  -- 0. Quittungen eines abgebrochenen Laufs nachholen
  CALL db2_fdw_sync_ack(p_mirror);

  LOOP
    EXIT WHEN i >= p_max_batches;
    i := i + 1;

    -- 1. Batch aus dem Journal (ORDER BY/LIMIT wird nach DB2 gepusht)
    EXECUTE format(
      'CREATE TEMP TABLE _db2sync_batch ON COMMIT DROP AS
         SELECT j.seq, %s FROM %s j ORDER BY j.seq LIMIT %s',
      pk_j, c.journal, c.batch_size);
    GET DIAGNOSTICS n_batch = ROW_COUNT;
    EXIT WHEN n_batch = 0;

    SELECT min(seq), max(seq) INTO lo, hi FROM _db2sync_batch;

    -- 2. aktueller Zustand der betroffenen Zeilen.
    --    Inner Join zweier Foreign Tables desselben Servers -> Join-Pushdown,
    --    DB2 liefert nur die geänderten Zeilen. DISTINCT ON entfernt Duplikate
    --    (mehrere Journal-Einträge pro PK).
    EXECUTE format(
      'CREATE TEMP TABLE _db2sync_src ON COMMIT DROP AS
         SELECT DISTINCT ON (%1$s) s.*
         FROM   %2$s s JOIN %3$s j ON %4$s
         WHERE  j.seq BETWEEN %5$s AND %6$s
         ORDER  BY %1$s',
      pk_s, c.source, c.journal, on_sj, lo, hi);

    -- 3. Upsert; unveränderte Zeilen werden nicht angefasst
    --    (wichtig für pg_ivm: keine leeren Deltas)
    EXECUTE format(
      'INSERT INTO %1$s AS m (%2$s) SELECT %2$s FROM _db2sync_src
       ON CONFLICT (%3$s) DO %4$s',
      c.mirror, cols, pk,
      CASE WHEN upd IS NULL THEN 'NOTHING'
           ELSE 'UPDATE SET ' || upd || ' WHERE ROW(m.*) IS DISTINCT FROM ROW(EXCLUDED.*)'
      END);
    GET DIAGNOSTICS n_up = ROW_COUNT;

    -- 4. Deletes: PK steht im Journal, existiert in DB2 aber nicht (mehr)
    EXECUTE format(
      'DELETE FROM %1$s m
       USING (SELECT DISTINCT %2$s FROM _db2sync_batch) b
       WHERE %3$s
         AND NOT EXISTS (SELECT 1 FROM _db2sync_src x WHERE %4$s)',
      c.mirror, pk, on_mb, on_xb);
    GET DIAGNOSTICS n_del = ROW_COUNT;

    -- 5. verarbeitete SEQs merken, lokal committen
    INSERT INTO db2_fdw_sync_pending (mirror, seq)
      SELECT p_mirror, seq FROM _db2sync_batch;
    UPDATE db2_fdw_sync_config
       SET last_sync     = now(),
           last_seq      = hi,
           rows_upserted = rows_upserted + n_up,
           rows_deleted  = rows_deleted  + n_del
     WHERE mirror = p_mirror;
    COMMIT;

    -- 6. in DB2 quittieren
    CALL db2_fdw_sync_ack(p_mirror);

    EXIT WHEN n_batch < c.batch_size;
  END LOOP;
END
$$;

/* ---------- Monitoring ------------------------------------------------- */

CREATE VIEW db2_fdw_sync_status AS
SELECT c.mirror, c.source, c.journal, c.last_sync, c.last_seq,
       c.rows_upserted, c.rows_deleted,
       (SELECT count(*) FROM db2_fdw_sync_pending p WHERE p.mirror = c.mirror) AS pending_acks
FROM   db2_fdw_sync_config c;

-- Rückstand = noch nicht verarbeitete Journal-Einträge in DB2 (Remote-Abfrage)
CREATE FUNCTION db2_fdw_sync_lag(p_mirror regclass) RETURNS bigint
LANGUAGE plpgsql STABLE AS
$$
DECLARE
  r bigint;
BEGIN
  EXECUTE format('SELECT count(*) FROM %s',
                 (SELECT journal FROM db2_fdw_sync_config WHERE mirror = p_mirror))
     INTO r;
  RETURN r;
END
$$;
