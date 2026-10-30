# db2_fdw sync – Skizze: Änderungen an DB2-Tabellen in lokale Tabellen übernehmen

Stand: Skizze auf Basis von `pg-fdw/db2_fdw` @ `0edc0b8` (18.3.0)

## Idee

Für jede zu überwachende DB2-Tabelle genügen **zwei Foreign Tables**:

| Foreign Table | DB2-Objekt | Inhalt |
|---|---|---|
| `fdw.employee` | `DB2INST1.EMPLOYEE` | die Daten |
| `fdw.j_employee` | `DB2FDW.J_EMPLOYEE` | Journal: `SEQ`, `OP`, `CHG_TS`, Primärschlüssel |

Statement-Trigger in DB2 füllen das Journal bei jedem INSERT, UPDATE und DELETE,
egal von welcher Anwendung die Änderung kommt. Eine Prozedur in PostgreSQL liest das
Journal über db2_fdw, holt den aktuellen Zeileninhalt aus `fdw.employee` und schreibt
das Ergebnis in eine **lokale Zieltabelle**. Danach löscht sie die verarbeiteten
Journaleinträge in DB2.

Für die Zieltabelle gibt es zwei Modi:

| Modus | Zieltabelle | Typischer Zweck |
|---|---|---|
| `mirror` | aktueller Stand der DB2-Tabelle, PK wie in DB2 | lokale Kopie, Reporting, Joins ohne DB2-Last |
| `history` | wächst nur; je Änderung eine neue Zeile mit `_db2_op`, `_db2_seq`, `_db2_chg_ts`, `_synced_at` | **Data Lake**, Historisierung, Auditing, Weitergabe an nachgelagerte Systeme |

```
 DB2                                         PostgreSQL
┌─────────────────────┐                     ┌────────────────────────────────────┐
│ DB2INST1.EMPLOYEE   │◄── Foreign Table ───│ fdw.employee                       │
│   │ Statement-      │                     │                                    │
│   ▼ Trigger I/U/D   │                     │   CALL db2_fdw_sync(target)        │
│ DB2FDW.J_EMPLOYEE   │◄── Foreign Table ───│ fdw.j_employee         │           │
│ (SEQ, OP, PK)       │   lesen + löschen   │                        ▼           │
└─────────────────────┘                     │ lake.employee_history  (append)    │
                                            │   oder                             │
                                            │ local.employee_mirror  (upsert)    │
                                            └────────────────────────────────────┘
```

## Warum nur PK im Journal

- Die Trigger bleiben schmal und billig: Pro DML-Statement wird ein einziges
  `INSERT … SELECT` aus der Transition Table ausgeführt.
- Den Zeileninhalt holt der Sync per **Join Quelle × Journal**. db2_fdw schiebt diesen Join
  nach DB2 (`db2GetForeignJoinPaths`, Inner Join, gleicher Server). Über die Leitung
  kommen also nur geänderte Zeilen.
- Mehrere Änderungen derselben Zeile zwischen zwei Läufen fallen automatisch auf den
  letzten Stand zusammen.
- Für die Journaltabelle ist kein Typ-Mapping nötig. Die PK-Spalten übernimmt DB2 selbst
  per `CREATE TABLE … AS (SELECT pk FROM quelle) WITH NO DATA`.

Die Konsequenz für den Data Lake: Er enthält den **Stand je Sync-Lauf**, nicht jeden
Zwischenstand. Wird eine Zeile zwischen zwei Läufen erst auf `x` und dann auf `y`
geändert, landet nur `y` im Lake. Wenn jeder Zwischenstand gebraucht wird, siehe
Ausbaustufe 1 (Full-Image-Journal).

## Konsistenz

IDENTITY-Werte werden beim INSERT vergeben, nicht beim COMMIT. Ein Verfahren nach dem Muster
„lies alles mit `SEQ > last_seq`“ würde deshalb Einträge verlieren. Die Skizze arbeitet
stattdessen nach dem Prinzip **konsumieren und löschen**:

1. Einen Batch aus dem Journal lesen. Unter CS mit `CUR_COMMIT=ON` sind nur kommittierte Zeilen sichtbar.
2. Den aktuellen Zustand der betroffenen PKs aus der Quelle holen und lokal schreiben.
   Die verarbeiteten SEQs in `db2_fdw_sync_pending` merken, dann **COMMIT**.
3. Genau diese SEQs im DB2-Journal löschen, dann **COMMIT** (`db2_fdw_sync_ack`).

| Abbruch nach … | Folge | Warum unkritisch |
|---|---|---|
| Schritt 1 / vor COMMIT in 2 | Rollback lokal, Journal unverändert | nächster Lauf wiederholt den Batch |
| COMMIT in 2 | SEQs pending, Journal noch voll | `db2_fdw_sync_ack` am Anfang des nächsten Laufs |
| Schritt 3 (DB2 committet, PG nicht) | SEQs bleiben pending | erneutes DELETE in DB2 ist ein No-op |

Zusätzliche Absicherung je Modus:

- **mirror:** Der Upsert schreibt nur Zeilen, die sich wirklich unterscheiden
  (`ROW(m.*) IS DISTINCT FROM ROW(EXCLUDED.*)`).
- **history:** `_db2_seq` ist UNIQUE, ein Batch kann also nicht doppelt angehängt werden.
  Zusätzlich wird eine Zeile nur angehängt, wenn sie sich von der letzten Zeile desselben
  PK im Lake unterscheidet.

Die Reihenfolge der Commits passt zu `db2Callbacks.c`: DB2 wird in
`XACT_EVENT_PRE_COMMIT` committet, also vor PostgreSQL.

**Initial Load:** Zuerst werden die sichtbaren Journal-SEQs gemerkt, dann wird die Quelle
gelesen. Alles, was in diesem Moment schon kommittiert war, steckt im Snapshot und wird
anschließend quittiert. Später kommittierte Einträge bleiben im Journal und laufen über
den normalen Sync.

## Zieltabelle im Modus `history`

| Spalte | Bedeutung |
|---|---|
| *Spalten der Quelle* | Zeileninhalt in DB2 zum Zeitpunkt des Syncs; bei `D` nur der PK |
| `_db2_op` | `L` = Initial Load, `I` = neu (oder nach Löschung wieder da), `U` = geändert, `D` = gelöscht |
| `_db2_seq` | letzte Journal-SEQ des PK im Batch (UNIQUE, NULL bei `L`) |
| `_db2_chg_ts` | Änderungszeitpunkt in DB2 (`CHG_TS` aus dem Journal) |
| `_synced_at` | Zeitpunkt der Übernahme in PostgreSQL |

Die Operation wird nicht aus dem Journal übernommen, sondern gegen die letzte Zeile des PK
im Lake bestimmt. Damit bleibt der Lake auch über Batch-Grenzen und Wiederholungen hinweg
in sich konsistent.

Den aktuellen Stand rekonstruieren:

```sql
SELECT *
FROM  (SELECT DISTINCT ON (empno) *
       FROM   lake.employee_history
       ORDER  BY empno, _synced_at DESC, _db2_seq DESC NULLS LAST) last
WHERE _db2_op <> 'D';
```

Den Stand zu einem Stichtag erhalten Sie mit derselben Abfrage plus
`WHERE _synced_at <= '2026-10-01'` in der inneren Abfrage.

Eine bestehende Data-Lake-Tabelle lässt sich direkt verwenden. Sie muss die Spalten der
Quelle sowie `_db2_op`, `_db2_seq` (UNIQUE), `_db2_chg_ts` und `_synced_at` enthalten.
Für große Lakes bietet sich eine Partitionierung nach `_synced_at` an.

## Bestandteile

| Datei | Inhalt |
|---|---|
| `db2_fdw_sync.sql` | Katalog, DDL-Generator, Setup, Initial Load, Sync, Ack, Status (PL/pgSQL) |
| `db2_side_employee.sql` | Beispiel der erzeugten DB2-DDL für `DB2INST1.EMPLOYEE` |

**Objekte:**

- `db2_fdw_sync_config`: eine Zeile pro Zieltabelle (Quelle, Journal, Modus, PK, Batchgröße, Zähler).
- `db2_fdw_sync_pending`: lokal geschriebene, in DB2 noch nicht quittierte SEQs.
- `db2_fdw_sync_db2_ddl(source)`: erzeugt die DB2-DDL für Journal und Trigger.
- `db2_fdw_sync_setup(source, journal, mode, target)`: legt die Zieltabelle an, falls sie fehlt,
  und registriert sie.
- `CALL db2_fdw_sync_initial_load(target)`: Vollabgleich. Bei `mirror` wird neu befüllt,
  bei `history` wird ein Snapshot mit `L` angehängt.
- `CALL db2_fdw_sync(target [, max_batches])`: inkrementeller Lauf.
- `CALL db2_fdw_sync_ack(target)`: Quittieren (wird von `db2_fdw_sync` mit aufgerufen).
- View `db2_fdw_sync_status` und Funktion `db2_fdw_sync_lag(target)` für das Monitoring.

## Verwendung (SAMPLE-Datenbank)

```sql
-- 1. Quelle importieren (setzt key 'true' auf die PK-Spalten)
IMPORT FOREIGN SCHEMA "DB2INST1" LIMIT TO (employee) FROM SERVER sample INTO fdw;

-- 2. DB2-DDL erzeugen und in DB2 ausführen (db2 -td@ -f ...)
\o /tmp/j_employee.db2
SELECT db2_fdw_sync_db2_ddl('fdw.employee', grantee => 'PGSYNC');
\o

-- 3. Journal als Foreign Table einbinden
IMPORT FOREIGN SCHEMA "DB2FDW" LIMIT TO (j_employee) FROM SERVER sample INTO fdw;

-- 4a. Data Lake (Trigger in DB2 müssen schon aktiv sein!)
SELECT db2_fdw_sync_setup('fdw.employee', 'fdw.j_employee',
                          mode => 'history', target => 'lake.employee_history');
CALL   db2_fdw_sync_initial_load('lake.employee_history');

-- 4b. oder: lokaler Spiegel
SELECT db2_fdw_sync_setup('fdw.employee', 'fdw.j_employee',
                          mode => 'mirror', target => 'local.employee');
CALL   db2_fdw_sync_initial_load('local.employee');

-- 5. zyklisch, z. B. mit pg_cron (Aufruf muss auf oberster Ebene erfolgen)
SELECT cron.schedule('db2sync-employee', '* * * * *',
                     $$CALL db2_fdw_sync('lake.employee_history')$$);

-- 6. Überwachung
SELECT * FROM db2_fdw_sync_status;
SELECT db2_fdw_sync_lag('lake.employee_history');
```

Pro Journal darf es genau **ein** Ziel geben, denn der Sync löscht konsumierte Einträge.
Sollen `mirror` und `history` dieselbe DB2-Tabelle bedienen, braucht jedes Ziel ein
eigenes Journal (z. B. `DB2FDW.J_EMPLOYEE` und `DB2FDW.J2_EMPLOYEE`). Alternativ wird
der Spiegel lokal aus dem Lake abgeleitet.

## Getestet

Getestet gegen PostgreSQL 16, mit lokalen Tabellen statt DB2 für Quelle und Journal.

**history:**

- Initial Load quittiert das Vor-Journal.
- Updates, doppelte Updates, PK-Änderung (wird zu `D` + `I`), Deletes und Inserts werden korrekt erfasst.
- Löschen und Neuanlage zwischen zwei Läufen ergeben `U`; Neuanlage plus Update ergeben `I`.
- Künstlich erneut eingespielte Journaleinträge hängen nichts an.
- Der aus dem Lake rekonstruierte Stand stimmt exakt mit der Quelle überein.

**mirror:**

- Updates, Deletes, Inserts und PK-Änderungen über mehrere Batches.
- Spiegel und Quelle sind danach identisch, das Journal ist leer.

Der DDL-Generator und das Setup (beide Modi) sind gegen einen Dummy-FDW mit
db2_fdw-Optionen getestet.

**Noch offen ist der Test gegen echtes DB2:**

- Kommt der Join-Pushdown für Quelle × Journal tatsächlich zustande?
  Prüfen mit `EXPLAIN (VERBOSE)` auf die Abfrage aus Schritt 2.
- Werden ORDER BY und LIMIT auf dem Journal nach DB2 gepusht?
- Wird `DELETE … WHERE seq = ANY($1)` als Direct Modify ausgeführt?
- Wie verhält sich `CUR_COMMIT` beim parallelen Schreiben?

Ein passender Testfall wäre `test/sql/tc035.sql`.

## Grenzen

- `TRUNCATE`, `LOAD` und `IMPORT … REPLACE` lösen in DB2 keine Trigger aus. Danach ist ein
  Initial Load nötig; im Modus `history` hängt er einen neuen `L`-Snapshot an.
  Gelöschte Zeilen erscheinen dabei nicht als `D`.
- Die Tabelle braucht einen PK bzw. `key`-Spalten. LOB-Spalten werden bei jeder Änderung
  komplett übertragen.
- Die Trigger erzeugen zusätzliche Schreiblast in DB2.
- Die Extension ist `relocatable = true`. Die Prozeduren referenzieren ihre Katalogtabellen
  deshalb über den `search_path`. Ein `SET search_path` an der Prozedur ist nicht möglich,
  weil es `COMMIT` verbietet.

## Ausbaustufen

1. **Full-Image-Journal** für einen lückenlosen Data Lake: Die Trigger schreiben alle Spalten
   (bei Updates altes und neues Bild) ins Journal. Der Sync hängt dann jede Journalzeile
   1:1 an, ein Join mit der Quelle ist nicht mehr nötig. Der Preis ist ein breiteres
   Journal und mehr Last in DB2. Umsetzbar als Option `full_image => true` im DDL-Generator
   und als Modus `history_full`.
2. **ASN-CD-Tabellen als Journalquelle**, falls IBM SQL Replication schon läuft
   (`IBMSNAP_COMMITSEQ`, `IBMSNAP_OPERATION`, Vor- und Nachbild). Dafür sind keine eigenen
   Trigger nötig. Die Reihenfolge folgt den Commits, das Lückenproblem entfällt also.
3. **C-Funktion `db2_execute(server name, sql text)`**: Damit kann `db2_fdw_sync_setup`
   die DB2-DDL selbst ausführen. Vorlage ist `db2_diag()` (Optionen sammeln, `db2GetSession`,
   `db2PrepareQuery`/`db2ExecuteQuery`).
4. **Background Worker** statt pg_cron, mit `NOTIFY db2_fdw_sync, '<target>'` nach jedem Batch,
   damit nachgelagerte Prozesse reagieren können.
