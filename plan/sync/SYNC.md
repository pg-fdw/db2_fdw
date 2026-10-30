# db2_fdw sync – Skizze: Änderungen an DB2-Tabellen inkrementell erkennen

Stand: Skizze auf Basis von `pg-fdw/db2_fdw` @ `0edc0b8` (18.3.0)

## Ziel

Änderungen in DB2-Tabellen (auch solche, die **nicht** über PostgreSQL laufen)
inkrementell in eine lokale PostgreSQL-Tabelle übernehmen. Auf dieser
Spiegeltabelle funktionieren dann lokale Werkzeuge wie **pg_ivm**,
logische Replikation, Trigger und `LISTEN/NOTIFY` ganz normal.

```
 DB2                                         PostgreSQL
┌─────────────────────┐                     ┌───────────────────────────────┐
│ DB2INST1.EMPLOYEE   │◄── Foreign Table ───│ employee        (db2_fdw)     │
│   │ Statement-      │                     │                               │
│   ▼ Trigger I/U/D   │                     │ CALL db2_fdw_sync(...)        │
│ DB2FDW.J_EMPLOYEE   │◄── Foreign Table ───│ j_employee      (db2_fdw)     │
│ (SEQ, OP, PK)       │                     │        │ upsert/delete        │
└─────────────────────┘                     │        ▼                      │
                                            │ employee_mirror (lokal, PK)   │
                                            │        │ pg_ivm-Trigger       │
                                            │        ▼                      │
                                            │ IMMVs                         │
                                            └───────────────────────────────┘
```

## Warum dieser Weg

- **Foreign-Table-Trigger reichen nicht:** pg_ivm hängt sich mit AFTER-Triggern an die Basistabellen.
  Diese feuern auf Foreign Tables nur bei DML über PostgreSQL. Änderungen direkt in DB2 sieht pg_ivm nie.
- **Das Journal hält nur Schlüssel:** Es enthält ausschließlich PK und Operation. Den aktuellen Zeileninhalt
  liest der Sync per **Join Quelle × Journal**. Diesen Join schiebt db2_fdw nach DB2
  (`db2GetForeignJoinPaths`, Inner Join, gleicher Server). So kommen nur geänderte Zeilen über die Leitung,
  und mehrere Änderungen an derselben Zeile fallen automatisch auf den letzten Stand zusammen.
- **Kein neuer C-Code für Stufe 1:** Alles nutzt die vorhandenen Pushdown-Pfade
  (Join, ORDER BY/LIMIT, `= ANY($1)` per `deparseScalarArrayOpExpr`, Direct Modify für das Quittieren).
  Benötigt werden nur die Spaltenoption `key`, die IMPORT FOREIGN SCHEMA bereits setzt, sowie die
  Tabellenoptionen `schema` und `table`.

## Konsistenz: das „Lückenproblem“ und wie es umgangen wird

IDENTITY-Werte werden beim INSERT vergeben, nicht beim COMMIT. Ein Ansatz der Form
„lies alles mit `SEQ > last_seq`“ verliert deshalb Einträge, wenn eine Transaktion
mit kleinerer SEQ später committet.

Die Skizze verzichtet daher auf ein Hochwasser-Verfahren und arbeitet nach dem Prinzip
**konsumieren und löschen**:

1. Den Batch lesen. Unter CS mit `CUR_COMMIT=ON` sind nur kommittierte Zeilen sichtbar.
2. Den Batch lokal anwenden (Upsert bzw. Delete nach aktuellem DB2-Zustand) und die verarbeiteten
   SEQs in `db2_fdw_sync_pending` merken, dann **COMMIT**.
3. Genau diese SEQs im DB2-Journal löschen, dann **COMMIT** (`db2_fdw_sync_ack`).

Eine später committete Zeile mit kleiner SEQ bleibt also einfach im Journal und
wird beim nächsten Lauf verarbeitet.

Fehlerfälle:

| Abbruch nach … | Folge | Warum unkritisch |
|---|---|---|
| Schritt 1 | nichts passiert | – |
| Schritt 2 (vor COMMIT) | Rollback lokal, Journal unverändert | nächster Lauf wiederholt den Batch |
| Schritt 2 (nach COMMIT) | SEQs pending, Journal noch voll | `db2_fdw_sync_ack` am Anfang des nächsten Laufs |
| Schritt 3 (DB2 committet, PG nicht) | SEQs bleiben pending | erneutes DELETE in DB2 ist ein No-op |

Das ergibt **at-least-once**-Verarbeitung mit **idempotentem** Anwenden: Der Sync liest
immer den aktuellen Zustand per PK, statt alte Werte zu übertragen. Im Ergebnis
entspricht das exactly-once. Die Reihenfolge der Commits passt zu `db2Callbacks.c`:
Dort committet DB2 in `XACT_EVENT_PRE_COMMIT`, also vor PostgreSQL.

## Bestandteile

| Datei | Inhalt |
|---|---|
| `db2_fdw_sync.sql` | Katalogtabellen, DDL-Generator, Setup, Initial-Load, Sync, Ack, Status (PL/pgSQL) |
| `db2_side_employee.sql` | Beispiel der erzeugten DB2-DDL für `DB2INST1.EMPLOYEE` |

**Objekte:**

- `db2_fdw_sync_config`: eine Zeile pro gespiegelter Tabelle (Spiegel, Quelle, Journal, PK, Batchgröße, Zähler).
- `db2_fdw_sync_pending`: lokal angewandte, in DB2 noch nicht quittierte SEQs.
- `db2_fdw_sync_db2_ddl(source)`: erzeugt Journaltabelle und Trigger für DB2.
  Die PK-Typen werden über `CREATE TABLE … AS (SELECT pk FROM quelle) WITH NO DATA` direkt aus DB2 übernommen,
  ein Typ-Mapping ist nicht nötig.
- `db2_fdw_sync_setup(source, journal)`: legt die Spiegeltabelle `<source>_mirror` mit PK an und trägt die Konfiguration ein.
- `CALL db2_fdw_sync_initial_load(mirror)`: Vollabgleich.
- `CALL db2_fdw_sync(mirror [, max_batches])`: inkrementeller Lauf.
- `CALL db2_fdw_sync_ack(mirror)`: Quittieren (wird von `db2_fdw_sync` mit aufgerufen).
- View `db2_fdw_sync_status` und Funktion `db2_fdw_sync_lag(mirror)` für das Monitoring.

## Verwendung (SAMPLE-Datenbank)

```sql
-- 1. Quelle importieren (setzt key 'true' auf die PK-Spalten)
IMPORT FOREIGN SCHEMA "DB2INST1" LIMIT TO (employee) FROM SERVER sample INTO public;

-- 2. DB2-DDL erzeugen und in DB2 ausführen (db2 -td@ -f ...)
\o /tmp/j_employee.db2
SELECT db2_fdw_sync_db2_ddl('employee', grantee => 'PGSYNC');
\o

-- 3. Journal als Foreign Table einbinden
IMPORT FOREIGN SCHEMA "DB2FDW" LIMIT TO (j_employee) FROM SERVER sample INTO public;

-- 4. Spiegel anlegen + erstbefüllen (Trigger in DB2 müssen schon aktiv sein!)
SELECT db2_fdw_sync_setup('employee', 'j_employee');
CALL   db2_fdw_sync_initial_load('employee_mirror');

-- 5. zyklisch (pg_cron), Aufruf muss auf oberster Ebene erfolgen
SELECT cron.schedule('db2sync-employee', '* * * * *',
                     $$CALL db2_fdw_sync('employee_mirror')$$);

-- 6. pg_ivm auf dem Spiegel
SELECT pgivm.create_immv('emp_per_dept',
  'SELECT workdept, count(*) AS cnt, sum(salary) AS total
     FROM employee_mirror GROUP BY workdept');
```

Hinweis zum Upsert: Er aktualisiert nur Zeilen, die sich tatsächlich geändert haben
(`WHERE ROW(m.*) IS DISTINCT FROM ROW(EXCLUDED.*)`). Dadurch bekommt pg_ivm keine
leeren Deltas, wenn ein Journaleintrag erneut verarbeitet wird.

## Getestet

Die Sync-Logik ist gegen PostgreSQL 16 mit simulierter Quelle und simuliertem Journal getestet,
also mit lokalen Tabellen statt DB2. Abgedeckt sind:

- mehrere Batches
- mehrfache Updates derselben Zeile
- PK-Änderung (wird zu Delete + Insert)
- Deletes und Inserts
- erneutes Anwenden alter Journaleinträge (Idempotenz, 0 Updates)

Der DDL-Generator und das Setup sind gegen einen Dummy-FDW mit db2_fdw-Optionen getestet.

**Noch offen ist der Test gegen echtes DB2:**

- Kommt der Join-Pushdown für Quelle × Journal tatsächlich zustande?
  Prüfen mit `EXPLAIN (VERBOSE)` auf die Abfrage aus Schritt 2.
- Werden ORDER BY und LIMIT auf dem Journal nach DB2 gepusht?
- Wird `DELETE … WHERE seq = ANY($1)` als Direct Modify ausgeführt?
- Wie verhält sich `CUR_COMMIT` beim parallelen Schreiben?

Ein passender Testfall wäre `test/sql/tc035.sql` nach dem Muster der vorhandenen `tcNNN.sql`.

## Grenzen

- `TRUNCATE`, `LOAD` und `IMPORT … REPLACE` lösen in DB2 keine Trigger aus. Danach ist `db2_fdw_sync_initial_load` nötig.
- Die Tabelle braucht einen PK bzw. `key`-Spalten. LOB-Spalten funktionieren, werden aber bei jeder Änderung voll übertragen.
- Die Trigger erzeugen zusätzliche Schreiblast in DB2. Statement-Trigger mit Transition Tables halten sie gering.
- Die Extension ist `relocatable = true`. Die Prozeduren referenzieren ihre Katalogtabellen deshalb
  unqualifiziert über den `search_path`. Ein `SET search_path` an der Prozedur ist nicht möglich,
  weil es `COMMIT` in Prozeduren verbietet.

## Ausbaustufen

1. **Alternative Journal-Quellen**, wählbar über `source_type` in der Konfiguration:
   - `asn`: CD-Tabellen von IBM SQL Replication (`IBMSNAP_COMMITSEQ`, `IBMSNAP_OPERATION`). Es sind keine
     eigenen Trigger nötig, aber `DATA CAPTURE CHANGES` muss aktiv sein. Die Spalte `IBMSNAP_COMMITSEQ` ist
     commit-geordnet, das Lückenproblem entfällt also.
   - `rowchange`: `ROW CHANGE TIMESTAMP` ohne Journal. Das ist der günstigste Weg, erkennt aber keine Deletes;
     dafür wäre ein periodischer PK-Abgleich nötig.
2. **C-Funktion `db2_execute(server name, sql text)`**: führt DDL direkt in DB2 aus. Damit kann
   `db2_fdw_sync_setup` die Trigger selbst anlegen. Sie lässt sich nach dem Muster von `db2_diag()`
   bauen (Optionen sammeln, `db2GetSession`, `db2PrepareQuery`/`db2ExecuteQuery` ohne Resultset).
3. **Background Worker** statt pg_cron: geringere Latenz, Backoff bei Verbindungsfehlern und
   `NOTIFY db2_fdw_sync, '<mirror>'` nach jedem angewandten Batch.
4. **Push statt Poll:** Ein DB2-Trigger sendet über `MQSEND` eine Nachricht, und der Worker
   startet daraufhin sofort einen Sync.
