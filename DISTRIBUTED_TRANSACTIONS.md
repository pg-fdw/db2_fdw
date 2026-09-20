# Distributed Transactions (Two-Phase Commit) — Design Plan

Status: **design / not implemented**. This document plans the work needed for
db2_fdw to keep multiple DB2 connections consistent when one PostgreSQL
transaction spans several `db2_fdw` foreign servers (or several user mappings
against the same server).

The XA/2PC machinery described here is designed to ship as a **separate,
optional companion extension** (`db2_fdw_xa`), not as code baked into
db2_fdw itself — see "Packaging: optional companion extension" below for why
and how. Everywhere this document says "the coordinator does X," read that as
"`db2_fdw_xa`, if installed, does X"; db2_fdw's own source only gains one
small, unconditional lookup that no-ops when `db2_fdw_xa` isn't present.

## Problem

A single backend can have several cached `DB2ConnEntry` connections at once
(`include/DB2ConnEntry.h`, `include/DB2EnvEntry.h` — one `SQLHENV` per backend,
N connections keyed by server/user hung off `rootenvEntry->connlist`). Today
each connection commits **independently**:

- `source/db2Callbacks.c:44-49` — `transactionCallback()` fires once *per
  connection* (the callback is registered with `arg = connp` in
  `db2RegisterCallback()`), and on `XACT_EVENT_PRE_COMMIT` immediately calls
  `db2EndTransaction(arg, 1, 0)`.
- `source/db2EndTransaction.c:50-56` — that call does a bare
  `SQLEndTran(SQL_HANDLE_DBC, connp->hdbc, SQL_COMMIT)`.

If a transaction writes through two DB2 connections and the first `SQLEndTran`
succeeds while the second fails (network blip, DB2 instance down, lock
timeout), the transaction is left half-committed across the two DB2 databases
with no way to reconcile — and PostgreSQL's own local commit still proceeds,
so there's no way to even roll the whole thing back after the fact. This is
the classic N-database atomicity problem 2PC exists to solve.

Separately, `source/db2Callbacks.c:50-52` currently makes `PREPARE
TRANSACTION` an outright error whenever DB2 tables were touched — there is no
attempt at real two-phase behavior anywhere in the extension today.

## Why `SQLEndTran` alone can't provide this

`SQLEndTran(SQL_COMMIT)` is one-phase: it commits or fails atomically, with no
"prepare now, decide commit-or-rollback later, possibly from a different
process" step. Getting that separation from DB2 requires the X/Open **XA**
protocol, which DB2 exposes via an XA switch struct (`xa_switch_t`) — the
same interface CICS/Tuxedo/WebSphere JTA use to enlist DB2 in distributed
transactions.

Verified against the DB2 11.5 client actually installed in this environment
(`$DB2_HOME` → `/opt/ibm/db2/V11.5`): there is **no separate XA-only shared
library** (no `libdb2xa64.so` or similar — the only "xa"-named file under
`lib64` is an unrelated Xalan XML library). The switch structures
(`db2xa_switch`, `db2xa_switch_std`, `db2xa_switch_static`,
`db2xa_switch_static_std`) are defined directly inside `libdb2.so`/`libdb2.so.1`
— the exact library db2_fdw already links via `-ldb2` (`Makefile:86`). A
runtime probe (small C program linked against `-ldb2`, reading the struct and
resolving each function pointer with `dladdr()`) confirmed all ten XA entry
points (`xa_open_entry`, `xa_close_entry`, `xa_start_entry`, `xa_end_entry`,
`xa_rollback_entry`, `xa_prepare_entry`, `xa_commit_entry`,
`xa_recover_entry`, `xa_forget_entry`, `xa_complete_entry`) resolve to real
code inside `libdb2.so.1` itself (backed by the library's own internal
`sqlxaConnect`/`sqlxaPrepareAll`/`sqlxaCommitAll`/`sqlxaRollbackAll`/
`sqlxaGetIndoubtList` implementations) — not stubs, not NULL, not forwarded
elsewhere. `db2xa_switch`/`db2xa_switch_std` report `flags=0x3` (dynamic
registration capable); `db2xa_switch_static`/`db2xa_switch_static_std` report
`flags=0x2` and leave `xa_complete_entry` NULL. The non-static, `_std`-suffixed
switch (`db2xa_switch_std`) is the right one for a single-threaded caller like
a PostgreSQL backend.

PostgreSQL core has no generic "foreign transaction manager" to lean on here:
a "global/foreign transaction manager for FDWs" patch set has circulated on
`-hackers` for years without being merged, and core's own two-phase resource
manager table (`twophase_rmgr.c`) is a fixed, compile-time array — an
out-of-core extension cannot register into it, so `PREPARE
TRANSACTION`/`RegisterTwoPhaseRecord` can't be reused either. db2_fdw has to
own the entire coordinator role itself: durable prepare-logging, driving XA on
each DB2 branch, and crash recovery of in-doubt branches.

## Goals / non-goals

- **Goal:** when one PostgreSQL transaction writes through ≥2 `db2_fdw`
  connections, either all of them commit or all roll back — including across
  a PostgreSQL or DB2 crash between phases.
- **Goal:** zero behavior change and zero overhead for the common case (a
  transaction touching at most one DB2 connection) when `two_phase_commit`
  is `off` — today's fast path stays untouched.
- **Goal:** when `two_phase_commit=on`, a transaction that only ever touches
  one DB2 connection must never pay for a standalone `xa_prepare` round
  trip — only genuinely multi-connection transactions pay the full
  two-phase cost (see "Cost model" under Architecture § Global commit
  coordinator).
- **Goal:** PostgreSQL's own local commit stays the single source of truth;
  once it's WAL-durable, DB2 branches are only ever resolved to match it
  (commit), never rolled back after that point.
- **Non-goal (initially):** exposing db2_fdw as an XA resource to an
  *external* transaction manager. Scope is coordinating N DB2 connections from
  inside one PostgreSQL backend's own commit.
- **Non-goal (initially):** lifting the `PREPARE TRANSACTION` restriction for
  client-driven distributed transactions (see Phase 4).
- **Goal:** an install of db2_fdw that never installs the companion
  `db2_fdw_xa` extension carries **zero** footprint from this feature — no
  new shared memory, no new locks, no new background worker, not even a new
  GUC beyond the per-server `two_phase_commit` option itself defaulting to a
  cheap early-out (see "Packaging" below).

## Packaging: optional companion extension

The coordinator, XA branch wrapper, durable prepare log, and crash-recovery
worker (items 1–4 below) ship as their own PGXS extension, `db2_fdw_xa`, not
as part of db2_fdw's own shared library. db2_fdw gains exactly one new,
always-present piece: at `XACT_EVENT_PRE_COMMIT`, its existing
`transactionCallback` (`source/db2Callbacks.c`) calls
`find_rendezvous_variable("db2_fdw_xa_coordinator")` — a standard core
PostgreSQL API (`fmgr.h`, confirmed present in this environment's PG18
headers) that lets one independently-loaded shared library publish a pointer
for another independently-loaded library to find by name, with no link-time
or load-order dependency between them.

- **`db2_fdw_xa` not installed** → the rendezvous variable is unset, the
  lookup returns `NULL`, db2_fdw falls straight through to today's exact
  code path (`db2EndTransaction`, independent per-connection
  `SQLEndTran`). One hash-table lookup is the entire cost paid by every
  installation that doesn't need this feature.
- **`db2_fdw_xa` installed** (`CREATE EXTENSION db2_fdw_xa;`, plus it in
  `shared_preload_libraries` for its recovery worker — see Phase 3) → its
  `_PG_init()` populates the rendezvous variable with a pointer to a
  version-tagged struct of function pointers (`register_branch`,
  `commit_all`, …). db2_fdw's callback dereferences that struct and
  delegates instead of committing directly.
- The dependency direction is deliberate: **db2_fdw optionally calls out to
  `db2_fdw_xa`**, never the reverse. The alternative (an external module
  reaching into db2_fdw's connection cache) would require db2_fdw to export
  `rootenvEntry`/`DB2ConnEntry` as public API and would leave callback
  *ordering* between db2_fdw's own per-connection handler and an external
  one ambiguous. Calling out from inside db2_fdw's own callback avoids both
  problems.
- **ABI handshake:** two independently built/versioned `.so`s are talking
  through a raw struct, so the struct carries a version field db2_fdw checks
  before using any of it — a version mismatch must fail loudly (`ERROR`,
  refuse to use the companion extension) rather than misinterpret a struct
  laid out differently by a mismatched build. Same pattern core already uses
  for `FdwRoutine` itself.
- This split also fully isolates the operationally heaviest piece —
  `db2_fdw_xa`'s crash-recovery `BackgroundWorker`, which needs
  `shared_preload_libraries` and therefore a server restart to enable — from
  everyone who doesn't need cross-connection atomicity. Nobody running plain
  db2_fdw today is asked to accept that cost.
- It also happens to be the right shape for the cross-FDW scenario discussed
  separately below: if `db2_fdw_xa`'s struct is kept generic (keyed by an XA
  switch pointer and an opaque resource name, rather than DB2-specific
  types), it's a plausible seed for a shared coordinator a patched
  oracle_fdw could register into too — worth deciding deliberately during
  Phase 0 rather than naming things DB2-specifically and having to redo it
  later.

## Architecture

### New components

Items 1–4 below live in the separate `db2_fdw_xa` extension's own source tree
(its own `Makefile`/control file/PGXS build), not in db2_fdw's. db2_fdw's
only change is the one `find_rendezvous_variable` lookup described under
"Packaging" above, added to the existing `transactionCallback` in
`source/db2Callbacks.c`.

1. **Global commit coordinator** (`db2_fdw_xa`'s own source, e.g.
   `xaCoordinator.c`) Runs from inside db2_fdw's existing
   `XACT_EVENT_PRE_COMMIT` handler once db2_fdw has found it via the
   rendezvous lookup — it does not register its own `XactCallback`, since
   db2_fdw's connection cache (`rootenvEntry->connlist`) is the source of
   truth for which `DB2ConnEntry`s participated. The struct handed back
   through the rendezvous variable therefore needs a way for the coordinator
   to iterate those connections without linking against db2_fdw's internal
   headers — e.g. db2_fdw passes its own connection list through the call
   rather than the coordinator reaching in on its own.

   **Participation is decided by a new per-connection write flag, not by
   `xact_level`.** Checked against the current code: `xact_level`
   (`DB2ConnEntry.xact_level`) is set to `1` unconditionally by
   `db2GetSession()` (`source/db2GetSession.c:44-46`) the first time *any*
   session is obtained on a connection — including from `db2BeginForeignScan`
   for a plain read-only `SELECT`, identically to the write paths
   (`db2BeginForeignModifyCommon`, `db2BeginDirectModify`,
   `db2ExecForeignTruncate`). So `xact_level > 0` means "this connection has
   an open remote transaction," not "this connection wrote something" — using
   it to decide 2PC participation would count two plain read-only
   cross-server `SELECT`s as 2 participants and either `ERROR` (if
   `two_phase_commit=off`) or run full 2PC for a transaction with nothing to
   protect. (The existing `dml_in_transaction` flag isn't a fix either: it's
   a single process-global `bool`, not per-connection, and it isn't even set
   by every write path — `db2BeginDirectModify.c`/`db2IterateDirectModify.c`,
   the pushed-down-UPDATE/DELETE path, never touches it.)

   So db2_fdw needs one small addition of its own (independent of whether
   `db2_fdw_xa` is installed — it's cheap, a single field write): a new
   `int wrote;` (or similar) field on `DB2ConnEntry`, set at every actual
   write site — `db2ExecForeignInsert.c`, `db2ExecForeignUpdate.c`,
   `db2ExecForeignDelete.c`, `db2ExecForeignBatchInsert.c`,
   `db2IterateDirectModify.c`, and `db2ExecuteTruncate.c` — and cleared
   alongside `xact_level` in `db2EndTransaction()`. The coordinator counts
   and enrolls only connections with `wrote` set; connections that only
   read (`xact_level>0`, `wrote=0`) are left entirely to today's existing
   per-connection `SQLEndTran(SQL_COMMIT)` path (`db2EndTransaction`) — no
   XA involvement, no `xa_prepare`, no reliance on `XA_RDONLY`, regardless of
   how many other DB2 servers were read from in the same transaction. This is
   strictly better than the `XA_RDONLY`-based approach floated earlier: a
   read-only connection now pays zero XA cost instead of paying for an
   `xa_prepare` round trip that just happens to come back harmless.
   - A connection that wrote, whose server has `two_phase_commit=off` (the
     default), never goes through XA at all — unchanged today's path,
     one-phase `SQLEndTran(SQL_COMMIT)`.
   - 0 or 1 writing connections XA-enrolled (server has
     `two_phase_commit=on`) →
     **one-phase-optimized XA commit**: `xa_end`, then `xa_commit(xid, rmid,
     TMONEPHASE)` directly — no `xa_prepare` round trip, since XA mandates
     every compliant resource manager support one-phase commit for the
     single-participant case (this is what WebLogic/Narayana/MSDTC/Tuxedo all
     do automatically for the same reason). Slightly more than the `off`
     path (an `xa_start`/`xa_end` pair was still unavoidable — see "Cost
     model" below) but never pays for a standalone prepare.
   - ≥2 writing connections XA-enrolled → run the full two-phase protocol
     below (`xa_prepare` each, durable log, then `xa_commit` each with
     `TMNOFLAGS`).
   - ≥2 writing connections where at least one involved server has
     `two_phase_commit=off` → `ERROR` and abort the local transaction; never
     fall back to independently committing each connection (see "Behavior at
     `two_phase_commit=off` with ≥2 connections" under Open Questions). A
     transaction reading from any number of DB2 servers plus writing to at
     most one is unaffected either way — only ≥2 *writing* connections ever
     reach this branch or the 2PC branch above.

   **Cost model.** `two_phase_commit=on` can safely be left configured on a
   server permanently — the coordinator decides *at commit time* which of
   the three paths above applies, so single-resource transactions never pay
   for a full 2PC exchange. What can't be avoided: `xa_start` must run
   *before* the first statement executes on a connection, because that's
   what associates subsequent SQL with a branch that can later be committed
   — there's no way to run statements first and decide only at commit time
   whether the connection needed to be under XA, since DB2 has no operation
   to retroactively enlist already-executed work into a branch. So every
   connection touched on a `two_phase_commit=on` server pays a small,
   unavoidable `xa_start`/`xa_end` enrollment cost per transaction; only the
   genuinely multi-connection case pays the larger cost of a standalone
   `xa_prepare` round trip per branch.

   Note DB2 CLI's own `SQL_ATTR_SYNC_POINT`/`SQL_ATTR_CONNECTTYPE` attributes
   (`sqlcli1.h:787-897`, documented in `sqlenv.h:1970` ff.) are not a
   shortcut around this: `SQL_SYNC_TWOPHASE` is explicitly documented as
   "requires a Transaction Manager (TM) to coordinate two-phase commits,"
   and the CLI default, `SQL_SYNC_ONEPHASE`, explicitly does *not* give real
   cross-database atomicity ("enforces single updater, multiple read
   behavior" only). db2_fdw's coordinator has to be that TM; there is no
   CLI-level feature that does it for us.

2. **XA branch wrapper** (`db2_fdw_xa`'s own `DB2Xa.h`/`db2Xa*.c` — this is
   the one component that does need to link against `-ldb2` itself, same as
   db2_fdw does, since it calls the switch's entry points directly)
   No `dlopen`/`dlsym` needed — `db2xa_switch_std` (the dynamic-registration
   variant of the `xa_switch_t` struct, see above) lives directly in
   `libdb2.so`, which db2_fdw already links via `-ldb2`. Declare it
   `extern struct xa_switch_t db2xa_switch_std;` against a local `xa_switch_t`
   definition (or the real `xa.h` under `$DB2_HOME/include` if it ships one)
   and call its entry points directly: `xa_open_entry`, `xa_start_entry`
   (`TMNOFLAGS`), `xa_end_entry` (`TMSUCCESS`), `xa_prepare_entry`,
   `xa_commit_entry`, `xa_rollback_entry`, `xa_recover_entry`. Each
   `DB2ConnEntry` gets an XID: `gtrid` unique
   per backend-transaction (e.g. `<node>.<backend pid>.<local xid>`, ≤64 bytes
   per the XA spec), `bqual` = the connection's branch index, so every DB2
   connection in one PostgreSQL transaction shares a `gtrid` with distinct
   `bqual`s. Since a backend is single-threaded, DB2's thread-association XA
   semantics collapse to a simple 1:1 mapping — much simpler than the
   multi-threaded TP-monitor case DB2's XA support is normally built for.

3. **Durable prepare log** (`db2_fdw_xa`'s own `xactLog.c`, on-disk directory
   `$PGDATA/pg_db2_fdw_xact/`, modeled on how core's own
   `$PGDATA/pg_twophase/` is written and fsynced)
   - At `PRE_COMMIT`, before any `xa_prepare`, write one file named by the
     local `gtrid` listing every branch about to be prepared; fsync the file
     and its directory (durable-rename pattern, same idea as
     `RecreateTwoPhaseFile`).
   - Call `xa_prepare` on each branch. On any failure: `xa_rollback` every
     branch already prepared, delete the log file, `ereport(ERROR, ...)` so
     PostgreSQL aborts the local transaction normally (driving
     `XACT_EVENT_ABORT`, which today's rollback path already handles for any
     other connections).
   - If every branch prepares, return normally from `PRE_COMMIT`. PostgreSQL
     now writes its own local commit WAL record — from this instant on, the
     transaction *is* committed, so the log's mere existence means "must
     finish committing," never "may still roll back." Deciding once at
     `PRE_COMMIT` and letting the local WAL commit be the tie-breaker is the
     standard way to bolt 2PC onto a system whose own commit is already
     authoritative.
   - At `XACT_EVENT_COMMIT`, `xa_commit` every logged branch, then delete the
     log file. Errors here must be logged, not raised — the local transaction
     is already committed, and `xa_commit` on an already-prepared branch is
     safe to retry.

4. **Crash / in-doubt recovery** (`db2_fdw_xa`'s own `xactRecovery.c`, needs a
   `BackgroundWorker`). Because this lives entirely in the companion
   extension, db2_fdw itself never needs `shared_preload_libraries` or a
   bgworker — only installs that opt into `db2_fdw_xa` take on that
   requirement.
   - On `_PG_init`, scan `$PGDATA/pg_db2_fdw_xact/` for leftover log files.
     Because a file only exists *after* the local-commit decision point above,
     every leftover file means "finish committing" — recovery never decides to
     roll back.
   - A background worker resolves each leftover entry: `xa_open` the branch,
     `xa_recover` to confirm DB2 still holds it prepared (DB2 may already have
     resolved it via its own crash recovery), `xa_commit` if still prepared,
     then delete the log file.

### Sequence (transaction touching two db2_fdw servers)

```
Backend                    DB2ConnEntry A (srv1)   DB2ConnEntry B (srv2)
-------                    ---------------------   ---------------------
INSERT ... srv1            xact_level=1
INSERT ... srv2                                     xact_level=1
COMMIT
  PRE_COMMIT (coordinator):
    connections = [A,B]  -> >1, use 2PC path
    write+fsync prepare-log(gtrid, [A,B])
    xa_prepare(A) ------> OK
    xa_prepare(B) ------------------------------->  OK
  (PostgreSQL's local WAL commit happens here)
  COMMIT (coordinator):
    xa_commit(A) -------> OK
    xa_commit(B) --------------------------------> OK
    delete prepare-log
```

## Rollout phases

1. **Phase 0 — groundwork, no behavior change.** Scaffold the `db2_fdw_xa`
   extension itself (its own `Makefile`/control file, `CREATE EXTENSION
   db2_fdw_xa;`) and add the `find_rendezvous_variable` lookup plus the
   version-tagged handoff struct to db2_fdw's `transactionCallback`, but with
   `db2_fdw_xa`'s coordinator implementing nothing yet beyond the ≤1-
   connection passthrough to `db2EndTransaction()`. Add a regression test
   (`tcNNN`) with two user mappings against the same DB2 server to exercise
   two `DB2ConnEntry`s without needing a second physical DB2 instance,
   run once with `db2_fdw_xa` absent and once installed, confirming both
   paths still commit correctly and that db2_fdw behaves identically either
   way at this stage. This is also where the generic-vs-DB2-specific naming
   decision for the handoff struct (see "Packaging" above) gets made.
2. **Phase 1 — XA plumbing (in `db2_fdw_xa`).** Add the XA branch wrapper and
   wire `xa_open`/`xa_start`/`xa_end` per connection alongside (not instead
   of) db2_fdw's own `SQLDriverConnect` — CLI still executes SQL, XA only
   governs transaction boundaries. Gate the whole feature behind a new
   per-server option (e.g. `two_phase_commit=on|off`, default `off`) so
   existing installs are unaffected until they opt in *and* install
   `db2_fdw_xa`. This phase must ship together with the
   ≥2-connections-and-`off`-means-`ERROR` guard from item 1 of the
   Architecture section — there is no intermediate released state where a
   multi-connection transaction silently commits each connection
   independently once this phase exists. Note this guard has to fire even
   when `db2_fdw_xa` isn't installed at all (≥2 connections + no companion
   extension present is just another form of "`two_phase_commit` can't
   actually be `on`").
3. **Phase 2 — real 2PC for the ≥2-connection case (in `db2_fdw_xa`)**, using
   the prepare/commit log. Needs a soak-test procedure that kills DB2
   connectivity to one server between prepare and commit (hard to script
   against a live DB2/pg_regress; document a manual procedure, and consider
   a mock XA switch for a unit-style test).
4. **Phase 3 — crash recovery worker (in `db2_fdw_xa`).** Test via `pg_ctl
   stop -m immediate` (or `kill -9` of the backend) between prepare and
   commit, then verify on restart that DB2 shows the branch committed on
   both servers. This is also where `db2_fdw_xa` goes into
   `shared_preload_libraries` for the first time in the rollout — db2_fdw
   itself never needs to. Also ships the two admin-facing functions from
   "Admin intervention" below: a `db2_fdw_xa_resolve_now()` to force an
   immediate retry pass instead of waiting for the worker's normal schedule,
   and a `db2_fdw_xa_forget(gid)` heuristic override for the genuinely
   unresolvable case — both are part of this phase's scope, not an
   afterthought, since Phase 3 is what makes in-doubt branches an operational
   reality operators need tools for.
5. **Phase 4 (stretch, optional) — client-driven `PREPARE TRANSACTION`.** Once
   branches are durable via XA, reconsider today's hard error at
   `XACT_EVENT_PRE_PREPARE`: key the durable log by PostgreSQL's own GID
   instead of a private `gtrid`, so db2_fdw can participate in a transaction
   coordinated by something other than this backend's own COMMIT (e.g. a
   distributed system driving 2PC across multiple PostgreSQL nodes). Recovery
   then has to consult `pg_prepared_xacts` instead of relying on "local WAL
   commit implies commit" — materially harder, so only scope this once Phases
   0–3 are solid.

## Cross-FDW coordination (e.g. db2_fdw + oracle_fdw) — out of scope for this repo alone

Everything above coordinates connections *within db2_fdw*. It does not help a
transaction that writes through db2_fdw **and** a separately-maintained FDW
such as `oracle_fdw` in the same transaction — that scenario needs PostgreSQL
itself to act as the XA coordinator, and that capability does not exist, and
cannot be added from this repo alone.

**Verified against this system's actual PG18 headers** (not just recollection
of mailing-list history):

- `foreign/fdwapi.h`'s `FdwRoutine` has **no** Prepare/Commit/Rollback/
  Transaction-related callback of any kind — grepping it for those terms
  returns nothing.
- `access/twophase_rmgr.h` defines `TWOPHASE_RM_MAX_ID` as a **fixed,
  compile-time array of exactly 5 entries** (`END`, `LOCK`, `PGSTAT`,
  `MULTIXACT`, `PREDICATELOCK`). There is no slot an out-of-core extension —
  db2_fdw, oracle_fdw, or anything else — can register into.

So core has no shared vocabulary today for two independent FDWs to enlist in
one transaction. What would be required to create one:

1. **A new, generic `FdwRoutine` callback set (core change).** Something like
   `GetPrepareId`, `PrepareForeignTransaction(server, usermapping, gid)`,
   `CommitForeignTransaction`, `RollbackForeignTransaction`,
   `ForgetForeignTransaction`, plus resolver-facing
   `ResolveForeignTransactions`/`IsForeignTransactionResolved`. This is the
   shape of the "global/foreign transaction manager" (`fdwxact`) patch series
   proposed on `-hackers` by Masahiko Sawada, Ashutosh Bapat and others
   roughly 2018–2021; it stalled and never merged.
2. **A durable, generic transaction-participant registry in core** — the
   FDW-agnostic analogue of `pg_prepared_xacts`, tracking which (foreign
   server, user mapping) pairs registered as writers in the current local
   transaction, WAL-logged so crash recovery can find in-doubt foreign
   branches (surfaced as something like a `pg_foreign_xacts` view).
3. **A GID scheme that survives translation across resource managers.** Core
   hands each participant one opaque id; each FDW then maps it into whatever
   its remote side needs — db2_fdw into an XA `XID` (gtrid/bqual) via the
   `db2xa_switch_std` struct confirmed working above, oracle_fdw into
   Oracle's own XA switch (OCI exposes one, `xaosw`, in `libclntsh.so` — same
   shape of problem, different library and GID rules), postgres_fdw into a
   remote's *native* `PREPARE TRANSACTION`/`COMMIT PREPARED` with the GID as
   plain text. Three different remote addressing schemes behind one
   core-issued id.
4. **Ordinary `COMMIT` becomes implicitly two-phase whenever ≥2 XA-capable
   FDWs participated** — not just an explicit client `PREPARE TRANSACTION`.
   Pre-commit calls `PrepareForeignTransaction` on every participant; only
   once all succeed does core write the local commit WAL record (the actual
   commit point); post-commit calls `CommitForeignTransaction` on each. Same
   "local WAL commit is the tiebreaker" shape as the db2_fdw-only design
   above, lifted to a place that can see participants from *any* FDW.
5. **A resolver background worker, plus a policy decision on blocking:** does
   the client's `COMMIT` wait for phase 2 against both DB2 and Oracle before
   returning, or return once local commit + all prepares are durable, with a
   worker finishing phase 2 asynchronously (better latency, but another
   session can briefly see the transaction as committed while the Oracle-side
   effect isn't visible yet)? This exact question is what stalled the
   original core patch.

**Why this can't be done unilaterally from db2_fdw:** oracle_fdw is Laurenz
Albe's independent project, not part of this repo. Even a flawless db2_fdw
implementation of items 1–5 only gets "all DB2 connections in this
transaction are atomic" — it says nothing about Oracle, and there is no
visibility from db2_fdw's `XactCallback` into oracle_fdw's connections or vice
versa. The realistic options:

- **Contribute `fdwxact` upstream.** The only path to a real, standard
  mechanism any FDW can implement once and interoperate through — a
  multi-year PostgreSQL core project with unresolved design contention
  (see point 5), not something scoped to this codebase.
- **A private, non-core convention:** a small shared coordinator (its own
  extension/library) that a patched db2_fdw *and* a patched oracle_fdw both
  explicitly call into at connection-open and commit time — reinventing
  pieces of 1–5 outside core, buildable without waiting on PostgreSQL, but
  requiring bilateral agreement with oracle_fdw's maintainer (or a fork) to
  add matching calls on the Oracle side. The `db2_fdw_xa` companion
  extension (see "Packaging: optional companion extension" above) is,
  mechanically, exactly this shape — an independently-loaded coordinator
  found via `find_rendezvous_variable` rather than a compile-time
  dependency. If it's ever worth pursuing this option for real, the groundwork
  (rendezvous-based handoff, a generic-rather-than-DB2-specific struct) is
  the same regardless of whether oracle_fdw ever adopts it; that's why Phase
  0 above calls out the generic-vs-DB2-specific naming decision explicitly.
- **Two independent islands** — db2_fdw's own coordinator (this document)
  plus a separate, hypothetical oracle_fdw-only one — gives atomicity
  *within* each FDW's own connections but explicitly **not** across the
  db2/oracle boundary. This is the status quo direction and does **not**
  solve the db2+oracle-in-one-transaction scenario.

**Recommendation:** treat cross-FDW (db2_fdw + oracle_fdw) coordination as out
of scope for this repository. If it becomes a real requirement, the honest
next step is opening a conversation with oracle_fdw's maintainer about a
shared private convention, rather than building something in isolation that
Oracle-side code structurally cannot see.

## Admin intervention: the commit/rollback asymmetry and stuck branches

An admin will eventually need to break out of a transaction that's stuck
mid-2PC — a hung `xa_prepare`/`xa_commit`, a DB2 outage, a genuine
split-brain. The instinct "prefer rollback, it's the safer option" is correct
for *most* software, but 2PC has a hard asymmetry that overrides it past a
certain point, and the available tools differ completely depending on which
side of that point the stuck branch is on.

**The asymmetry.** There is exactly one moment that decides which recoveries
are still available: whether PostgreSQL has already written its own local
commit WAL record.
- **Before that moment** (still in `PRE_COMMIT`, still preparing branches):
  aborting and rolling back every prepared branch is free and always safe —
  this is already the default behavior on any `xa_prepare` failure (see
  Architecture § Durable prepare log). Nothing new is needed here; this is
  exactly the "prefer rollback" instinct, and it's correct for this half of
  the timeline.
- **After that moment**: the *only* correct terminal state for every
  participating branch is commit. Rolling one back instead would mean the
  local database says "committed" while DB2 says "never happened" — the
  exact cross-database inconsistency this whole feature exists to prevent.
  This isn't a conservative policy choice; it's what "atomic across two
  databases" means. "Roll back as much as possible" simply has no correct
  answer on this side of the line — the system has already told every other
  session the transaction happened.

**Why commit ordering (local last) is fixed, not a tunable.** It's tempting
to ask whether committing remote branches *before* the local commit —
instead of after — would reduce the in-doubt/blocking window. It wouldn't,
and it would make failures strictly worse:
- The reason 2PC has an in-doubt window at all is structural: only one write
  in the whole system can ever be truly atomic, so every other participant
  must reach a durable "prepared" state before that one write happens, and
  match it afterward. Whichever side goes last is, by definition, the
  tie-breaker. Committing DB2 first doesn't remove the need for a
  tie-breaker, it just hands that role to DB2 instead of Postgres — a hang
  or crash between DB2's commit and the local WAL commit is exactly as
  blocking as one between prepare and commit is today, just mirrored.
- It's worse for us specifically because Postgres's WAL commit is the one
  operation in this whole design that comes with a fully solved,
  crash-recoverable durability story for free (it's why the durable prepare
  log + recovery worker in Architecture § Durable prepare log always have a
  deterministic outcome to finish). If DB2 committed first and the local WAL
  commit then failed or the backend crashed before landing, there would be
  nothing to recover from on restart — Postgres never decided to commit, so
  the transaction looks like it never happened locally while DB2 already
  applied it, with no log entry pointing at the problem. That's a strictly
  worse, invisible failure mode compared to today's design, where every
  failure after the log is written is eventually resolvable.
- Split-brain specifically isn't a sequencing question at all — it comes
  from DB2's own indoubt-transaction timeout/heuristics unilaterally
  resolving a long-prepared branch, or from a network partition (which is
  exactly why "Admin intervention" above points at DB2's own `RESOLVE
  INDOUBT TRANSACTIONS` as the real tool for that case). Neither depends on
  which side we call last.
- (Protocols that do address 2PC's inherent blocking property — three-phase
  commit, Paxos/Raft-based commit — trade it for materially more round
  trips and stronger assumptions than XA offers, and DB2's interface doesn't
  support anything beyond standard two-phase XA regardless, so they aren't
  an avenue open to us here.)

**Why a strict connection-close guard doesn't cost you an escape hatch.**
`rootenvEntry` and the whole connection cache are per-backend, private
memory — `DB2_close_connections()` can only ever affect the session that
calls it, never a *different*, stuck session. So it was never a viable tool
for admin intervention in someone else's hung backend in the first place;
the real tool for that is `pg_terminate_backend()` (or an OS-level kill),
which PostgreSQL already provides, entirely independent of how strict our
guard is. Making the guard strict (per the connection-lifecycle resolution
above) only removes a way for a session to corrupt *its own* bookkeeping —
it doesn't remove or weaken anyone's ability to kill a hung session from
outside. There's no real trade-off to weigh here.

**What the actual tools are, mapped to before/after the commit point:**
- **Stuck before local commit** (hung in `xa_prepare`, DB2 unreachable
  during prepare): `pg_terminate_backend()` kills the backend before it ever
  reaches local commit, so no branch was ever obligated to commit —
  effectively a rollback, and already correct today with no new code.
- **Stuck after local commit, in-doubt** (branch prepared, phase-2
  `xa_commit` failed or DB2 became unreachable before it could run): still
  only one correct outcome — commit — so the tools are about forcing
  resolution *sooner*, not choosing a different outcome.
  `db2_fdw_xa_resolve_now()` (Phase 3) triggers an immediate retry pass
  against the durable log instead of waiting for the recovery worker's
  normal schedule.
- **Genuinely unresolvable** (the DB2 database itself is gone — restored
  from an older backup, dropped, migrated away — so `xa_recover` will never
  find that branch again): the one case with no correct automatic answer.
  The honest tool is not a rollback button but an explicit, loudly-logged
  override, `db2_fdw_xa_forget(gid)` (Phase 3), that removes the durable log
  entry *without* claiming to have committed or rolled back anything on the
  DB2 side — it's the admin declaring "I've reconciled this by hand outside
  the protocol, stop tracking it," accepting the risk of inconsistency as a
  documented, deliberate trade-off rather than the system silently guessing
  rollback on their behalf. DB2 itself has an equivalent last-resort tool at
  its own layer (`RESOLVE INDOUBT TRANSACTIONS`) for the same scenario.

## Open questions to settle before coding

- ~~Does the XA switch ship with the DB2 client actually in use?~~ **Resolved**
  — verified present and functional in `libdb2.so` on the DB2 11.5 client
  used in this environment (see above). Still worth a one-time check against
  whatever Instant Client build CI ultimately uses, since Instant Client
  packaging has in the past been thinner than a full server client install —
  but there is no reason to expect the XA switch to be missing there either.
- ~~Behavior at `two_phase_commit=off` with ≥2 connections?~~ **Resolved:
  error, don't silently fall back.** When the coordinator (item 1 above)
  finds ≥2 connections that *wrote* in one transaction (not merely ≥2 with
  an open remote transaction — see "Read-only branches" below) and
  `two_phase_commit=off` on any of the servers involved, it raises
  `ERROR` (e.g. `errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION)`,
  "transaction spans multiple DB2 connections; enable two_phase_commit on
  every db2_fdw server involved to commit it safely") instead of falling back
  to today's independent-commit-per-connection behavior. Rationale: silently
  keeping the known inconsistency window as a "default" would make the unsafe
  path the one every existing install ends up on with zero visibility into
  the risk. Failing loudly forces the operator to explicitly opt every
  involved server into `two_phase_commit=on` (Phase 1) before such a
  transaction can commit at all — properly configured and administered beats
  a silent assumption. This also means Phase 1 must ship the GUC/option and
  this guard *together*: there should never be a released state where ≥2
  connections silently commit independently without the operator having
  chosen that.
- ~~Connection lifecycle vs. in-flight branches?~~ **Resolved as two concrete
  fixes.** Checked against the code: `db2CloseConnections()`
  (`source/db2CloseConnections.c:51,105`) calls `SQLDisconnect` and then
  `free()`s the `DB2ConnEntry` itself — genuine deallocation, not a soft
  close — and the only thing preventing `DB2_close_connections()` (the
  SQL-callable wrapper) from doing this mid-transaction is the existing
  `dml_in_transaction` guard (`source/db2_fdw.c:370`), which is process-global
  (not per-connection) and isn't even set by the direct-modify write path
  (`db2BeginDirectModify.c`/`db2IterateDirectModify.c`) — so this gap exists
  independent of 2PC and is worth fixing regardless.
  1. **Guard fix:** `DB2_close_connections()` must refuse to close any
     connection with `xact_level > 0` (still open) or the new `wrote`/XA-
     enrolled state set (item 1 under Architecture), checked per connection
     — not the global, incomplete `dml_in_transaction` flag. This closes the
     use-after-free / silently-abandoned-branch risk from freeing a
     `DB2ConnEntry` the coordinator or its bookkeeping still references.
  2. **State-reset audit:** because db2_fdw already caches a `DB2ConnEntry`
     across many separate PostgreSQL transactions in one backend (by design
     — DB2 sessions are expensive to establish), and a connection pooler
     handing the same backend to different clients just makes this happen
     across more unrelated work, every new per-connection 2PC field
     (`wrote`, any "currently prepared"/"currently XA-enrolled" state) must
     be reset on *every* path that ends a transaction on that connection —
     normal commit, normal abort, and any error path inside the coordinator's
     own prepare/commit logic — so a later, unrelated transaction reusing the
     same cached connection never inherits stale state from a previous one.

  This does **not** resolve what happens when a connection is stuck/
  unreachable mid-2PC (a hung `xa_prepare`/`xa_commit`, a split-brain DB2
  outage) — that is a distinct question about admin intervention and the
  commit/rollback asymmetry inherent to 2PC, tracked separately below.
- ~~Background worker footprint?~~ **Resolved by the packaging split** —
  the `shared_preload_libraries` requirement and the worker itself live
  entirely in the optional `db2_fdw_xa` extension (Phase 3), so it's a cost
  only operators who install that extension take on; db2_fdw proper never
  needs it. Still open *within* `db2_fdw_xa`: a permanent
  `BgWorkerStart_PostmasterStart` worker vs. `RegisterDynamicBackgroundWorker`
  registered on demand (avoids requiring a server restart to enable, at the
  cost of not being guaranteed running immediately after a crash until
  something triggers it).
- ~~Rendezvous struct versioning policy?~~ **Resolved: a self-describing
  struct, checked by size, not just by version number.** The struct behind
  the rendezvous variable (owned by db2_fdw's own headers — see below —
  since db2_fdw has to exist independent of whether `db2_fdw_xa` is ever
  built) starts with a fixed, permanent header:

  ```c
  #define DB2_FDW_XA_ABI_MAGIC  0x44325841u   /* 'D2XA' */
  #define DB2_FDW_XA_ABI_MAJOR  1             /* bump on ANY incompatible change */
  #define DB2_FDW_XA_ABI_MINOR  0             /* bump on additive-only change */

  typedef struct DB2FdwXaAbi {
      uint32   magic;        /* == DB2_FDW_XA_ABI_MAGIC, else: not our struct */
      uint32   struct_size;  /* sizeof(*this) as built by the PROVIDER (db2_fdw_xa) */
      uint16   abi_major;    /* bumped only on a breaking change */
      uint16   abi_minor;    /* bumped on backward-compatible additions */
      /* --- major 1, minor 0 fields: fixed forever within major 1 --- */
      void (*register_branch)(void *conn_handle, ...);
      void (*commit_all)(...);
      /* later minors append new fields *here*, never earlier */
  } DB2FdwXaAbi;
  ```

  db2_fdw's lookup checks, in order: (1) `magic` matches — else not our
  struct, ignore; (2) `abi_major` matches exactly what this db2_fdw build was
  compiled against — else refuse regardless of size, since a major bump can
  mean a field was *repurposed*, not just added; (3) `struct_size >=
  DB2_FDW_XA_ABI_MIN_SIZE`, where `DB2_FDW_XA_ABI_MIN_SIZE` is computed at
  db2_fdw's own compile time as `offsetof(DB2FdwXaAbi, <last field this
  build actually calls>) + sizeof(that field)`. This third check, not the
  minor number, is what actually prevents an out-of-bounds read — the minor
  number is only for diagnostics. Net effect: a newer `db2_fdw_xa` (bigger
  struct, more trailing fields) is silently forward-compatible; an older or
  mismatched one (struct too small) is detected and cleanly disables XA
  support — same behavior as the companion not being installed at all —
  instead of reading past the end of a shorter struct. Any failed check
  falls back to "as if `db2_fdw_xa` weren't installed" (i.e. `ERROR` if
  `two_phase_commit=on` and ≥2 writing connections, unchanged behavior
  otherwise), never a silent misinterpretation.

  Maintenance policy for `db2_fdw_xa`: minor-only changes may *append* new
  fields (including new optional function pointers — callers must check
  `struct_size` covers the field, and still treat a `NULL` value as "not
  implemented" even when it's present) and must never reorder, retype, or
  change the meaning of an existing field. Anything else requires bumping
  `abi_major`. No explicit reserved-padding is needed — the `struct_size`
  mechanism already accommodates growth. This scheme only works if both
  binaries compile against a byte-identical copy of this one header, so it
  should have exactly one owner (db2_fdw's `include/`) with `db2_fdw_xa`'s
  build referencing it directly rather than vendoring a copy that could
  drift.
- ~~Read-only branches?~~ **Resolved by checking the code: `xact_level` is
  already set for read-only connections, so it cannot be used to decide 2PC
  participation.** `db2GetSession()` (`source/db2GetSession.c:44-46`) sets
  `xact_level = 1` unconditionally the first time a connection is obtained,
  and `db2BeginForeignScan.c` calls it for plain read-only `SELECT`s exactly
  like the write paths do — so `xact_level > 0` means "has an open remote
  transaction," not "wrote something." Using it directly, as earlier drafts
  of this document did, would have made two ordinary read-only cross-server
  `SELECT`s look like 2 writing participants and either `ERROR` or trigger
  full 2PC for a transaction with nothing to protect. The existing
  `dml_in_transaction` flag doesn't fix this either: it's process-global
  (not per-connection) and isn't set by the direct-modify path
  (`db2BeginDirectModify.c`/`db2IterateDirectModify.c`) at all. Fix adopted
  in the Architecture section above: a new per-connection `wrote` flag on
  `DB2ConnEntry`, set at every real write site, is what the coordinator
  counts — read-only connections never enter the XA path at all (not even
  via an `xa_prepare` that comes back `XA_RDONLY`), so they cost nothing
  extra regardless of how many DB2 servers a transaction reads from.
