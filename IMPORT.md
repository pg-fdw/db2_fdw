# IMPORT FOREIGN SCHEMA and DB2 name case

How db2_fdw's `IMPORT FOREIGN SCHEMA` matches DB2 schema and table names, and how
it names the foreign tables it creates. The short reference is in
[README.md](README.md#support-for-import-foreign-schema); the behaviour is
exercised by `test/sql/tc032.sql` (name matching) and `test/sql/tc033.sql`
(option values).

DB2 stores ordinary names in upper case, but names created in double quotes
keep their case. So `FDWCASE`, `"fdwcase"` and `"FDWCase"` can be three
different schemas, and one schema can hold `ORDERS`, `"orders"` and `"Orders"`
side by side.

The examples below assume these three schemas, each holding the same five
tables:

| DB2 table | Kind of name |
|---|---|
| `ORDERS` | upper case (ordinary DB2 name) |
| `"orders"` | lower case |
| `"Orders"` | mixed case |
| `CUSTOMER` | upper case, no case twin |
| `"Items"` | mixed case, no case twin |

(The fixture of `tc032` uses `FDWCASE`, `"fdwcase"` and `"FdwCase"`; the rules
are the same.)

## 1. The options

```sql
IMPORT FOREIGN SCHEMA <remote schema>
  [ LIMIT TO (<table>, ...) | EXCEPT (<table>, ...) ]
  FROM SERVER <server> INTO <local schema>
  [ OPTIONS (case '...', readonly '...', importtype '...') ];
```

| Option | Values | Effect |
|---|---|---|
| `case` | `keep`, `lower`, `smart` (default); lower case only | How local table and column names are formed |
| `readonly` | `on/yes/true/off/no/false`, any case | Sets `readonly 'true'` on every imported table |
| `importtype` | `T` or `V`, any case | Tables only or views only; leave it out for both |
| `LIMIT TO` / `EXCEPT` | table names | Standard PostgreSQL clause; each name is resolved against the DB2 tables (section 3) |

## 2. Which DB2 schema you get

PostgreSQL lower-cases unquoted names before db2_fdw sees them. db2_fdw then uses an exact match if there is one. Otherwise it uses a case-insensitive match, but only if exactly one schema fits.

| You write | db2_fdw receives | DB2 schema used |
|---|---|---|
| `"FDWCASE"` | `FDWCASE` | `FDWCASE` |
| `"fdwcase"` or `fdwcase` | `fdwcase` | `fdwcase` |
| `"FDWCase"` | `FDWCase` | `FDWCase` |
| **`FDWCASE`** (unquoted) | `fdwcase` | **`fdwcase`**, not the upper-case one |
| `FDWCase` (unquoted) | `fdwcase` | `fdwcase` |
| `"FdwCase"` | `FdwCase` | Error: ambiguous (3 candidates) |
| `nosuch` | `nosuch` | Nothing imported, no error |

The unquoted row in bold is the trap. As soon as case variants exist, quote the schema name. If only `FDWCASE` existed, an unquoted `fdwcase` would find it through the fallback.

Every created foreign table gets the real DB2 names as options, for example `schema 'FDWCase', table 'Orders'`. Queries then always use them quoted, so the right table is hit.

## 3. `LIMIT TO` / `EXCEPT` entries

Entries use the same rule, matched against the tables of the chosen schema:

| Entry | Matches |
|---|---|
| `"ORDERS"` | `ORDERS` |
| `"Orders"` | `"Orders"` |
| `orders`, or `ORDERS` unquoted (becomes `orders`) | `"orders"` (exact match) |
| `"ORders"` | Error: ambiguous (3 candidates) |
| `customer` | `CUSTOMER` (only one case-insensitive match) |
| `items` | `"Items"` |
| `nosuch` | Ignored |

## 4. Local table names for each `case` value

| DB2 table | `keep` | `lower` | `smart` (default) |
|---|---|---|---|
| `ORDERS` | `"ORDERS"` | `orders` | `orders` |
| `"orders"` | `orders` | `orders` | `orders` |
| `"Orders"` | `"Orders"` | `orders` | `"Orders"` |
| `CUSTOMER` | `"CUSTOMER"` | `customer` | `customer` |
| `"Items"` | `"Items"` | `items` | `"Items"` |

The quotes show how you have to write the name in PostgreSQL.

Without a `case` option, `smart` is used: all-upper-case DB2 names such as
`ORDERS` or `CUSTOMER_2024` become lower case, every other name is kept
exactly. For an ordinary DB2 schema this gives plain lower-case PostgreSQL
names that need no quotes.

### What a full import of one schema does

- **`keep`:** all 5 tables are imported. No two names clash, because DB2 names are unique within a schema.
- **`smart`:** the whole import fails. `ORDERS` and `"orders"` would both become `orders`.
- **`lower`:** the whole import fails. All three orders variants would become `orders`.

With `smart` or `lower`, pick the tables with `LIMIT TO`:

```sql
-- smart: ORDERS -> orders, "Orders" stays "Orders"; "orders" is not imported
IMPORT FOREIGN SCHEMA "FDWCASE" LIMIT TO ("ORDERS", "Orders", customer, items)
  FROM SERVER sample INTO pg_upper;
```

`EXCEPT` does not help with this clash. In `EXCEPT ("ORDERS")` under `smart`, the remaining `"orders"` becomes `orders`, the same local name the excluded `ORDERS` would have had. PostgreSQL filters `EXCEPT` again on local names, so it would silently drop `"orders"` too. db2_fdw therefore stops with an error instead. Use `LIMIT TO`, or `keep`.

## 5. All three schemas together

Each schema has to go into its own local schema, because the local table names repeat. A second import into the same local schema fails with PostgreSQL's "relation already exists".

```sql
CREATE SCHEMA pg_upper; CREATE SCHEMA pg_lower; CREATE SCHEMA pg_mixed;
IMPORT FOREIGN SCHEMA "FDWCASE" FROM SERVER sample INTO pg_upper OPTIONS (case 'keep');
IMPORT FOREIGN SCHEMA "fdwcase" FROM SERVER sample INTO pg_lower OPTIONS (case 'keep');
IMPORT FOREIGN SCHEMA "FDWCase" FROM SERVER sample INTO pg_mixed OPTIONS (case 'keep');
```

Result: 15 foreign tables.

| PostgreSQL table | DB2 options |
|---|---|
| `pg_upper."ORDERS"` | `schema 'FDWCASE', table 'ORDERS'` |
| `pg_upper.orders` | `schema 'FDWCASE', table 'orders'` |
| `pg_upper."Orders"` | `schema 'FDWCASE', table 'Orders'` |
| `pg_upper."CUSTOMER"` | `schema 'FDWCASE', table 'CUSTOMER'` |
| `pg_upper."Items"` | `schema 'FDWCASE', table 'Items'` |
| `pg_lower.…` | Same five tables, `schema 'fdwcase'` |
| `pg_mixed.…` | Same five tables, `schema 'FDWCase'` |

With `smart` plus a `LIMIT TO` like the one in section 4, each local schema would instead get `orders` (from `ORDERS`), `"Orders"`, `customer` and `"Items"`.

## 6. Column names

Column names are folded by the same `case` rule, but two limits apply:

- **The DB2 column must be all upper case.** At query time db2_fdw builds the DB2 column name by upper-casing the local name, and there is no `column_name` option. A DB2 column `"Name"` imports as `"Name"` (keep/smart) or `name` (lower), but queries ask DB2 for `NAME` and fail. The table definition imports fine either way.
- **Column-name clashes aren't caught by db2_fdw.** A DB2 table with both `ID` and `"id"` gives two `id` columns under `smart`/`lower`. PostgreSQL rejects that `CREATE FOREIGN TABLE` with its own "column specified more than once" error, which aborts the import. Use `keep` in that case, and note that `"id"` still can't be queried because of the first limit.

## Rules of thumb

1. Quote the remote schema name whenever case variants exist.
2. Use `case 'keep'` for schemas with case twins. Otherwise use the default `smart` and pick tables with `LIMIT TO`.
3. Import each DB2 schema into its own local schema.
4. Only all-upper-case DB2 columns work at query time.