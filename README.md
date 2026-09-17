# refinery POC: first migration + concurrent-runner contention

A minimal Rust project exploring the [`refinery`](https://docs.rs/refinery/latest/refinery/)
SQL migration crate (v0.9.2) against a local PostgreSQL 17 instance (via `brew`), using
`tokio-postgres` on a tokio runtime (refinery's `tokio-postgres` feature + `run_async`), and
specifically answering: **when two services run migrations concurrently, does one win and
the other cleanly no-op?**

## Setup

```bash
brew services start postgresql@17   # if not already running
createdb refinery_poc
cargo build
```

Connection string defaults to `postgres://siningma@localhost/refinery_poc`; override with
`DATABASE_URL`.

## Commands

| Command | What it does |
|---|---|
| `cargo run -- migrate` | Runs `embedded::migrations::runner().run()` once. Applies `migrations/V1__create_users_table.sql`. |
| `cargo run -- race --threads N` | Spawns N tokio tasks, each with its own `tokio_postgres::Client`, released simultaneously via a `tokio::sync::Barrier`, each calling `run_async()` against the *same fresh* schema. |
| `cargo run -- race --threads N --lock` | Same race, but every runner serializes behind a DB row lock first. One applies; the rest block, then find nothing to apply. |
| `cargo run -- reset` | Drops `users`, `refinery_schema_history` and `migration_lock` for a clean slate. |
| `cargo run -- status` | Dumps `refinery_schema_history` rows and `users` column list. |

## Result 1: the basic migration works

```
$ cargo run -q -- migrate
applied 1 migration(s):
  V1__create_users_table

$ psql -d refinery_poc -c '\d users'
   Column   |           Type           | Nullable |              Default
------------+--------------------------+----------+------------------------------------
 id         | integer                  | not null | nextval('users_id_seq'::regclass)
 email      | character varying(255)   | not null |
 created_at | timestamp with time zone | not null | now()
```

Re-running `migrate` is a clean no-op — refinery reads `refinery_schema_history`, sees
version 1 already applied, and reports 0 migrations applied.

## Result 2: concurrent runners — one wins, the rest ERROR (not no-op)

```
$ cargo run -q -- reset
$ cargo run -q -- race --threads 4

racing 4 concurrent runners against the same database...

[thread 0] Ok: applied 1 migration(s)
[thread 1] Err:
    error: `error asserting migrations table`, `db error`
      caused by: db error
        caused by: ERROR: duplicate key value violates unique constraint "pg_type_typname_nsp_index"
DETAIL: Key (typname, typnamespace)=(refinery_schema_history, 2200) already exists.
[thread 2] Err: (same)
[thread 3] Err: (same)

tally: 1 succeeded, 3 failed (out of 4 runners)
```

Reproduced consistently across multiple runs (4 threads and 8 threads), always exactly one
`Ok`, the rest `Err`. Final state is correct — `users` has 3 columns, `refinery_schema_history`
has exactly 1 row — but that correctness is a side effect of the losers crashing, not of any
coordination refinery provides.

### Why: refinery takes zero locks

Reading `refinery_core` 0.9.2 source (`traits/mod.rs`, `traits/sync.rs`, `traits/async.rs`,
`runner.rs`, `drivers/postgres.rs`) — there are no advisory locks, mutexes, or `SELECT ... FOR
UPDATE` anywhere in the migrate path. `Migrate::migrate()` does, in order:

1. `assert_migrations_table` — `CREATE TABLE IF NOT EXISTS refinery_schema_history(...)`,
   in its own transaction, committed immediately.
2. `get_applied_migrations` — `SELECT ...`, its own transaction, committed and released.
3. Diff applied vs. embedded migrations, in Rust memory (no DB involvement).
4. Apply each pending migration — in *default* (non-grouped) mode, this is **two more
   separate transactions per migration**: one for the migration SQL, one for the
   `INSERT INTO refinery_schema_history` bookkeeping row.

Every step commits and releases before the next one starts. So N concurrent runners can all
pass step 1 (or race inside it), all see an empty history at step 2, and all conclude V1 is
pending at step 3.

What actually stops double-application isn't refinery — it's Postgres's own catalog
uniqueness. In this run, all four runners collided on `CREATE TABLE IF NOT EXISTS
refinery_schema_history` itself: Postgres's `IF NOT EXISTS` is not concurrency-safe against a
simultaneous `CREATE TABLE` of the same name, so three threads got `pg_type_typname_nsp_index`
unique-violations before ever reaching the `users` table. If the race window instead landed
past that point, the collision would show up on `users` already existing, or on the
`refinery_schema_history` primary key (`version`). Which one you get is nondeterministic —
it depends on exactly where the interleaving lands.

**Answer to the original question:** no — it is not "one wins, one no-ops." One wins:
`run()` returns `Ok`. The rest get a hard `Err` from `run()`, surfaced as a Postgres unique-
violation several layers under refinery's own `Error` type. A caller that doesn't handle this
explicitly will see failed deploys/migrations on every service except the one that happened to
win the race — not a benign skip.

## Result 3: `--lock` gives the behavior you actually want

`--lock` serializes runners behind an ordinary **row lock** — no advisory locks involved:

```sql
-- bootstrapped once, before any runner starts
CREATE TABLE IF NOT EXISTS migration_lock (id INT PRIMARY KEY);
INSERT INTO migration_lock (id) VALUES (1) ON CONFLICT DO NOTHING;

-- each runner, in its own transaction, on a dedicated connection
BEGIN;
SELECT id FROM migration_lock WHERE id = 1 FOR UPDATE;   -- blocks until holder commits
--   ... run refinery migrations on a SECOND connection ...
COMMIT;                                                   -- releases
```

```
$ cargo run -q -- reset
$ cargo run -q -- race --threads 4 --lock

racing 4 runners serialized behind a DB row lock...
[thread 1] Ok: applied 1 migration(s)
[thread 0] Ok: no-op, migrations already applied by another runner
[thread 2] Ok: no-op, migrations already applied by another runner
[thread 3] Ok: no-op, migrations already applied by another runner

tally: 1 applied, 3 no-op, 0 failed (out of 4 runners)
```

**1 applied, N-1 no-op, 0 failed** — reproduced across repeated runs at 4 and 8 threads. This
is the "one wins, the rest cleanly skip" semantics refinery does not give you on its own: the
losers no longer error, they wait their turn, re-read `refinery_schema_history`, see V1 already
applied, and return `Ok` with 0 migrations.

### Why it works

`SELECT ... FOR UPDATE` takes a row-level exclusive lock that any competing transaction must
wait on until the holder commits. Because the winner's `COMMIT` happens *after* its migrations
are committed, every runner that wakes up afterwards observes the completed history table — so
its diff comes back empty. Standard SQL row locking, portable, no `pg_advisory_lock`.

Two implementation details that matter:

- **Migrations run on a second connection.** refinery opens its own transactions internally,
  which cannot nest inside the transaction holding the lock — so each runner uses one
  connection for the lock and one for refinery. (Each `tokio_postgres::connect` also spawns
  a task to drive its `Connection` future, which must be polled for the client to work.)
- **The lock table is bootstrapped once before any runner starts.**
  Creating it concurrently would hit the very same `CREATE TABLE` catalog race that Result 2
  demonstrates. In a real deployment this table is infrastructure that must pre-exist the
  services depending on it (e.g. created by a bootstrap job, not by the racing services).

Blocking was verified directly rather than inferred: holding the sentinel row in a separate
`psql` transaction with `pg_sleep(5)` makes `race --lock` take ~4s instead of milliseconds,
then complete with `1 applied, 1 no-op, 0 failed`.

### Caveats / follow-ups

- **Non-atomicity**: default mode applies DDL and the history-row INSERT in *separate*
  transactions. A crash between them leaves schema changed but unrecorded — the next run
  retries and fails permanently on "relation already exists." `set_grouped(true)` batches
  all pending migrations into one transaction, which narrows but does not eliminate this
  (the initial `assert_migrations_table` step is still separate).
- **Mitigation**: implemented as `--lock` (see Result 3), using a row lock rather than a
  `pg_advisory_lock`. An advisory lock would also work and needs no sentinel table, but was
  deliberately avoided here.
- This analysis is Postgres-specific. MySQL is worse for this scenario since its DDL isn't
  transactional at all.
