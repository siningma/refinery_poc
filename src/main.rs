use clap::{Parser, Subcommand};
use refinery::Target;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Barrier;
use tokio_postgres::{Client, NoTls};
use tracing::{error, info};

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("./migrations");
}

#[derive(Debug, Error)]
enum AppError {
    #[error("database error")]
    Database(#[from] tokio_postgres::Error),
    #[error("migration error")]
    Migration(#[from] refinery::Error),
}

#[derive(Parser)]
#[command(name = "refinery_poc", about = "refinery migration POC")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Run pending migrations once
    Migrate,
    /// Race N concurrent runners against the same database
    Race {
        /// Number of concurrent runners
        #[arg(long, default_value_t = 4)]
        threads: usize,
        /// Serialize runners behind a DB row lock, so only one applies migrations at a
        /// time and the rest block, then find nothing left to apply
        #[arg(long)]
        lock: bool,
    },
    /// Adopt refinery on a DB whose schema already exists: record migrations up to
    /// --version as applied WITHOUT executing their SQL
    Baseline {
        /// Highest migration version already present in the database
        #[arg(long)]
        version: i32,
    },
    /// Drop users, refinery_schema_history and migration_lock for a clean slate
    Reset,
    /// Show refinery_schema_history rows and users columns
    Status,
}

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://siningma@localhost/refinery_poc".to_string())
}

/// Open a connection and spawn its driver task.
///
/// tokio-postgres splits a connection into a `Client` (used to issue queries) and a
/// `Connection` future that performs the actual I/O — the latter must be polled for the
/// client to make progress, so it gets its own task. It resolves once the client is dropped.
async fn connect() -> Result<Client, AppError> {
    let (client, connection) = tokio_postgres::connect(&database_url(), NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("connection driver error: {e}");
        }
    });
    Ok(client)
}

/// Walk the full `source()` chain of an error so the underlying SQLSTATE / message
/// (buried a few layers below refinery's own Error type, and now below AppError too)
/// is visible.
fn log_error_chain(err: &(dyn std::error::Error + 'static)) {
    error!("error: {err}");
    let mut source = err.source();
    let mut depth = 1;
    while let Some(cause) = source {
        error!("{}caused by: {cause}", "  ".repeat(depth));
        source = cause.source();
        depth += 1;
    }
}

async fn run_migrations(conn: &mut Client) -> Result<refinery::Report, AppError> {
    Ok(embedded::migrations::runner().run_async(conn).await?)
}

/// Create the sentinel lock table and its single row.
///
/// This deliberately runs ONCE before any runner starts: bootstrapping the lock table is
/// itself a `CREATE TABLE` that would hit the same catalog race the unlocked demo exposes.
/// In a real deployment this is infrastructure that has to exist before the services that
/// depend on it.
async fn ensure_lock_table() -> Result<(), AppError> {
    let conn = connect().await?;
    conn.batch_execute(
        "CREATE TABLE IF NOT EXISTS migration_lock (id INT PRIMARY KEY); \
         INSERT INTO migration_lock (id) VALUES (1) ON CONFLICT DO NOTHING;",
    )
    .await?;
    Ok(())
}

/// Take the row lock on the sentinel row, run migrations while holding it, then release.
///
/// `SELECT ... FOR UPDATE` blocks any other transaction trying to lock the same row until
/// this transaction commits — ordinary row-level locking, no advisory locks involved.
/// Migrations run on a *separate* connection because refinery opens its own transactions,
/// which cannot nest inside the one holding the lock.
async fn run_migrations_locked(
    conn: &mut Client,
    lock_conn: &mut Client,
) -> Result<refinery::Report, AppError> {
    let tx = lock_conn.transaction().await?;
    // Blocks here until whichever runner currently holds the row commits.
    tx.execute("SELECT id FROM migration_lock WHERE id = 1 FOR UPDATE", &[])
        .await?;

    let report = run_migrations(conn).await;

    // Release the lock regardless of whether the migration itself succeeded.
    tx.commit().await?;
    report
}

async fn cmd_migrate() -> Result<(), AppError> {
    let mut conn = connect().await?;
    let report = run_migrations(&mut conn).await?;
    let applied = report.applied_migrations();
    if applied.is_empty() {
        info!("no migrations applied (already up to date)");
    } else {
        info!("applied {} migration(s):", applied.len());
        for m in applied {
            info!("  {m}");
        }
    }
    Ok(())
}

async fn cmd_race(runners: usize, lock: bool) -> Result<(), AppError> {
    if lock {
        ensure_lock_table().await?;
        info!("racing {runners} runners serialized behind a DB row lock...");
    } else {
        info!("racing {runners} concurrent runners against the same database...");
    }

    // Barrier releases all tasks at once, so every runner hits
    // get_unapplied_migrations() -> apply in the same narrow window,
    // regardless of how long each task took to connect.
    let barrier = Arc::new(Barrier::new(runners));

    let handles: Vec<_> = (0..runners)
        .map(|i| {
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                // Connect before the barrier so connection setup is not part of the race.
                let prepared = async {
                    let conn = connect().await?;
                    let lock_conn = if lock { Some(connect().await?) } else { None };
                    Ok::<_, AppError>((conn, lock_conn))
                }
                .await;

                barrier.wait().await;

                let result = match prepared {
                    Ok((mut conn, Some(mut lock_conn))) => {
                        run_migrations_locked(&mut conn, &mut lock_conn).await
                    }
                    Ok((mut conn, None)) => run_migrations(&mut conn).await,
                    Err(e) => Err(e),
                };
                (i, result)
            })
        })
        .collect();

    let mut applied_count = 0;
    let mut noop_count = 0;
    let mut err_count = 0;

    for handle in handles {
        let (i, result) = handle.await.expect("runner task panicked");
        match result {
            Ok(report) => {
                let applied = report.applied_migrations().len();
                if applied == 0 {
                    noop_count += 1;
                    info!("[runner {i}] Ok: no-op, migrations already applied by another runner");
                } else {
                    applied_count += 1;
                    info!("[runner {i}] Ok: applied {applied} migration(s)");
                }
            }
            Err(e) => {
                err_count += 1;
                error!("[runner {i}] Err:");
                log_error_chain(&e);
            }
        }
    }

    info!(
        "tally: {applied_count} applied, {noop_count} no-op, {err_count} failed \
         (out of {runners} runners)"
    );
    Ok(())
}

/// Mark every migration up to `version` as applied without running its SQL.
///
/// `Target::FakeVersion(n)` makes refinery write the `refinery_schema_history` rows (and
/// create the history table) while skipping the migration SQL itself, then stop at n. Run
/// this ONCE when adopting refinery on a database whose schema already exists; afterwards a
/// normal `run_async` applies only versions above n.
async fn cmd_baseline(version: i32) -> Result<(), AppError> {
    let mut conn = connect().await?;

    let report = embedded::migrations::runner()
        .set_target(Target::FakeVersion(version))
        .run_async(&mut conn)
        .await?;

    // NOTE: with a Fake target refinery leaves Report::applied_migrations empty even though
    // it inserted the history rows, so read the table back to show what was recorded.
    info!(
        "baselined at version {version} (report lists {} applied, which is expected to be 0 \
         for a fake target)",
        report.applied_migrations().len()
    );
    for row in conn
        .query(
            "SELECT version, name FROM refinery_schema_history ORDER BY version",
            &[],
        )
        .await?
    {
        let v: i32 = row.get(0);
        let name: String = row.get(1);
        info!("  recorded as applied: V{v}__{name}");
    }
    Ok(())
}

async fn cmd_reset() -> Result<(), AppError> {
    let conn = connect().await?;
    conn.batch_execute("DROP TABLE IF EXISTS users, refinery_schema_history, migration_lock;")
        .await?;
    info!("dropped users, refinery_schema_history and migration_lock (if they existed)");
    Ok(())
}

async fn cmd_status() -> Result<(), AppError> {
    let conn = connect().await?;

    info!("-- refinery_schema_history --");
    match conn
        .query(
            "SELECT version, name, applied_on FROM refinery_schema_history ORDER BY version",
            &[],
        )
        .await
    {
        Ok(rows) => {
            if rows.is_empty() {
                info!("  (no rows)");
            }
            for row in rows {
                let version: i32 = row.get(0);
                let name: String = row.get(1);
                let applied_on: String = row.get(2);
                info!("  version={version} name={name} applied_on={applied_on}");
            }
        }
        Err(e) => info!("  (table does not exist yet: {e})"),
    }

    info!("-- users columns --");
    match conn
        .query(
            "SELECT column_name, data_type FROM information_schema.columns \
             WHERE table_name = 'users' ORDER BY ordinal_position",
            &[],
        )
        .await
    {
        Ok(rows) => {
            if rows.is_empty() {
                info!("  (table does not exist)");
            }
            for row in rows {
                let name: String = row.get(0);
                let data_type: String = row.get(1);
                info!("  {name}: {data_type}");
            }
        }
        Err(e) => info!("  (error querying columns: {e})"),
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    // tracing-subscriber's default features bridge `log` records (which is what
    // refinery emits internally) into `tracing` automatically on init().
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    let result = match cli.command.unwrap_or(Command::Migrate) {
        Command::Migrate => cmd_migrate().await,
        Command::Race { threads, lock } => cmd_race(threads, lock).await,
        Command::Baseline { version } => cmd_baseline(version).await,
        Command::Reset => cmd_reset().await,
        Command::Status => cmd_status().await,
    };

    if let Err(e) = result {
        error!("fatal:");
        log_error_chain(&e);
        std::process::exit(1);
    }
}
