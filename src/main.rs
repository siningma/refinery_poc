use clap::{Parser, Subcommand};
use postgres::{Client, NoTls};
use std::sync::{Arc, Barrier};
use std::thread;
use thiserror::Error;
use tracing::{error, info};

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("./migrations");
}

#[derive(Debug, Error)]
enum AppError {
    #[error("database error")]
    Database(#[from] postgres::Error),
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
    /// Drop users, refinery_schema_history and migration_lock for a clean slate
    Reset,
    /// Show refinery_schema_history rows and users columns
    Status,
}

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://siningma@localhost/refinery_poc".to_string())
}

fn connect() -> Result<Client, AppError> {
    Ok(Client::connect(&database_url(), NoTls)?)
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

fn run_migrations(conn: &mut Client) -> Result<refinery::Report, AppError> {
    Ok(embedded::migrations::runner().run(conn)?)
}

/// Create the sentinel lock table and its single row.
///
/// This deliberately runs ONCE from the main thread before any runners start: bootstrapping
/// the lock table is itself a `CREATE TABLE` that would hit the same catalog race the
/// unlocked demo exposes. In a real deployment this is infrastructure that has to exist
/// before the services that depend on it.
fn ensure_lock_table() -> Result<(), AppError> {
    let mut conn = connect()?;
    conn.batch_execute(
        "CREATE TABLE IF NOT EXISTS migration_lock (id INT PRIMARY KEY); \
         INSERT INTO migration_lock (id) VALUES (1) ON CONFLICT DO NOTHING;",
    )?;
    Ok(())
}

/// Take the row lock on the sentinel row, run migrations while holding it, then release.
///
/// `SELECT ... FOR UPDATE` blocks any other transaction trying to lock the same row until
/// this transaction commits — ordinary row-level locking, no advisory locks involved.
/// Migrations run on a *separate* connection because refinery opens its own transactions,
/// which cannot nest inside the one holding the lock.
fn run_migrations_locked(
    conn: &mut Client,
    lock_conn: &mut Client,
) -> Result<refinery::Report, AppError> {
    let mut tx = lock_conn.transaction()?;
    // Blocks here until whichever runner currently holds the row commits.
    tx.execute("SELECT id FROM migration_lock WHERE id = 1 FOR UPDATE", &[])?;

    let report = run_migrations(conn);

    // Release the lock regardless of whether the migration itself succeeded.
    tx.commit()?;
    report
}

fn cmd_migrate() -> Result<(), AppError> {
    let mut conn = connect()?;
    let report = run_migrations(&mut conn)?;
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

fn cmd_race(threads: usize, lock: bool) -> Result<(), AppError> {
    if lock {
        ensure_lock_table()?;
        info!("racing {threads} runners serialized behind a DB row lock...");
    } else {
        info!("racing {threads} concurrent runners against the same database...");
    }

    // Barrier releases all threads at once, so every runner hits
    // get_unapplied_migrations() -> apply in the same narrow window,
    // regardless of how long each thread took to connect.
    let barrier = Arc::new(Barrier::new(threads));

    let handles: Vec<_> = (0..threads)
        .map(|i| {
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                // Connect before the barrier so connection setup is not part of the race.
                let prepared = (|| -> Result<(Client, Option<Client>), AppError> {
                    let conn = connect()?;
                    let lock_conn = if lock { Some(connect()?) } else { None };
                    Ok((conn, lock_conn))
                })();

                barrier.wait();

                let result = prepared.and_then(|(mut conn, lock_conn)| match lock_conn {
                    Some(mut lock_conn) => run_migrations_locked(&mut conn, &mut lock_conn),
                    None => run_migrations(&mut conn),
                });
                (i, result)
            })
        })
        .collect();

    let mut applied_count = 0;
    let mut noop_count = 0;
    let mut err_count = 0;

    for handle in handles {
        let (i, result) = handle.join().expect("thread panicked");
        match result {
            Ok(report) => {
                let applied = report.applied_migrations().len();
                if applied == 0 {
                    noop_count += 1;
                    info!("[thread {i}] Ok: no-op, migrations already applied by another runner");
                } else {
                    applied_count += 1;
                    info!("[thread {i}] Ok: applied {applied} migration(s)");
                }
            }
            Err(e) => {
                err_count += 1;
                error!("[thread {i}] Err:");
                log_error_chain(&e);
            }
        }
    }

    info!(
        "tally: {applied_count} applied, {noop_count} no-op, {err_count} failed \
         (out of {threads} runners)"
    );
    Ok(())
}

fn cmd_reset() -> Result<(), AppError> {
    let mut conn = connect()?;
    conn.batch_execute("DROP TABLE IF EXISTS users, refinery_schema_history, migration_lock;")?;
    info!("dropped users, refinery_schema_history and migration_lock (if they existed)");
    Ok(())
}

fn cmd_status() -> Result<(), AppError> {
    let mut conn = connect()?;

    info!("-- refinery_schema_history --");
    match conn.query(
        "SELECT version, name, applied_on FROM refinery_schema_history ORDER BY version",
        &[],
    ) {
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
    match conn.query(
        "SELECT column_name, data_type FROM information_schema.columns \
         WHERE table_name = 'users' ORDER BY ordinal_position",
        &[],
    ) {
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

fn main() {
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
        Command::Migrate => cmd_migrate(),
        Command::Race { threads, lock } => cmd_race(threads, lock),
        Command::Reset => cmd_reset(),
        Command::Status => cmd_status(),
    };

    if let Err(e) = result {
        error!("fatal:");
        log_error_chain(&e);
        std::process::exit(1);
    }
}
