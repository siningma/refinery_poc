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
    },
    /// Drop users and refinery_schema_history so migrations can be re-run from clean state
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

fn cmd_migrate() -> Result<(), AppError> {
    let mut conn = connect()?;
    let report = embedded::migrations::runner().run(&mut conn)?;
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

fn cmd_race(threads: usize) {
    info!("racing {threads} concurrent runners against the same database...");

    // Barrier releases all threads at once, so every runner hits
    // get_unapplied_migrations() -> apply in the same narrow window,
    // regardless of how long each thread took to connect.
    let barrier = Arc::new(Barrier::new(threads));

    let handles: Vec<_> = (0..threads)
        .map(|i| {
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                let conn_result = connect();
                barrier.wait();
                let result = conn_result.and_then(|mut conn| {
                    embedded::migrations::runner()
                        .run(&mut conn)
                        .map_err(AppError::from)
                });
                (i, result)
            })
        })
        .collect();

    let mut ok_count = 0;
    let mut err_count = 0;

    for handle in handles {
        let (i, result) = handle.join().expect("thread panicked");
        match result {
            Ok(report) => {
                ok_count += 1;
                info!(
                    "[thread {i}] Ok: applied {} migration(s)",
                    report.applied_migrations().len()
                );
            }
            Err(e) => {
                err_count += 1;
                error!("[thread {i}] Err:");
                log_error_chain(&e);
            }
        }
    }

    info!("tally: {ok_count} succeeded, {err_count} failed (out of {threads} runners)");
}

fn cmd_reset() -> Result<(), AppError> {
    let mut conn = connect()?;
    conn.batch_execute("DROP TABLE IF EXISTS users, refinery_schema_history;")?;
    info!("dropped users and refinery_schema_history (if they existed)");
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
        Command::Race { threads } => {
            cmd_race(threads);
            Ok(())
        }
        Command::Reset => cmd_reset(),
        Command::Status => cmd_status(),
    };

    if let Err(e) = result {
        error!("fatal:");
        log_error_chain(&e);
        std::process::exit(1);
    }
}
