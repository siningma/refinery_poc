use postgres::{Client, NoTls};
use std::error::Error as StdError;
use std::sync::{Arc, Barrier};
use std::thread;

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("./migrations");
}

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://siningma@localhost/refinery_poc".to_string())
}

fn connect() -> Client {
    Client::connect(&database_url(), NoTls).expect("failed to connect to postgres")
}

/// Walk the full `source()` chain of an error so the underlying SQLSTATE / message
/// (buried a few layers below refinery's own Error type) is visible.
fn print_error_chain(err: &(dyn StdError + 'static)) {
    eprintln!("    error: {err}");
    let mut source = err.source();
    let mut depth = 1;
    while let Some(cause) = source {
        eprintln!("    {}caused by: {cause}", "  ".repeat(depth));
        source = cause.source();
        depth += 1;
    }
}

fn cmd_migrate() {
    let mut conn = connect();
    match embedded::migrations::runner().run(&mut conn) {
        Ok(report) => {
            let applied = report.applied_migrations();
            if applied.is_empty() {
                println!("no migrations applied (already up to date)");
            } else {
                println!("applied {} migration(s):", applied.len());
                for m in applied {
                    println!("  {m}");
                }
            }
        }
        Err(e) => {
            println!("migration failed:");
            print_error_chain(&e);
        }
    }
}

fn cmd_race(threads: usize) {
    println!("racing {threads} concurrent runners against the same database...\n");

    // Barrier releases all threads at once, so every runner hits
    // get_unapplied_migrations() -> apply in the same narrow window,
    // regardless of how long each thread took to connect.
    let barrier = Arc::new(Barrier::new(threads));

    let handles: Vec<_> = (0..threads)
        .map(|i| {
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                let mut conn = connect();
                barrier.wait();
                let result = embedded::migrations::runner().run(&mut conn);
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
                println!(
                    "[thread {i}] Ok: applied {} migration(s)",
                    report.applied_migrations().len()
                );
            }
            Err(e) => {
                err_count += 1;
                println!("[thread {i}] Err:");
                print_error_chain(&e);
            }
        }
    }

    println!("\ntally: {ok_count} succeeded, {err_count} failed (out of {threads} runners)");
}

fn cmd_reset() {
    let mut conn = connect();
    conn.batch_execute("DROP TABLE IF EXISTS users, refinery_schema_history;")
        .expect("failed to reset tables");
    println!("dropped users and refinery_schema_history (if they existed)");
}

fn cmd_status() {
    let mut conn = connect();

    println!("-- refinery_schema_history --");
    match conn.query(
        "SELECT version, name, applied_on FROM refinery_schema_history ORDER BY version",
        &[],
    ) {
        Ok(rows) => {
            if rows.is_empty() {
                println!("  (no rows)");
            }
            for row in rows {
                let version: i32 = row.get(0);
                let name: String = row.get(1);
                let applied_on: String = row.get(2);
                println!("  version={version} name={name} applied_on={applied_on}");
            }
        }
        Err(e) => println!("  (table does not exist yet: {e})"),
    }

    println!("\n-- users columns --");
    match conn.query(
        "SELECT column_name, data_type FROM information_schema.columns \
         WHERE table_name = 'users' ORDER BY ordinal_position",
        &[],
    ) {
        Ok(rows) => {
            if rows.is_empty() {
                println!("  (table does not exist)");
            }
            for row in rows {
                let name: String = row.get(0);
                let data_type: String = row.get(1);
                println!("  {name}: {data_type}");
            }
        }
        Err(e) => println!("  (error querying columns: {e})"),
    }
}

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args: Vec<String> = std::env::args().collect();
    let subcommand = args.get(1).map(String::as_str).unwrap_or("migrate");

    match subcommand {
        "migrate" => cmd_migrate(),
        "race" => {
            let threads = args
                .iter()
                .position(|a| a == "--threads")
                .and_then(|i| args.get(i + 1))
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(4);
            cmd_race(threads);
        }
        "reset" => cmd_reset(),
        "status" => cmd_status(),
        other => {
            eprintln!("unknown subcommand: {other}");
            eprintln!("usage: refinery_poc [migrate|race --threads N|reset|status]");
            std::process::exit(1);
        }
    }
}
