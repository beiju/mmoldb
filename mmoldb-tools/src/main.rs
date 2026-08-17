pub mod tables;

use clap::{Parser, Subcommand};
use crate::tables::{CheckTablesError, KindStyle};

#[derive(Subcommand)]
#[command(version, about, long_about = None)]
enum Commands {
    /// Output the SQL command to delete all derived data for all time
    DeleteDerived,

    /// Output the SQL command to delete all derived data after the given time,
    /// for the given kind or all kinds if not provided
    DeleteDerivedAfter {
        after: String,
        for_kind: Option<String>,
    },
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

fn main() -> Result<(), CheckTablesError> {
    let cli = Cli::parse();

    tables::check_tables()
        .map_err(|err| {
            eprintln!("Check tables failed: {}", err);
            err
        })?;

    match &cli.command {
        Commands::DeleteDerived => {
            gen_delete_derived();
        }
        Commands::DeleteDerivedAfter { after, for_kind } => {
            gen_delete_derived_after(after, for_kind.as_deref());
        }
    }

    Ok(())
}

fn gen_delete_derived() {
    let mut is_first = true;
    let mut emit_table = move |table_name| {
        if is_first {
            is_first = false;
            println!("\tdata.{}", table_name);
        } else {
            println!("\t, data.{}", table_name);
        }
    };

    // Emit the truncate
    let tables = tables::tables();

    println!("begin;");
    println!("truncate table");

    println!("\t-- *_processed tables");
    for table in &tables.origin_tables {
        if let Some(processed) = table.processed {
            emit_table(processed);
        }
    }

    for (kind, table) in &tables.derived_tables {
        match table {
            KindStyle::Game { root_table, child_tables, auxiliary_tables, materialized_views: _ } => {
                println!("\t-- base table for kind={kind}");
                emit_table(root_table);

                if !child_tables.is_empty() {
                    println!("\t-- child tables for kind={kind}");
                    for child_table in child_tables {
                        emit_table(child_table.table);
                    }
                }

                if !auxiliary_tables.is_empty() {
                    println!("\t-- auxiliary tables for kind={kind}");
                    for auxiliary_table in auxiliary_tables {
                        emit_table(auxiliary_table);
                    }
                }
            }
            KindStyle::Version { version_derived_tables, auxiliary_tables, feed_derived_tables } => {
                if !version_derived_tables.is_empty() {
                    println!("\t-- version-derived tables for kind={kind}");
                    for version_derived_table in version_derived_tables {
                        emit_table(version_derived_table);
                    }
                }

                if !auxiliary_tables.is_empty() {
                    println!("\t-- auxiliary tables for kind={kind}");
                    for auxiliary_table in auxiliary_tables {
                        emit_table(auxiliary_table);
                    }
                }

                if !feed_derived_tables.is_empty() {
                    println!("\t-- feed-derived tables for kind={kind}");
                    for feed_derived_table in feed_derived_tables {
                        emit_table(feed_derived_table);
                    }
                }
            }
        }
    }
    println!("\t;");

    // Refresh any matviews
    for (kind, table) in &tables.derived_tables {
        match table {
            KindStyle::Game { root_table: _, child_tables: _, auxiliary_tables: _, materialized_views } => {
                println!("-- refresh materialized views for kind={kind}");
                for materialized_view in materialized_views {
                    println!("refresh materialized view data.{materialized_view};");
                }

            }
            KindStyle::Version { version_derived_tables: _, auxiliary_tables: _, feed_derived_tables: _ } => {}
        }
    }

    println!("end;");
}

fn gen_delete_derived_after(after: &str, for_kind: Option<&str>) {
    let tables = tables::tables();

    println!("begin;");

    for (kind, table) in &tables.derived_tables {
        if for_kind.is_some_and(|for_kind| for_kind != *kind) {
            println!("\t-- skipping non-selected kind {kind}");
            continue;
        }

        match table {
            KindStyle::Game { root_table, .. } => {
                // Game-style tables only need the root table to be deleted. Everything else
                // has `on delete cascade`.
                println!("\t-- deleting from game-style table data.{root_table}");
                println!("\tdelete from data.{root_table} where valid_from >= '{after}';");
            }
            KindStyle::Version { version_derived_tables, auxiliary_tables, feed_derived_tables } => {
                println!("\t-- deleting from version-style table data.{kind}");

                if !version_derived_tables.is_empty() {
                    println!("\t-- deleting version-derived tables for kind={kind}");
                    for version_derived_table in version_derived_tables {
                        println!("\tdelete from data.{version_derived_table} where valid_from >= '{after}';");
                        println!("\tupdate data.{version_derived_table} set valid_until=null where valid_until >= '{after}';");
                    }
                }

                // No need to delete from auxiliary tables (and no way to do it either)
                let _ = auxiliary_tables;

                // I decided not to lump in feed delete with version delete. Feed delete is not yet
                // implemented
                let _ = feed_derived_tables;

                // TODO Add an optimization to collapse multiple of these if for_kind is None.
                //   Note: Don't delete feed_ingest_log unless feed version delete is implemented
                println!("\tdelete from data.versions_processed where kind = '{kind}' and valid_from >= '{after}';");
                println!("\tdelete from info.version_ingest_log where kind = '{kind}' and valid_from >= '{after}';");
            }
        }
    }

    println!();
    println!("\trefresh materialized view info.entities_count;");
    println!("\trefresh materialized view info.entities_with_issues_count;");

    println!("end;");
}