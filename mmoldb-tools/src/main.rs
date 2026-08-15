pub mod tables;

use clap::{Parser, Subcommand};
use crate::tables::{CheckTablesError, KindStyle};

#[derive(Subcommand)]
#[command(version, about, long_about = None)]
enum Commands {
    /// Output the SQL command to delete all derived data for all time
    DeleteDerived,
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

    println!("begin;");
    println!("truncate table");

    // Emit the truncate
    let tables = tables::tables();

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