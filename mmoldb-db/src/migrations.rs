use crate::taxa::Taxa;
use crate::QueryError;
use diesel::{Connection, ConnectionError, PgConnection, RunQueryDsl};
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};
use std::error::Error;
use diesel::sql_types::BigInt;
use log::{info, warn};
use miette::Diagnostic;
use thiserror::Error;

const MIGRATIONS: EmbeddedMigrations = embed_migrations!("../migrations");
const MIGRATION_LOCK_ID: i64 = 42416;

#[derive(Debug, Error, Diagnostic)]
pub enum MigrationError {
    #[error("couldn't connect to database")]
    FailedToConnectToDatabase(#[source] ConnectionError),

    #[error("error acquiring migrations lock")]
    FailedToAcquireMigrationsLock(#[source] QueryError),

    #[error("error running migrations")]
    FailedToRunMigrations(#[source] Box<dyn Error + Send + Sync>),

    #[error("error creating taxa")]
    FailedToCreateTaxa(#[source] QueryError),
}

pub fn run_migrations() -> Result<Taxa, MigrationError> {
    let url = crate::postgres_url_from_environment();

    let mut conn = PgConnection::establish(&url)
        .map_err(MigrationError::FailedToConnectToDatabase)?;

    info!("Acquiring migrations lock");
    diesel::sql_query("select pg_advisory_lock($1);")
        .bind::<BigInt, _>(MIGRATION_LOCK_ID)
        .execute(&mut conn)
        .map_err(MigrationError::FailedToAcquireMigrationsLock)?;

    info!("Running any pending migrations");
    conn.run_pending_migrations(MIGRATIONS)
        .map_err(MigrationError::FailedToRunMigrations)?;

    info!("Ensuring taxa is up to date");
    let taxa = Taxa::new(&mut conn)
        .map_err(MigrationError::FailedToCreateTaxa)?;

    let unlock_result = diesel::sql_query("select pg_advisory_unlock($1);")
        .bind::<BigInt, _>(MIGRATION_LOCK_ID)
        .execute(&mut conn);

    if let Err(e) = unlock_result {
        warn!(
            "Failed to unlock migrations lock. It will be unlocked when the connection is dropped, \
            but this still may be indicative of an application issue. Error: {:?}",
            e,
        );
    }

    info!("Migrations finished");
    Ok(taxa)
}