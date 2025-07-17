mod check_round_trip;
mod sim;
mod worker;
mod config;

pub use config::*;
pub use worker::*;

use log::error;
use miette::Diagnostic;
use mmoldb_db::QueryError;
use thiserror::Error;

use chron::ChronStreamError;


#[derive(Debug, Error, Diagnostic)]
pub enum IngestSetupError {
    #[error("Database error during ingest setup: {0}")]
    DbSetupError(#[from] QueryError),

    #[error("Couldn't get a database connection")]
    CouldNotGetConnection,

    #[error("Ingest task transitioned away from NotStarted before liftoff")]
    LeftNotStartedTooEarly,
}

#[derive(Debug, Error, Diagnostic)]
pub enum IngestFatalError {
    #[error("couldn't deserialize game")]
    DeserializeError(#[from] serde_json::Error),

    #[error(transparent)]
    ChronError(#[from] ChronStreamError),

    #[error(transparent)]
    DbError(#[from] QueryError),

    #[error(transparent)]
    JoinError(#[from] tokio::task::JoinError),
}

pub struct IngestStats {
    pub num_ongoing_games_skipped: usize,
    pub num_terminal_incomplete_games_skipped: usize,
    pub num_already_ingested_games_skipped: usize,
    pub num_games_with_fatal_errors: usize,
    pub num_games_imported: usize,
}

impl IngestStats {
    pub fn new() -> Self {
        Self {
            num_ongoing_games_skipped: 0,
            num_terminal_incomplete_games_skipped: 0,
            num_already_ingested_games_skipped: 0,
            num_games_with_fatal_errors: 0,
            num_games_imported: 0,
        }
    }

    pub fn add(&mut self, other: &IngestStats) {
        self.num_ongoing_games_skipped += other.num_ongoing_games_skipped;
        self.num_terminal_incomplete_games_skipped += other.num_terminal_incomplete_games_skipped;
        self.num_already_ingested_games_skipped += other.num_already_ingested_games_skipped;
        self.num_games_with_fatal_errors += other.num_games_with_fatal_errors;
        self.num_games_imported += other.num_games_imported;
    }
}
