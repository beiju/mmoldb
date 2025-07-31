mod check_round_trip;
mod config;
mod sim;
mod worker;

pub use worker::*;

use log::error;
use miette::Diagnostic;
use mmoldb_db::QueryError;
use thiserror::Error;

#[derive(Debug, Error, Diagnostic)]
pub enum IngestFatalError {
    #[error(transparent)]
    DeserializeError(#[from] serde_json::Error),

    #[error(transparent)]
    DbError(#[from] QueryError),

    #[error(transparent)]
    JoinError(#[from] tokio::task::JoinError),
}

pub struct IngestStats {
    pub num_ongoing_games_skipped: usize,
    pub num_bugged_games_skipped: usize,
    pub num_games_with_fatal_errors: usize,
    pub num_games_imported: usize,
}
