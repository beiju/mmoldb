use std::num::NonZero;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use mmoldb_db::ConnectionPool;
use crate::{IngestFatalError, Stage2Ingest};
use crate::ingest_player_feed::PlayerFeedIngestFromVersions;
use crate::ingest_players::PlayerIngestFromVersions;
use crate::ingest_team_feed::TeamFeedIngestFromVersions;
use crate::ingest_teams::TeamIngestFromVersions;

#[derive(Debug, Clone)]
pub struct ProcessingArgs {
    pub shutdown_requested: CancellationToken,
    pub pool: ConnectionPool,
    pub enabled: bool,
    pub processing_interval_seconds: u64,
    pub parallelism: NonZero<usize>,
    pub process_batch_size: NonZero<usize>,
    pub debug_db_insert_delay: f64,
}

// It may be possible to remove 'static
pub async fn process_entity_kind(kind: &'static str, args: ProcessingArgs) -> Result<(), IngestFatalError> {
    assert_eq!(kind, "game", "`game` is the only supported entity kind");

    // TODO Refactor this code to get rid of remnants of the old staged system
    crate::ingest_games::ingest_stage_2(
        args.pool,
        args.shutdown_requested,
    ).await
}

// It may be possible to remove 'static
pub async fn process_version_kind(kind: &'static str, args: ProcessingArgs) -> Result<(), IngestFatalError> {
    // TODO Refactor this to not match on kind
    match kind {
        "player" => {
            // TODO Refactor this code to get rid of remnants of the old staged system
            let stage = Arc::new(Stage2Ingest::new(kind, PlayerIngestFromVersions));
            stage.run(args).await
        }
        "team" => {
            // TODO Refactor this code to get rid of remnants of the old staged system
            let stage = Arc::new(Stage2Ingest::new(kind, TeamIngestFromVersions));
            stage.run(args).await
        }
        _ => {
            panic!("`player` and `team` are the only supported version kinds")
        }
    }
}

// It may be possible to remove 'static
pub async fn process_feed_event_version_kind(kind: &'static str, args: ProcessingArgs) -> Result<(), IngestFatalError> {
    // TODO Refactor this to not match on kind
    match kind {
        "player_feed" => {
            // TODO Refactor this code to get rid of remnants of the old staged system
            let stage = Arc::new(Stage2Ingest::new(kind, PlayerFeedIngestFromVersions));
            stage.run(args).await
        }
        "team_feed" => {
            // TODO Refactor this code to get rid of remnants of the old staged system
            let stage = Arc::new(Stage2Ingest::new(kind, TeamFeedIngestFromVersions));
            stage.run(args).await
        }
        _ => {
            panic!("`player_feed` and `team_feed` are the only supported feed event version kinds")
        }
    }
}