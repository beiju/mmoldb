use crate::ingest_players::PlayerIngestFromVersions;
use crate::ingest_teams::TeamIngestFromVersions;
use crate::{EntityIngestKind, IngestFatalError, Stage2Ingest, VersionedIngestKind};
use mmoldb_db::ConnectionPool;
use mmoldb_db::db::{refresh_game_matviews, refresh_player_matviews};
use std::num::NonZero;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use crate::ingest_time::TimeIngestFromVersions;
use crate::partitioner::Partitioner;

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
pub async fn process_entity_kind(
    kind: EntityIngestKind,
    args: ProcessingArgs,
) -> Result<(), IngestFatalError> {
    match kind {
        EntityIngestKind::Game => {
            // TODO Refactor this code to get rid of remnants of the old staged system
            crate::ingest_games::ingest_stage_2(args.pool.clone(), args.shutdown_requested).await?;
            info!("game process iteration finished. Refreshing game matviews.");
            // TODO Don't hard-code this
            match args.pool.get() {
                Ok(mut conn) => {
                    for err in refresh_game_matviews(&mut conn) {
                        warn!("Error updating game matview: {}", err);
                    }
                }
                Err(err) => {
                    warn!(
                "Couldn't get database connection to update game matviews: {}",
                err
            );
                }
            }
        }
    }

    Ok(())
}

// It may be possible to remove 'static
pub async fn process_version_kind(
    kind: VersionedIngestKind,
    args: ProcessingArgs,
) -> Result<(), IngestFatalError> {
    // TODO Refactor this to not match on kind
    match kind {
        VersionedIngestKind::Time => {
            // TODO Refactor this code to get rid of remnants of the old staged system
            let stage = Arc::new(Stage2Ingest::new(kind.as_kind(), TimeIngestFromVersions));
            let partitioner = Partitioner::with_single_partition();
            stage.run(args, partitioner).await
        }
        VersionedIngestKind::Player => {
            let pool_for_matviews = args.pool.clone();
            // TODO Refactor this code to get rid of remnants of the old staged system
            let stage = Arc::new(Stage2Ingest::new(kind.as_kind(), PlayerIngestFromVersions));
            let partitioner = Partitioner::with_partitions(args.parallelism);
            stage.run(args, partitioner).await?;
            info!("Player process iteration finished. Refreshing player matviews.");
            // TODO Don't hard-code this
            match pool_for_matviews.get() {
                Ok(mut conn) => {
                    for err in refresh_player_matviews(&mut conn) {
                        warn!("Error updating player matview: {}", err);
                    }
                }
                Err(err) => {
                    warn!(
                        "Couldn't get database connection to update player matviews: {}",
                        err
                    );
                }
            }
            Ok(())
        }
        VersionedIngestKind::Team => {
            // TODO Refactor this code to get rid of remnants of the old staged system
            let stage = Arc::new(Stage2Ingest::new(kind.as_kind(), TeamIngestFromVersions));
            let partitioner = Partitioner::with_partitions(args.parallelism);
            stage.run(args, partitioner).await
        }
    }
}
