mod logic;

pub use logic::day_to_db;

use mmoldb_db::{db, ConnectionPool};
use tokio_util::sync::CancellationToken;
use crate::config::IngestibleConfig;

// I made this a constant because I'm constant-ly terrified of typoing
// it and introducing a difficult-to-find bug
const PLAYER_KIND: &'static str = "player";

pub async fn ingest_players(
    ingest_id: i64,
    pool: ConnectionPool,
    abort: CancellationToken,
    config: &IngestibleConfig,
) -> miette::Result<()> {
    crate::ingest::ingest(
        ingest_id,
        PLAYER_KIND,
        config,
        pool,
        abort,
        |version| match version {
            serde_json::Value::Object(obj) => obj
                .iter()
                .filter(|(k, _)| *k != "Stats" && *k != "SeasonStats")
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            other => other.clone(),
        },
        db::get_player_ingest_start_cursor,
        logic::ingest_page_of_players,
    )
    .await
}
