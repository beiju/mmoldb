mod logic;

pub use logic::day_to_db;

use mmoldb_db::db;
use tokio_util::sync::CancellationToken;

// I made this a constant because I'm constant-ly terrified of typoing
// it and introducing a difficult-to-find bug
const PLAYER_KIND: &'static str = "player";
const CHRON_FETCH_PAGE_SIZE: usize = 1000;
const RAW_PLAYER_INSERT_BATCH_SIZE: usize = 1000;
const PROCESS_PLAYER_BATCH_SIZE: usize = 50000;

pub async fn ingest_players(ingest_id: i64, pg_url: String, abort: CancellationToken) -> miette::Result<()> {
    crate::ingest::ingest(
        ingest_id,
        PLAYER_KIND,
        CHRON_FETCH_PAGE_SIZE,
        RAW_PLAYER_INSERT_BATCH_SIZE,
        PROCESS_PLAYER_BATCH_SIZE,
        pg_url,
        abort,
        |version| match version {
            serde_json::Value::Object(obj) => {
                obj.iter()
                    .filter(|(k, _)| *k != "Stats" && *k != "SeasonStats")
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            }
            other => other.clone(),
        },
        db::get_player_ingest_start_cursor,
        logic::ingest_page_of_players,
    ).await
}
