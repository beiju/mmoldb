mod logic;

use mmoldb_db::db;
use tokio_util::sync::CancellationToken;

// I made this a constant because I'm constant-ly terrified of typoing
// it and introducing a difficult-to-find bug
const PLAYER_KIND: &'static str = "player";
const CHRON_FETCH_PAGE_SIZE: usize = 1000;
const RAW_PLAYER_INSERT_BATCH_SIZE: usize = 1000;
const PROCESS_PLAYER_BATCH_SIZE: usize = 100000;

pub async fn ingest_players(pg_url: String, abort: CancellationToken) -> miette::Result<()> {
    crate::ingest::ingest(
        PLAYER_KIND,
        CHRON_FETCH_PAGE_SIZE,
        RAW_PLAYER_INSERT_BATCH_SIZE,
        PROCESS_PLAYER_BATCH_SIZE,
        pg_url,
        abort,
        db::get_player_ingest_start_cursor,
        logic::ingest_page_of_players,
    ).await
}
