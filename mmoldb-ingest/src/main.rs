mod ingest;

use chron::{Chron, ChronEntity};
use chrono::{DateTime, NaiveDateTime, Utc};
use futures::{StreamExt, TryStreamExt, pin_mut};
use log::{debug, info, warn};
use miette::{Diagnostic, IntoDiagnostic};
use mmoldb_db::{Connection, PgConnection, db};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use mmoldb_db::taxa::Taxa;
use crate::ingest::{ingest_page_of_games, IngestConfig};

const CHRON_FETCH_PAGE_SIZE: usize = 1000;
const RAW_GAME_INSERT_BATCH_SIZE: usize = 1000;
const PROCESS_GAME_BATCH_SIZE: usize = 1000;

#[derive(Debug, Error, Diagnostic)]
#[error(transparent)]
struct BoxedError(#[from] Box<dyn std::error::Error + Send + Sync + 'static>);

#[tokio::main]
async fn main() -> miette::Result<()> {
    env_logger::init();

    let config = IngestConfig::default();

    let url = mmoldb_db::postgres_url_from_environment();
    let notify = Arc::new(Notify::new());
    let finish = CancellationToken::new();

    let mut conn = PgConnection::establish(&url).into_diagnostic()?;
    let ingest_id = db::start_ingest(&mut conn, Utc::now()).into_diagnostic()?;

    let process_games_handle = tokio::spawn({
        let url = url.clone();
        let notify = notify.clone();
        let finish = finish.clone();
        async move { process_games(&url, config, ingest_id, notify, finish).await }
    });

    info!("Launched process games task");
    info!("Beginning raw game ingest");

    ingest_raw_games(conn, notify).await?;

    info!("Raw game ingest finished. Waiting for process games task.");

    process_games_handle.await.into_diagnostic()??;

    Ok(())
}

async fn ingest_raw_games(mut conn: PgConnection, notify: Arc<Notify>) -> miette::Result<()> {

    let start_date = db::get_latest_entity_valid_from(&mut conn, "game")
        .into_diagnostic()?
        .as_ref()
        .map(NaiveDateTime::and_utc);

    info!("Fetch will start at {:?}", start_date);

    let chron = Chron::new(CHRON_FETCH_PAGE_SIZE);

    let stream = chron
        .entities("game", start_date)
        .try_chunks(RAW_GAME_INSERT_BATCH_SIZE);
    pin_mut!(stream);

    while let Some(chunk) = stream.next().await {
        let (chunk, maybe_err): (Vec<ChronEntity<serde_json::Value>>, _) = match chunk {
            Ok(chunk) => (chunk, None),
            Err(err) => (err.0, Some(err.1)),
        };
        info!("Saving {} games", chunk.len());
        let inserted = db::insert_entities(&mut conn, chunk).into_diagnostic()?;
        info!("Saved {} games", inserted);

        notify.notify_one();

        if let Some(err) = maybe_err {
            Err(err)?;
        }
    }

    Ok(())
}

async fn process_games(
    url: &str,
    config: IngestConfig,
    ingest_id: i64,
    notify: Arc<Notify>,
    finish: CancellationToken,
) -> miette::Result<()> {
    let mut conn = PgConnection::establish(url).into_diagnostic()?;
    let taxa = Taxa::new(&mut conn).into_diagnostic()?;

    let mut page_index = 0;
    while {
        debug!("Process games task is waiting to be woken up");
        tokio::select! {
            // When notified, keep going
            _ = notify.notified() => { true }
            // When finished, exit
            _ = finish.cancelled() => { false }
        }
    } {
        debug!("Process games task is woken up");

        // The inner loop is over batches of games to process
        loop {
            let raw_games = db::get_batch_of_unprocessed_games(&mut conn, PROCESS_GAME_BATCH_SIZE)
                .into_diagnostic()?;
            if raw_games.is_empty() {
                debug!("All games have been processed. Waiting to be woken up again.");
                break;
            }

            info!("Processing batch of {} raw games", raw_games.len());
            let stats = ingest_page_of_games(
                &config,
                &taxa,
                ingest_id,
                page_index,
                raw_games,
                &mut conn,
            ).into_diagnostic()?;
            info!("{} games successfully ingested", stats.num_games_imported);
            info!("{} ongoing games skipped", stats.num_ongoing_games_skipped);
            // TODO Don't skip them, instead add a new state games can be in for "bugged" and
            //   save them so they can be linked to in the web interface
            info!("{} bugged games skipped", stats.num_terminal_incomplete_games_skipped);
            info!("{} games failed to ingest", stats.num_games_with_fatal_errors);
            info!("{} games had already been ingested", stats.num_already_ingested_games_skipped);

            page_index += 1;
        }
    }

    debug!("Process games task is exiting");

    Ok(())
}
