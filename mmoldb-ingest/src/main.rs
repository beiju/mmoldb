mod ingest;

use chron::{Chron, ChronEntity};
use chrono::{NaiveDateTime, Utc};
use futures::{StreamExt, TryStreamExt, pin_mut};
use log::{debug, info};
use miette::{Diagnostic, IntoDiagnostic};
use mmoldb_db::{Connection, PgConnection, db};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use mmoldb_db::taxa::Taxa;
use crate::ingest::{ingest_page_of_games};

const CHRON_FETCH_PAGE_SIZE: usize = 1000;
const RAW_GAME_INSERT_BATCH_SIZE: usize = 1000;
const PROCESS_GAME_BATCH_SIZE: usize = 1000;

#[derive(Debug, Error, Diagnostic)]
#[error(transparent)]
struct BoxedError(#[from] Box<dyn std::error::Error + Send + Sync + 'static>);

#[tokio::main]
async fn main() -> miette::Result<()> {
    env_logger::init();

    let url = mmoldb_db::postgres_url_from_environment();
    let notify = Arc::new(Notify::new());
    let finish = CancellationToken::new();

    let mut conn = PgConnection::establish(&url).into_diagnostic()?;
    let ingest_id = db::start_ingest(&mut conn, Utc::now()).into_diagnostic()?;

    let process_games_handle = tokio::spawn({
        let url = url.clone();
        let notify = notify.clone();
        let finish = finish.clone();
        async move { process_games(&url, ingest_id, notify, finish).await }
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
    ingest_id: i64,
    notify: Arc<Notify>,
    finish: CancellationToken,
) -> miette::Result<()> {
    let mut conn = PgConnection::establish(url).into_diagnostic()?;
    let taxa = Taxa::new(&mut conn).into_diagnostic()?;

    // Permit ourselves to start processing right away, in case there
    // are unprocessed games left over from a previous ingest. This
    // will happen after every derived data reset.
    notify.notify_one();

    let mut page_index = 0;
    while {
        debug!("Process games task is waiting to be woken up");
        tokio::select! {
            biased; // We always want to try to complete the latest ingest before exiting
            _ = notify.notified() => { true }
            _ = finish.cancelled() => { false }
        }
    } {
        debug!("Process games task is woken up");

        // The inner loop is over batches of games to process
        loop {
            let get_batch_to_process_start = Utc::now();
            let raw_games = db::get_batch_of_unprocessed_games(&mut conn, PROCESS_GAME_BATCH_SIZE)
                .into_diagnostic()?;
            let get_batch_to_process_duration = (Utc::now() - get_batch_to_process_start).as_seconds_f64();
            if raw_games.is_empty() {
                debug!("All games have been processed. Waiting to be woken up again.");
                break;
            }

            info!("Processing batch of {} raw games", raw_games.len());
            let stats = ingest_page_of_games(
                &taxa,
                ingest_id,
                page_index,
                get_batch_to_process_duration,
                raw_games,
                &mut conn,
            ).into_diagnostic()?;
            info!(
                "Ingested {} games, skipped {} games due to fatal errors, ignored {} games in \
                progress, and skipped {} bugged games.",
                stats.num_games_imported, stats.num_games_with_fatal_errors,
                stats.num_ongoing_games_skipped, stats.num_bugged_games_skipped,
            );

            page_index += 1;
        }
    }

    debug!("Process games task is exiting");

    Ok(())
}
