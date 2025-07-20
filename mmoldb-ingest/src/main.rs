mod ingest;

use std::collections::HashMap;
use chron::{Chron, ChronEntity};
use chrono::{DateTime, NaiveDateTime, Utc};
use futures::{StreamExt, TryStreamExt, pin_mut};
use log::{debug, error, info, warn};
use miette::{Diagnostic, IntoDiagnostic};
use mmoldb_db::{Connection, PgConnection, db};
use std::sync::{Arc, Mutex};
use itertools::Itertools;
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
    console_subscriber::init();

    let url = mmoldb_db::postgres_url_from_environment();

    let mut conn = PgConnection::establish(&url).into_diagnostic()?;
    let ingest_id = db::start_ingest(&mut conn, Utc::now()).into_diagnostic()?;

    let notify = Arc::new(Notify::new());
    let finish = CancellationToken::new();
    let ingest_cursor = Arc::new(Mutex::new(
        db::get_ingest_cursor(&mut conn).into_diagnostic()?
            .map(|(dt, id)| (dt.and_utc(), id))
    ));
    // dupe_tracker is only for debugging
    let dupe_tracker = Arc::new(Mutex::new(HashMap::new()));

    let num_cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or_else(|err| {
            warn!("Couldn't get available cores: {}. Falling back to 1.", err);
            1
        });
    debug!("Ingesting with {} workers", num_cores);

    let process_games_handles = (1..num_cores).map(|n| {
        let url = url.clone();
        let notify = notify.clone();
        let finish = finish.clone();
        let ingest_cursor = ingest_cursor.clone();
        let dupe_tracker = dupe_tracker.clone();
        let handle = tokio::runtime::Handle::current();
        tokio::task::Builder::new()
            .name(format!("Ingest worker {n}").leak())
            .spawn_blocking(move || {
                process_games(&url, ingest_id, ingest_cursor, dupe_tracker, notify, finish, handle, n)
            })
            .expect("Could not spawn worker thread")
    })
        .collect_vec();

    info!("Launched process games task");
    info!("Beginning raw game ingest");

    ingest_raw_games(conn, notify).await?;

    info!("Raw game ingest finished. Waiting for process games task.");

    for process_games_handle in process_games_handles {
        process_games_handle.await.into_diagnostic()??;
    }

    Ok(())
}

async fn ingest_raw_games(mut conn: PgConnection, notify: Arc<Notify>) -> miette::Result<()> {
    let start_date = db::get_latest_entity_valid_from(&mut conn, "game")
        .into_diagnostic()?
        .as_ref()
        .map(NaiveDateTime::and_utc);

    info!("Fetch will start from {:?}", start_date);

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

fn process_games(
    url: &str,
    ingest_id: i64,
    ingest_cursor: Arc<Mutex<Option<(DateTime<Utc>, String)>>>,
    dupe_tracker: Arc<Mutex<HashMap<String, DateTime<Utc>>>>,
    notify: Arc<Notify>,
    finish: CancellationToken,
    handle: tokio::runtime::Handle,
    worker_id: usize,
) -> miette::Result<()> {
    let result = process_games_internal(url, ingest_id, ingest_cursor, dupe_tracker, notify, finish, handle, worker_id);
    if let Err(err) = &result {
        error!("Error in process games: {}. ", err);
    }
    result
}

fn process_games_internal(
    url: &str,
    ingest_id: i64,
    ingest_cursor: Arc<Mutex<Option<(DateTime<Utc>, String)>>>,
    dupe_tracker: Arc<Mutex<HashMap<String, DateTime<Utc>>>>,
    notify: Arc<Notify>,
    finish: CancellationToken,
    handle: tokio::runtime::Handle,
    worker_id: usize,
) -> miette::Result<()> {
    let mut conn = PgConnection::establish(url).into_diagnostic()?;
    let taxa = Taxa::new(&mut conn).into_diagnostic()?;

    // Permit ourselves to start processing right away, in case there
    // are unprocessed games left over from a previous ingest. This
    // will happen after every derived data reset.
    notify.notify_one();

    let mut page_index = 0;
    while {
        debug!("Process games task worker {worker_id} is waiting to be woken up");
        handle.block_on(async {
            tokio::select! {
                biased; // We always want to try to complete the latest ingest before exiting
                _ = notify.notified() => { true }
                _ = finish.cancelled() => { false }
            }
        })
    } {
        debug!("Process games task worker {worker_id} is woken up");

        // The inner loop is over batches of games to process
        loop {
            debug!("Starting ingest loop on worker {worker_id}");
            let get_batch_to_process_start = Utc::now();
            let this_cursor = {
                let mut ingest_cursor = ingest_cursor.lock().unwrap();
                let this_cursor = ingest_cursor.clone();
                debug!("Worker {worker_id} getting games after {:?}", ingest_cursor);
                let next_cursor = db::get_next_cursor(
                    &mut conn,
                    PROCESS_GAME_BATCH_SIZE,
                    ingest_cursor.as_ref().map(|(d, i)| (d.naive_utc(), i.as_str())),
                )
                    .into_diagnostic()?
                    .map(|(dt, id)| (dt.and_utc(), id));
                if let Some(cursor) = next_cursor {
                    *ingest_cursor = Some(cursor);
                    debug!("Worker {worker_id} set cursor to {:?}", ingest_cursor);
                } else {
                    debug!("All games have been processed. Worker {worker_id} waiting to be woken up again.");
                    break;
                }

                this_cursor
            };

            let raw_games = db::get_batch_of_unprocessed_games(
                &mut conn,
                PROCESS_GAME_BATCH_SIZE,
                this_cursor.as_ref().map(|(d, i)| (d.naive_utc(), i.as_str())),
            )
                .into_diagnostic()?;

            {
                let mut dupe_tracker = dupe_tracker.lock().unwrap();
                for game in &raw_games {
                    if let Some(prev_valid_from) = dupe_tracker.get(&game.entity_id) {
                        error!("get_batch_of_unprocessed_games returned a duplicate game {}. Previous valid_from={}, our valid_from={}", game.entity_id, prev_valid_from, game.valid_from);
                    } else {
                        dupe_tracker.insert(game.entity_id.clone(), game.valid_from);
                    }
                }
            }
            let get_batch_to_process_duration = (Utc::now() - get_batch_to_process_start).as_seconds_f64();

            info!("Processing batch of {} raw games on worker {worker_id}", raw_games.len());
            let stats = ingest_page_of_games(
                &taxa,
                ingest_id,
                page_index,
                get_batch_to_process_duration,
                raw_games,
                &mut conn,
                worker_id
            ).into_diagnostic()?;
            info!(
                "Ingested {} games, skipped {} games due to fatal errors, ignored {} games in \
                progress, and skipped {} bugged games on worker {worker_id}.",
                stats.num_games_imported, stats.num_games_with_fatal_errors,
                stats.num_ongoing_games_skipped, stats.num_bugged_games_skipped,
            );

            page_index += 1;
        }
    }

    debug!("Process games worker {worker_id} is exiting");

    Ok(())
}
