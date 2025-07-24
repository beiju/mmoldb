mod logic;

use chron::{Chron, ChronEntity};
use chrono::{DateTime, Utc};
use futures::{StreamExt, TryStreamExt, pin_mut};
use itertools::Itertools;
use log::{debug, error, info};
use miette::IntoDiagnostic;
use mmoldb_db::taxa::Taxa;
use mmoldb_db::{Connection, PgConnection, db};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;


// I made this a constant because I'm constant-ly terrified of typoing
// it and introducing a difficult-to-find bug
const PLAYER_KIND: &'static str = "player";
const CHRON_FETCH_PAGE_SIZE: usize = 1000;
const RAW_PLAYER_INSERT_BATCH_SIZE: usize = 1000;
const PROCESS_PLAYER_BATCH_SIZE: usize = 1000;

pub async fn ingest_players(
    pg_url: String,
    ingest_id: i64,
    abort: CancellationToken,
) -> miette::Result<()> {
    let mut conn = PgConnection::establish(&pg_url).into_diagnostic()?;
    let notify = Arc::new(Notify::new());
    // Finish tells the task "once there are no more players to process, exit", while
    // abort tells the task "exit immediately, even if there are more players to process"
    let finish = CancellationToken::new();
    let ingest_cursor = Arc::new(Mutex::new(
        db::get_player_ingest_start_cursor(&mut conn)
            .into_diagnostic()?
            .map(|(dt, id)| (dt.and_utc(), id)),
    ));

    let num_workers = 4;
    debug!("Ingesting with {} workers", num_workers);

    let process_players_handles = (1..=num_workers)
        .map(|n| {
            let pg_url = pg_url.clone();
            let notify = notify.clone();
            let finish = finish.clone();
            let abort = abort.clone();
            let ingest_cursor = ingest_cursor.clone();
            let handle = tokio::runtime::Handle::current();
            tokio::task::Builder::new()
                .name(format!("Ingest worker {n}").leak())
                .spawn_blocking(move || {
                    process_players(
                        &pg_url,
                        ingest_id,
                        ingest_cursor,
                        abort,
                        notify,
                        finish,
                        handle,
                        n,
                    )
                })
        })
        .collect::<Result<Vec<_>, _>>()
        .into_diagnostic()?;

    info!("Launched process players task");
    info!("Beginning raw player ingest");

    let ingest_conn = PgConnection::establish(&pg_url).into_diagnostic()?;

    tokio::select! {
        result = ingest_raw_players(ingest_conn, notify) => {
            result?;
            // Tell process players workers to stop waiting and exit
            finish.cancel();

            info!("Raw player ingest finished. Waiting for process players task.");
        }
        _ = abort.cancelled() => {
            // No need to set any signals because abort was already set by the caller
            info!("Raw player ingest aborted. Waiting for process players task.");
        }
    }

    for process_players_handle in process_players_handles {
        process_players_handle.await.into_diagnostic()??;
    }

    Ok(())
}

async fn ingest_raw_players(mut conn: PgConnection, notify: Arc<Notify>) -> miette::Result<()> {
    let start_cursor = db::get_latest_raw_version_cursor(&mut conn, PLAYER_KIND)
        .into_diagnostic()?
        .map(|(dt, id)| (dt.and_utc(), id));
    let start_date = start_cursor.as_ref().map(|(dt, _)| *dt);

    info!("Players fetch will start from {:?}", start_date);

    let chron = Chron::new(CHRON_FETCH_PAGE_SIZE);

    // TODO Remove this after debugging
    let num_skipped_at_start = std::sync::atomic::AtomicUsize::new(0);
    let stream = chron
        .versions(PLAYER_KIND, start_date)
        // We ask Chron to start at a given valid_from. It will give us
        // all versions whose valid_from is greater than _or equal to_
        // that value. That's good, because it means that if we left
        // off halfway through processing a batch of entities with
        // identical valid_from, we won't miss the rest of the batch.
        // However, we will receive the first half of the batch again.
        // Later steps in the ingest code will error if we attempt to
        // ingest a value that's already in the database, so we have to
        // filter them out. That's what this skip_while is doing.
        // It returns a future just because that's what's dictated by
        // the stream api.
        .skip_while(|result| {
            let skip_this = start_cursor.as_ref().is_some_and(|(start_valid_from, start_entity_id)| {
                result.as_ref().is_ok_and(|entity| {
                    (entity.valid_from, &entity.entity_id) <= (*start_valid_from, start_entity_id)
                })
            });

            if skip_this {
                // Probably doesn't need to be as strict as SeqCst but it's debugging
                // code, so I'm optimizing for confidence that it'll work properly over
                // performance.
                num_skipped_at_start.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }

            futures::future::ready(skip_this)
        })
        .try_chunks(RAW_PLAYER_INSERT_BATCH_SIZE);
    pin_mut!(stream);

    while let Some(chunk) = stream.next().await {
        // When a chunked stream encounters an error, it returns the portion
        // of the chunk that was collected before the error and the error
        // itself. We want to insert the successful portion of the chunk,
        // _then_ propagate any error.
        let (chunk, maybe_err): (Vec<ChronEntity<serde_json::Value>>, _) = match chunk {
            Ok(chunk) => (chunk, None),
            Err(err) => (err.0, Some(err.1)),
        };

        info!("Saving {} players ({} skipped at start)", chunk.len(), num_skipped_at_start.load(std::sync::atomic::Ordering::SeqCst));
        let inserted = db::insert_versions(&mut conn, &chunk).into_diagnostic()?;
        info!("Saved {} players", inserted);

        notify.notify_one();

        if let Some(err) = maybe_err {
            Err(err)?;
        }
    }

    Ok(())
}

fn process_players(
    url: &str,
    ingest_id: i64,
    ingest_cursor: Arc<Mutex<Option<(DateTime<Utc>, String)>>>,
    abort: CancellationToken,
    notify: Arc<Notify>,
    finish: CancellationToken,
    handle: tokio::runtime::Handle,
    worker_id: usize,
) -> miette::Result<()> {
    let result = process_players_internal(
        url,
        ingest_id,
        ingest_cursor,
        abort,
        notify,
        finish,
        handle,
        worker_id,
    );
    if let Err(err) = &result {
        error!("Error in process players: {}. ", err);
    }
    result
}

fn process_players_internal(
    url: &str,
    ingest_id: i64,
    ingest_cursor: Arc<Mutex<Option<(DateTime<Utc>, String)>>>,
    abort: CancellationToken,
    notify: Arc<Notify>,
    finish: CancellationToken,
    handle: tokio::runtime::Handle,
    worker_id: usize,
) -> miette::Result<()> {
    let mut conn = PgConnection::establish(url).into_diagnostic()?;
    let taxa = Taxa::new(&mut conn).into_diagnostic()?;

    // Permit ourselves to start processing right away, in case there
    // are unprocessed players left over from a previous ingest. This
    // will happen after every derived data reset.
    notify.notify_one();

    let mut page_index = 0;
    while {
        debug!("Process players task worker {worker_id} is waiting to be woken up");
        handle.block_on(async {
            tokio::select! {
                biased; // We always want to respond to abort, notify, and finish in that order
                _ = abort.cancelled() => { false }
                _ = notify.notified() => { true }
                _ = finish.cancelled() => { false }
            }
        })
    } {
        debug!("Process players task worker {worker_id} is woken up");

        // The inner loop is over batches of players to process
        while !abort.is_cancelled() {
            debug!("Starting ingest loop on worker {worker_id}");
            let get_batch_to_process_start = Utc::now();
            let this_cursor = {
                let mut ingest_cursor = ingest_cursor.lock().unwrap();
                let this_cursor = ingest_cursor.clone();
                debug!(
                    "Worker {worker_id} getting players after {:?}",
                    ingest_cursor
                );
                let next_cursor = db::advance_version_cursor(
                    &mut conn,
                    PLAYER_KIND,
                    ingest_cursor
                        .as_ref()
                        .map(|(d, i)| (d.naive_utc(), i.as_str())),
                    PROCESS_PLAYER_BATCH_SIZE,
                )
                .into_diagnostic()?
                .map(|(dt, id)| (dt.and_utc(), id));
                if let Some(cursor) = next_cursor {
                    *ingest_cursor = Some(cursor);
                    debug!("Worker {worker_id} set cursor to {:?}", ingest_cursor);
                } else {
                    debug!(
                        "All players have been processed. Worker {} waiting to be woken up again.",
                        worker_id,
                    );
                    break;
                }

                this_cursor
            };

            let raw_players = db::get_versions_at_cursor(
                &mut conn,
                PLAYER_KIND,
                PROCESS_PLAYER_BATCH_SIZE,
                this_cursor
                    .as_ref()
                    .map(|(d, i)| (d.naive_utc(), i.as_str())),
            )
            .into_diagnostic()?;

            let get_batch_to_process_duration =
                (Utc::now() - get_batch_to_process_start).as_seconds_f64();

            // Make "graceful" shutdown a bit more responsive by checking `abort` in between
            // long-running operations
            if abort.is_cancelled() {
                break;
            }

            info!(
                "Processing batch of {} raw players on worker {worker_id}",
                raw_players.len()
            );
            logic::ingest_page_of_players(
                &taxa,
                ingest_id,
                page_index,
                get_batch_to_process_duration,
                raw_players,
                &mut conn,
                worker_id,
            )
            .into_diagnostic()?;

            // TEMP Try to make my debugger stop hanging
            handle.block_on(tokio::time::sleep(std::time::Duration::from_secs(1)));

            page_index += 1;
            // Yield to allow the tokio scheduler to do its thing
            // (with out this, signal handling effectively doesn't work because it has to wait
            // for the entire process players task to finish)
            handle.block_on(tokio::task::yield_now());
        }
    }

    debug!("Process players worker {worker_id} is exiting");

    Ok(())
}
