use miette::WrapErr;
mod logic;

use chron::{Chron, ChronEntity};
use futures::{StreamExt, TryStreamExt, pin_mut};
use log::{debug, error, info};
use miette::IntoDiagnostic;
use mmoldb_db::taxa::Taxa;
use mmoldb_db::{AsyncConnection, AsyncPgConnection, Connection, PgConnection, async_db, db};
use std::sync::Arc;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

// I made this a constant because I'm constant-ly terrified of typoing
// it and introducing a difficult-to-find bug
const PLAYER_KIND: &'static str = "player";
const CHRON_FETCH_PAGE_SIZE: usize = 1000;
const RAW_PLAYER_INSERT_BATCH_SIZE: usize = 1000;
const PROCESS_PLAYER_BATCH_SIZE: usize = 100000;

pub async fn ingest_players(pg_url: String, abort: CancellationToken) -> miette::Result<()> {
    let notify = Arc::new(Notify::new());
    // Finish tells the task "once there are no more players to process, exit", while
    // abort tells the task "exit immediately, even if there are more players to process"
    let finish = CancellationToken::new();

    let num_workers = 1; // Don't increase this past 1 until player.augments/priority_shifts/recompositions is fixed to handle it
    debug!("Ingesting with {} workers", num_workers);

    let process_players_handles = (1..=num_workers)
        .map(|n| {
            let pg_url = pg_url.clone();
            let notify = notify.clone();
            let finish = finish.clone();
            let abort = abort.clone();
            tokio::task::Builder::new()
                .name(format!("Ingest worker {n}").leak())
                .spawn(process_players(pg_url, abort, notify, finish, n))
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
            let skip_this =
                start_cursor
                    .as_ref()
                    .is_some_and(|(start_valid_from, start_entity_id)| {
                        result.as_ref().is_ok_and(|entity| {
                            (entity.valid_from, &entity.entity_id)
                                <= (*start_valid_from, start_entity_id)
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

        info!(
            "Saving {} players ({} skipped at start)",
            chunk.len(),
            num_skipped_at_start.load(std::sync::atomic::Ordering::SeqCst)
        );
        let inserted = db::insert_versions(&mut conn, &chunk).into_diagnostic()?;
        info!("Saved {} players", inserted);

        notify.notify_one();

        if let Some(err) = maybe_err {
            Err(err)?;
        }
    }

    Ok(())
}

async fn process_players(
    url: String,
    abort: CancellationToken,
    notify: Arc<Notify>,
    finish: CancellationToken,
    worker_id: usize,
) -> miette::Result<()> {
    let result = process_players_internal(&url, abort, notify, finish, worker_id).await;
    if let Err(err) = &result {
        error!("Error in process players: {}. ", err);
    }
    result
}

async fn process_players_internal(
    url: &str,
    abort: CancellationToken,
    notify: Arc<Notify>,
    finish: CancellationToken,
    worker_id: usize,
) -> miette::Result<()> {
    let mut async_conn = AsyncPgConnection::establish(url).await.into_diagnostic()?;
    let mut conn = PgConnection::establish(url).into_diagnostic()?;
    let taxa = Taxa::new(&mut conn).into_diagnostic()?;

    // Permit ourselves to start processing right away, in case there
    // are unprocessed players left over from a previous ingest. This
    // will happen after every derived data reset.
    notify.notify_one();

    'outer: while {
        debug!(
            "Process players task worker {worker_id} is waiting to be woken up (notify: {:?})",
            notify
        );
        tokio::select! {
            biased; // We always want to respond to abort, notify, and finish in that order
            _ = abort.cancelled() => { info!("abort"); false }
            _ = notify.notified() => { info!("notify"); true }
            _ = finish.cancelled() => { info!("finish"); false }
        }
    } {
        debug!("Process players task worker {worker_id} is woken up");

        // This can be cached, but as it now only runs once per wakeup it probably doesn't
        // need to be
        let ingest_start_cursor =
            db::get_player_ingest_start_cursor(&mut conn).into_diagnostic()?;

        info!("Player derived data ingest starting at {ingest_start_cursor:?}");

        let players_stream = async_db::stream_versions_at_cursor(
            &mut async_conn,
            PLAYER_KIND,
            ingest_start_cursor
                .as_ref()
                .map(|(dt, id)| (*dt, id.as_str())),
        )
        .await
        .into_diagnostic()
        .context("Getting versions at cursor")?
        .try_chunks(PROCESS_PLAYER_BATCH_SIZE)
        .map(|raw_players| {
            // when an error occurs, try_chunks gives us the successful portion of the chunk
            // and then the error. we could ingest the successful part, but we don't (yet).
            let raw_players = raw_players.map_err(|err| err.1)?;

            info!(
                "Processing batch of {} raw players on worker {worker_id}",
                raw_players.len()
            );
            logic::ingest_page_of_players(
                &taxa,
                0.0, // TODO
                raw_players,
                &mut conn,
                worker_id,
            )
        });
        pin_mut!(players_stream);

        // Consume the stream and abort on error while handling abort
        while let Some(result) = {
            debug!("Process players task worker {worker_id} is waiting for the next batch");
            tokio::select! {
                biased; // We always want to respond to abort, then the stream in that order
                _ = abort.cancelled() => {
                    // Note this is in the loop condition, so it breaks the outer loop
                    break 'outer;
                }
                p = players_stream.next() => p,
            }
        } {
            // Bind to () so if this ever becomes a meaningful value I don't forget to use it
            let () = result?;
        }
    }

    debug!("Process players worker {worker_id} is exiting");

    Ok(())
}
