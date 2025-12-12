mod check_round_trip;
mod config;
mod sim;
mod worker;

use worker::*;

use chron::{Chron, ChronEntity};
use chrono::{DateTime, NaiveDateTime, Utc};
use futures::{StreamExt, TryStreamExt, pin_mut};
use itertools::Itertools;
use log::{debug, error, info, warn};
use miette::{WrapErr, IntoDiagnostic};
use mmoldb_db::taxa::Taxa;
use mmoldb_db::{PgConnection, db, ConnectionPool, AsyncPgConnection, AsyncConnection, async_db};
use std::collections::{HashMap, HashSet};
use std::hash::RandomState;
use std::num::NonZero;
use tokio::fs;
use std::sync::{Arc, Mutex};
use hashbrown::hash_map::Entry;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc::Receiver;
use tokio::sync::Notify;
use tokio_stream::wrappers::LinesStream;
use tokio_util::{sync::CancellationToken};
use crate::partitioner::Partitioner;

// I made this a constant because I'm constant-ly terrified of typoing
// it and introducing a difficult-to-find bug
const GAME_KIND: &'static str = "game";
const CHRON_MAX_IDS_PER_CALL: usize = 50;
const CHRON_FETCH_PAGE_SIZE: usize = 100;
const RAW_GAME_INSERT_BATCH_SIZE: usize = 100;
const PROCESS_GAME_BATCH_SIZE: usize = 100;

pub async fn fetch_missed_games(
    pool: ConnectionPool,
) -> miette::Result<()> {
    let known_game_ids_file = match fs::File::open("known-game-ids.txt").await {
        Ok(file) => file,
        Err(err) => {
            warn!("Can't fetch missed games from file: {err}");
            return Ok(());
        }
    };

    let reader = BufReader::new(known_game_ids_file);
    let lines = LinesStream::new(reader.lines());
    let known_ids = lines.try_collect::<HashSet<_>>().await
        .into_diagnostic()?;

    let mut conn = pool.get()
        .into_diagnostic()
        .wrap_err("error getting database connection in fetch_missed_games")?;

    let our_ids = db::get_all_game_entity_ids_set(&mut conn).into_diagnostic()?;

    let chron = Chron::new(CHRON_MAX_IDS_PER_CALL);

    let missing_ids = known_ids.difference(&our_ids).map(|s| s.as_str()).collect_vec();
    info!("{} out of {} known games are missing", missing_ids.len(), known_ids.len());

    for chunk in missing_ids.chunks(CHRON_MAX_IDS_PER_CALL) {
        let entities = chron.entities_by_id("game", chunk).await?;

        assert_eq!(chunk.len(), entities.items.len());

        // For debug
        assert_eq!(
            HashSet::<_, RandomState>::from_iter(chunk.iter().copied()),
            HashSet::<_, RandomState>::from_iter(entities.items.iter().map(|e| e.entity_id.as_str())),
        );

        info!("Saving {} games", chunk.len());
        let inserted = db::insert_entities(&mut conn, entities.items).into_diagnostic()?;
        info!("Saved {} games", inserted);
    }

    Ok(())
}

pub async fn ingest_games(
    pool: ConnectionPool,
    ingest_id: i64,
    abort: CancellationToken,
    use_local_cheap_cashews: bool,
) -> miette::Result<()> {
    let notify = Arc::new(Notify::new());
    // Finish tells the task "once there are no more games to process, exit", while
    // abort tells the task "exit immediately, even if there are more games to process"
    let finish = CancellationToken::new();

    let stage_2_task = tokio::task::Builder::new()
        .name("Games Stage 2 Ingest Coordinator")
        .spawn(move || {
            ingest_stage_2(
                pool,
                ingest_id,
                abort,
                notify,
                finish,
            )
        })?;

    info!("Launched process games task");
    info!("Beginning raw game ingest");

    let mut ingest_conn = pool.get().into_diagnostic()?;

    tokio::select! {
        result = ingest_raw_games(&mut ingest_conn, notify, use_local_cheap_cashews) => {
            result?;
            // Tell process games workers to stop waiting and exit
            finish.cancel();

            info!("Raw game ingest finished. Waiting for process games task.");
        }
        _ = abort.cancelled() => {
            // No need to set any signals because abort was already set by the caller
            info!("Raw game ingest aborted. Waiting for process games task.");
        }
    }

    stage_2_task.await.into_diagnostic()??;

    Ok(())
}

async fn ingest_raw_games(conn: &mut PgConnection, notify: Arc<Notify>, use_local_cheap_cashews: bool) -> miette::Result<()> {
    let start_date = db::get_latest_entity_valid_from(conn, GAME_KIND)
        .into_diagnostic()?
        .as_ref()
        .map(NaiveDateTime::and_utc);

    info!("Fetch will start from {:?}", start_date);

    let chron = Chron::new(CHRON_FETCH_PAGE_SIZE);

    let stream = chron
        .entities(GAME_KIND, start_date, 3, use_local_cheap_cashews)
        .try_chunks(RAW_GAME_INSERT_BATCH_SIZE);
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
        info!("Saving {} games", chunk.len());
        let inserted = db::insert_entities(conn, chunk).into_diagnostic()?;
        info!("Saved {} games", inserted);

        notify.notify_one();

        if let Some(err) = maybe_err {
            Err(err)?;
        }
    }

    Ok(())
}

async fn process_games(
    pool: ConnectionPool,
    ingest_id: i64,
    game_recv: Receiver<ChronEntity<serde_json::Value>>,
    handle: tokio::runtime::Handle,
    worker_id: usize,
) -> miette::Result<()> {
    let result = process_games_internal(
        pool,
        ingest_id,
        game_recv,
        handle,
        worker_id,
    ).await;
    if let Err(err) = &result {
        error!("Error in process games: {}. ", err);
    }
    result
}

async fn process_games_internal(
    pool: ConnectionPool,
    ingest_id: i64,
    game_recv: Receiver<ChronEntity<serde_json::Value>>,
    handle: tokio::runtime::Handle,
    worker_idx: usize,
) -> miette::Result<()> {
    let mut conn = pool.get().into_diagnostic()?;
    let taxa = Taxa::new(&mut conn).into_diagnostic()?;

    let chunk_stream = tokio_stream::wrappers::ReceiverStream::new(game_recv)
        .chunks(PROCESS_GAME_BATCH_SIZE);
    pin_mut!(chunk_stream);

    // TODO This is going to be duplicated across workers now. It's only used for
    //   timings, so it's not terrible, but it should be fixed.
    let mut page_index = 0;
    let mut get_batch_to_process_start = Utc::now();
    while let Some(raw_games) = chunk_stream.next().await {
        let get_batch_to_process_duration = (Utc::now() - get_batch_to_process_start).as_seconds_f64();
        info!(
            "Processing batch of {} raw games on worker {worker_idx}",
            raw_games.len()
        );
        let stats = ingest_page_of_games(
            &taxa,
            ingest_id,
            page_index,
            get_batch_to_process_duration,
            raw_games,
            &mut conn,
            worker_idx,
        )
            .into_diagnostic()?;
        info!(
            "Ingested {} games, skipped {} games due to fatal errors, ignored {} games in \
            progress, skipped {} unsupported games, and skipped {} bugged games on worker {}.",
            stats.num_games_imported,
            stats.num_games_with_fatal_errors,
            stats.num_ongoing_games_skipped,
            stats.num_unsupported_games_skipped,
            stats.num_bugged_games_skipped,
            worker_idx,
        );

        page_index += 1;
        // Yield to allow the tokio scheduler to do its thing
        // (with out this, signal handling effectively doesn't work because it has to wait
        // for the entire process games task to finish)
        handle.block_on(tokio::task::yield_now());

        // Must be the last thing in the loop
        get_batch_to_process_start = Utc::now();
    }

    debug!("game stage 2 ingest worker {} is exiting", worker_idx);

    Ok(())
}
