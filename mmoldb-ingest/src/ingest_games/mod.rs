mod check_round_trip;
mod config;
mod sim;
mod worker;

use worker::*;

use chron::{Chron, ChronEntity};
use chrono::{NaiveDateTime, Utc};
use futures::{StreamExt, TryStreamExt, pin_mut, Stream};
use itertools::Itertools;
use log::{debug, error, info, warn};
use mmoldb_db::taxa::Taxa;
use mmoldb_db::{PgConnection, db, ConnectionPool, AsyncPgConnection, AsyncConnection, async_db, QueryResult};
use std::collections::{HashSet};
use std::hash::RandomState;
use std::num::NonZero;
use tokio::fs;
use std::sync::{Arc};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::LinesStream;
use tokio_util::{sync::CancellationToken};
use crate::{IngestErrorContainer, IngestFatalError, IngestStageError};
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
) -> Result<(), IngestFatalError> {
    let known_game_ids_file = match fs::File::open("known-game-ids.txt").await {
        Ok(file) => file,
        Err(err) => {
            warn!("Can't fetch missed games from file: {err}");
            return Ok(());
        }
    };

    let reader = BufReader::new(known_game_ids_file);
    let lines = LinesStream::new(reader.lines());
    let known_ids = lines.try_collect::<HashSet<_>>().await?;

    let mut conn = pool.get()?;

    let our_ids = db::get_all_game_entity_ids_set(&mut conn)?;

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
        let inserted = db::insert_entities(&mut conn, entities.items)?;
        info!("Saved {} games", inserted);
    }

    Ok(())
}

pub async fn ingest_stage_2(
    pool: ConnectionPool,
    ingest_id: i64,
    _abort: CancellationToken,  // TODO Use this argument
    notify: Arc<Notify>,
    finish: CancellationToken,
) -> IngestErrorContainer {
    let mut errs = IngestErrorContainer::new();
    let num_workers = std::thread::available_parallelism()
        .unwrap_or_else(|err| {
            warn!("Couldn't get available cores: {}. Falling back to 1.", err);
            NonZero::new(1).expect("Literal 1 should be nonzero")
        });
    debug!("Ingesting with {} workers", num_workers);

    let partitioner = Partitioner::new(num_workers);

    let url = mmoldb_db::postgres_url_from_environment();
    let async_conn_result = AsyncPgConnection::establish(&url).await
        .map_err(|error| IngestStageError {
            stage_name: "game Stage 2 coordinator".to_string(),
            error: IngestFatalError::AsyncDbPoolError(error),
        });
    let Some(mut async_conn) = errs.append(async_conn_result) else {
        return errs;
    };

    // Task names have to outlive their tasks, so we build then in advance
    let task_names_and_nums = (0..=num_workers.get())
        .map(|worker_idx| (format!("game Stage 2 worker {}", worker_idx), worker_idx))
        .collect_vec();

    // Loop so that we can restart every time we're notify()'d
    loop {
        // Launching and awaiting subtasks outside the loop leads to multiple copies of a
        // game being stuck in the queue. There's probably a more elegant solution than
        // spinning up and shutting down tasks so often, but this will do for now.
        let tasks = task_names_and_nums.iter()
            .flat_map(|(name, worker_idx)| {
                // There's not a principled reason `parallelism` is used as the buffer size,
                // it's just that the more parallelism you want the more buffer you probably
                // also want.
                let (send, recv) = tokio::sync::mpsc::channel(num_workers.get());
                let task_result = tokio::task::Builder::new()
                    .name(name)
                    .spawn(process_games(
                        pool.clone(),
                        ingest_id,
                        recv,
                        *worker_idx
                    ))
                    .map_err(|error| IngestStageError {
                        stage_name: "game Stage 2 coordinator".to_string(),
                        error: IngestFatalError::TaskSpawnError(error),
                    });

                errs.append(task_result)
                    .map(|task| (name.as_str(), send, task))
            })
            .collect_vec();

        let stream_result = async_db::stream_unprocessed_game_versions(&mut async_conn).await
            .map_err(|error| IngestStageError {
                stage_name: "game Stage 2 coordinator".to_string(),
                error: IngestFatalError::DbError(error),
            });

        let tasks = if let Some(stream) = errs.append(stream_result) {
            let result = dispatch_to_stage_2_workers(&partitioner, tasks, stream).await
                .map_err(|error| IngestStageError {
                    stage_name: "game Stage 2 coordinator".to_string(),
                    error,
                });
            errs.append(result)
        } else {
            Some(tasks)
        };

        if let Some(tasks) = tasks {
            // Drop all the senders. This causes the receivers to output None, which is the
            // signal the workers use to know when to exit.
            let tasks = tasks.into_iter()
                .map(|(name, _, task)| (name, task))
                .collect_vec();

            debug!("All available versions for game Stage 2 are dispatched to workers. Waiting for workers to exit.");

            for (name, task) in tasks {
                let join_result = task.await
                    .map_err(|error| IngestStageError {
                        stage_name: name.to_string(),
                        error: IngestFatalError::JoinError(error),
                    });
                if let Some(task_result) = errs.append(join_result) {
                    let task_result = task_result
                        .map_err(|error| IngestStageError {
                            stage_name: name.to_string(),
                            error,
                        });
                    errs.append(task_result);
                }
            }
        }

        info!("game stage 2 ingest coordinator has processed all available versions. Waiting to be waken up or exited...");
        tokio::select! {
            biased; // We want to always
            _ = notify.notified() => {
                info!("game stage 2 ingest coordinator was woken up");
            }
            _ = finish.cancelled() => {
                info!("game stage 2 ingest coordinator exiting because finished was set at the end of the loop");
                break;
            }
        }
    }

    errs
}

// Need to pass `tasks` in and out because we might consume it but we might not
async fn dispatch_to_stage_2_workers<'name>(
    partitioner: &Partitioner,
    tasks: Vec<(&'name str, Sender<ChronEntity<Value>>, JoinHandle<Result<(), IngestFatalError>>)>,
    stream: impl Stream<Item=QueryResult<ChronEntity<Value>>>,
) -> Result<Vec<(&'name str, Sender<ChronEntity<Value>>, JoinHandle<Result<(), IngestFatalError>>)>, IngestFatalError> {
    pin_mut!(stream);

    while let Some(entity) = stream.try_next().await? {
        let assigned_worker = partitioner.partition_for(&entity.entity_id)?;
        // This panics on OOB, which is correct
        let (_, pipe, _) = &tasks[assigned_worker];

        // If the send fails it's probably because a child errored. Propagate child
        // errors first
        if let Err(pipe_err) = pipe.send(entity).await {
            warn!("Got a pipe error, which probably means there's an error in a child task. Joining child tasks...");
            for (_, pipe, task) in tasks.into_iter() {
                drop(pipe); // Signals child to exit
                task.await.map_err(IngestFatalError::JoinError)??;
            }
            warn!("No child tasks exited with errors. Propagating the pipe error instead.");
            return Err(IngestFatalError::SendFailed(pipe_err));
        }
    }

    Ok(tasks)
}

pub async fn ingest_games(
    pool: ConnectionPool,
    ingest_id: i64,
    abort: CancellationToken,
    use_local_cheap_cashews: bool,
) -> IngestErrorContainer {
    let mut errs = IngestErrorContainer::new();
    let notify = Arc::new(Notify::new());
    // Finish tells the task "once there are no more games to process, exit", while
    // abort tells the task "exit immediately, even if there are more games to process"
    let finish = CancellationToken::new();

    let stage_2_task_result = tokio::task::Builder::new()
        .name("Games Stage 2 Ingest Coordinator")
        .spawn(ingest_stage_2(
            pool.clone(),
            ingest_id,
            abort.clone(),
            notify.clone(),
            finish.clone(),
        ))
        .map_err(|err| IngestStageError {
            stage_name: "game Stage 2 coordinator".to_string(),
            error: IngestFatalError::TaskSpawnError(err),
        });
    let stage_2_task = errs.append(stage_2_task_result);

    info!("Launched process games task");
    info!("Beginning raw game ingest");

    let ingest_conn_result = pool.get()
        .map_err(|err| IngestStageError {
            stage_name: "game Stage 1".to_string(),
            error: IngestFatalError::DbPoolError(err),
        });
    if let Some(mut ingest_conn) = errs.append(ingest_conn_result) {
        tokio::select! {
            result = ingest_raw_games(&mut ingest_conn, notify, use_local_cheap_cashews) => {
                let result = result
                    .map_err(|error| IngestStageError {
                        stage_name: "game Stage 1".to_string(),
                        error,
                    });
                
                errs.append(result);

                // Tell process games workers to stop waiting and exit
                finish.cancel();

                info!("Raw game ingest finished. Waiting for process games task.");
            }
            _ = abort.cancelled() => {
                // No need to set any signals because abort was already set by the caller
                info!("Raw game ingest aborted. Waiting for process games task.");
            }
        }
    }

    if let Some(stage_2_task) = stage_2_task {
        let stage_2_task_result = stage_2_task.await
            .map_err(|error| IngestStageError {
                stage_name: "game Stage 2 coordinator".to_string(),
                error: IngestFatalError::JoinError(error),
            });
        if let Some(stage_2_task_result) = errs.append(stage_2_task_result) {
            errs.extend(stage_2_task_result);
        }
    }

    errs
}

async fn ingest_raw_games(conn: &mut PgConnection, notify: Arc<Notify>, use_local_cheap_cashews: bool) -> Result<(), IngestFatalError> {
    let start_date = db::get_latest_entity_valid_from(conn, GAME_KIND)?
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
        let inserted = db::insert_entities(conn, chunk)?;
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
    worker_id: usize,
) -> Result<(), IngestFatalError> {
    let result = process_games_internal(
        pool,
        ingest_id,
        game_recv,
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
    worker_idx: usize,
) -> Result<(), IngestFatalError> {
    let mut conn = pool.get()?;
    let taxa = Taxa::new(&mut conn)?;

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
        )?;
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

        // Must be the last thing in the loop
        get_batch_to_process_start = Utc::now();
    }

    debug!("game stage 2 ingest worker {} is exiting", worker_idx);

    Ok(())
}
