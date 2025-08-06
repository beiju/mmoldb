use std::iter;
use chron::{Chron, ChronEntity};
use futures::{StreamExt, TryStreamExt, pin_mut};
use log::{debug, error, info};
use miette::{Diagnostic, IntoDiagnostic, WrapErr};
use mmoldb_db::taxa::Taxa;
use mmoldb_db::{AsyncConnection, AsyncPgConnection, Connection, PgConnection, async_db, db, QueryResult, QueryError};
use std::sync::Arc;
use chrono::NaiveDateTime;
use hashbrown::hash_map::Entry;
use hashbrown::HashMap;
use itertools::Itertools;
use thiserror::Error;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Error, Diagnostic)]
pub enum IngestFatalError {
    #[error(transparent)]
    DeserializeError(#[from] serde_json::Error),

    #[error(transparent)]
    DbError(#[from] QueryError),
}
pub async fn ingest(
    kind: &'static str,
    chron_fetch_page_size: usize,
    stage_1_chunks_page_size: usize,
    stage_2_chunks_page_size: usize,
    pg_url: String,
    abort: CancellationToken,
    get_start_cursor: impl Fn(&mut PgConnection) -> QueryResult<Option<(NaiveDateTime, String)>> + Copy + Send + Sync + 'static,
    ingest_versions_page: impl Fn(&Taxa, Vec<ChronEntity<serde_json::Value>>, &mut PgConnection, usize) -> Result<(), IngestFatalError> + Copy + Send + Sync + 'static,
) -> miette::Result<()> {
    let notify = Arc::new(Notify::new());
    // Finish tells the task "once there are no more players to process, exit", while
    // abort tells the task "exit immediately, even if there are more players to process"
    let finish = CancellationToken::new();

    // Don't increase this past 1 until players are dispatched to tasks by ID. The DB insert
    // logic requires that the versions for a given player are inserted in order.
    let num_workers = 1;
    debug!("Ingesting {kind} with {num_workers} workers");

    // Names have to outlast tasks, so they're lifted out
    let task_names_and_numbers = (1..=num_workers)
        .map(|n| (format!("{kind} ingest worker {n}"), n))
        .collect_vec();

    let ingest_stage_2_handles = task_names_and_numbers
        .iter()
        .map(|(task_name, n)| {
            let pg_url = pg_url.clone();
            let notify = notify.clone();
            let finish = finish.clone();
            let abort = abort.clone();
            info!("Spawning stage 2 worker {n} named '{task_name}'");
            tokio::task::Builder::new()
                .name(task_name)
                .spawn(ingest_stage_2(
                    kind,
                    stage_2_chunks_page_size,
                    pg_url,
                    abort,
                    notify,
                    finish,
                    *n,
                    get_start_cursor,
                    ingest_versions_page,
            ))
        })
        .collect::<Result<Vec<_>, _>>()
        .into_diagnostic()?;

    info!("Launched Stage 2 {kind} ingest task(s) in the background");
    info!("Beginning Stage 1 {kind} ingest");

    let ingest_conn = PgConnection::establish(&pg_url).into_diagnostic()?;

    tokio::select! {
        result = ingest_stage_1(kind, chron_fetch_page_size, stage_1_chunks_page_size, ingest_conn, notify) => {
            result?;
            // Tell process {kind} workers to stop waiting and exit
            finish.cancel();

            info!("Raw player ingest finished. Waiting for process players task.");
        }
        _ = abort.cancelled() => {
            // No need to set any signals because abort was already set by the caller
            info!("Raw player ingest aborted. Waiting for process players task.");
        }
    }

    for ingest_stage_2_handle in ingest_stage_2_handles {
        ingest_stage_2_handle.await.into_diagnostic()??;
    }

    Ok(())
}

async fn ingest_stage_1(
    kind: &'static str,
    chron_fetch_page_size: usize,
    stage_1_chunks_page_size: usize,
    mut conn: PgConnection,
    notify: Arc<Notify>,
) -> miette::Result<()> {
    let start_cursor = db::get_latest_raw_version_cursor(&mut conn, kind)
        .into_diagnostic()?
        .map(|(dt, id)| (dt.and_utc(), id));
    let start_date = start_cursor.as_ref().map(|(dt, _)| *dt);

    info!("{kind} fetch will start from date {:?}", start_date);

    let chron = Chron::new(chron_fetch_page_size);

    // TODO Remove this after debugging
    let num_skipped_at_start = std::sync::atomic::AtomicUsize::new(0);
    let stream = chron
        .versions(kind, start_date)
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
                let prev = num_skipped_at_start.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                info!("Skipping {} and counting", prev + 1);
            }

            futures::future::ready(skip_this)
        })
        .try_chunks(stage_1_chunks_page_size);
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
            "Stage 1 ingest saving {} {kind}(s) ({} skipped at start)",
            chunk.len(),
            num_skipped_at_start.load(std::sync::atomic::Ordering::SeqCst)
        );
        let inserted = db::insert_versions(&mut conn, &chunk).into_diagnostic()?;
        info!("Stage 1 ingest saved {}(s) {kind}", inserted);

        notify.notify_one();

        if let Some(err) = maybe_err {
            Err(err)?;
        }
    }

    Ok(())
}

async fn ingest_stage_2(
    kind: &'static str,
    stage_2_chunks_page_size: usize,
    url: String,
    abort: CancellationToken,
    notify: Arc<Notify>,
    finish: CancellationToken,
    worker_id: usize,
    get_start_cursor: impl Fn(&mut PgConnection) -> QueryResult<Option<(NaiveDateTime, String)>>,
    ingest_versions_page: impl Fn(&Taxa, Vec<ChronEntity<serde_json::Value>>, &mut PgConnection, usize) -> Result<(), IngestFatalError>,
) -> miette::Result<()> {
    let result = ingest_stage_2_internal(kind, stage_2_chunks_page_size, &url, abort, notify, finish, worker_id, get_start_cursor, ingest_versions_page).await;
    if let Err(err) = &result {
        error!("Error in {kind} Stage 2 ingest: {}. ", err);
    }
    result
}

async fn ingest_stage_2_internal(
    kind: &'static str,
    stage_2_chunks_page_size: usize,
    url: &str,
    abort: CancellationToken,
    notify: Arc<Notify>,
    finish: CancellationToken,
    worker_id: usize,
    get_start_cursor: impl Fn(&mut PgConnection) -> QueryResult<Option<(NaiveDateTime, String)>>,
    ingest_versions_page: impl Fn(&Taxa, Vec<ChronEntity<serde_json::Value>>, &mut PgConnection, usize) -> Result<(), IngestFatalError>,
) -> miette::Result<()> {
    info!("{kind} stage 2 ingest worker launched");
    let mut async_conn = AsyncPgConnection::establish(url).await.into_diagnostic()?;
    let mut conn = PgConnection::establish(url).into_diagnostic()?;
    let taxa = Taxa::new(&mut conn).into_diagnostic()?;

    // Permit ourselves to start processing right away, in case there
    // are unprocessed players left over from a previous ingest. This
    // will happen after every derived data reset.
    notify.notify_one();

    'outer: while {
        debug!("{kind} stage 2 ingest worker {worker_id} is waiting to be woken up");
        tokio::select! {
            biased; // We always want to respond to abort, notify, and finish in that order
            _ = abort.cancelled() => { false }
            _ = notify.notified() => { true }
            _ = finish.cancelled() => { false }
        }
    } {
        debug!("{kind} stage 2 ingest worker {worker_id} is woken up");

        // This can be cached, but as it now only runs once per wakeup it probably doesn't
        // need to be
        let ingest_start_cursor = get_start_cursor(&mut conn).into_diagnostic()?;

        info!("{kind} stage 2 ingest starting at {ingest_start_cursor:?}");

        let versions_stream = async_db::stream_versions_at_cursor(
            &mut async_conn,
            kind,
            ingest_start_cursor
                .as_ref()
                .map(|(dt, id)| (*dt, id.as_str())),
        )
            .await
            .into_diagnostic()
            .context("Getting versions at cursor")?
            .try_chunks(stage_2_chunks_page_size)
            .map(|raw_versions| {
                // when an error occurs, try_chunks gives us the successful portion of the chunk
                // and then the error. we could ingest the successful part, but we don't (yet).
                let raw_versions = raw_versions.map_err(|err| err.1)?;

                info!(
                "Processing batch of {} {kind} on worker {worker_id}",
                raw_versions.len()
            );
                ingest_versions_page(
                    &taxa,
                    raw_versions,
                    &mut conn,
                    worker_id,
                )
            });
        pin_mut!(versions_stream);

        // Consume the stream and abort on error while handling abort
        while let Some(result) = {
            debug!("{kind} stage 2 ingest worker {worker_id} is waiting for the next batch");
            tokio::select! {
                biased; // We always want to respond to abort, then the stream in that order
                _ = abort.cancelled() => break 'outer,
                p = versions_stream.next() => p,
            }
        } {
            // Bind to () so if this ever becomes a meaningful value I don't forget to use it
            let () = result?;
        }
    }

    debug!("{kind} stage 2 ingest worker {worker_id} is exiting");

    Ok(())
}

pub fn batch_by_entity<T>(versions: Vec<T>, get_id: impl Fn(&T) -> &str + 'static) -> impl Iterator<Item=Vec<T>> {
    let mut pending_versions = versions;
    iter::from_fn(move || {
        if pending_versions.is_empty() {
            return None;  // End iteration
        }

        let mut this_batch = HashMap::new();

        // Pull out all players who don't yet appear
        let mut remaining_versions = std::mem::take(&mut pending_versions)
            .into_iter()
            .flat_map(|version| {
                let id = get_id(&version).to_string();
                match this_batch.entry(id) {
                    Entry::Occupied(_) => {
                        // Then retain this version for the next batch
                        Some(version)
                    }
                    Entry::Vacant(entry) => {
                        // Then insert this version into the map and don't retain it
                        entry.insert(version);
                        None
                    }
                }
            })
            .collect_vec();

        let batch = this_batch
            .into_iter()
            .map(|(_, version)| version)
            .collect_vec();

        std::mem::swap(&mut pending_versions, &mut remaining_versions);

        Some(batch)
    })
}