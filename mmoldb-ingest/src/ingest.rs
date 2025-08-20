use std::iter;
use chron::{Chron, ChronEntity};
use futures::{StreamExt, TryStreamExt, pin_mut};
use log::{debug, error, info};
use miette::{Diagnostic, IntoDiagnostic, WrapErr};
use mmoldb_db::taxa::Taxa;
use mmoldb_db::{AsyncConnection, AsyncPgConnection, Connection, PgConnection, async_db, db, QueryResult, QueryError, IngestLog};
use std::sync::Arc;
use chrono::{DateTime, NaiveDateTime, Utc};
use hashbrown::hash_map::{Entry, EntryRef};
use hashbrown::HashMap;
use itertools::Itertools;
use thiserror::Error;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use mmoldb_db::models::NewVersionIngestLog;

// const ROLL_BACK_INGEST_TO_DATE: Option<&'static str> = Some("2025-07-16 04:07:42.699296Z");
const ROLL_BACK_INGEST_TO_DATE: Option<&'static str> = None;

#[derive(Debug, Error, Diagnostic)]
pub enum IngestFatalError {
    #[error(transparent)]
    DeserializeError(#[from] serde_json::Error),

    #[error(transparent)]
    DbError(#[from] QueryError),
}

pub struct VersionIngestLogs<'a> {
    pub kind: &'a str,
    pub entity_id: &'a str,
    pub valid_from: NaiveDateTime,
    logs: Vec<NewVersionIngestLog<'a>>,
}

impl<'a> VersionIngestLogs<'a> {
    pub fn new(kind: &'a str, entity_id: &'a str, valid_from: DateTime<Utc>) -> Self {
        Self { kind, entity_id, valid_from: valid_from.naive_utc(), logs: Vec::new() }
    }

    pub fn add_log(&mut self, log_level: i32, s: impl Into<String>) {
        let log_index = self.logs.len() as i32;
        self.logs.push(NewVersionIngestLog {
            kind: self.kind,
            entity_id: self.entity_id,
            valid_from: self.valid_from,
            log_index,
            log_level,
            log_text: s.into(),
        });
    }

    #[allow(dead_code)]
    pub fn critical(&mut self, s: impl Into<String>) {
        self.add_log(0, s);
    }

    #[allow(dead_code)]
    pub fn error(&mut self, s: impl Into<String>) {
        self.add_log(1, s);
    }

    #[allow(dead_code)]
    pub fn warn(&mut self, s: impl Into<String>) {
        self.add_log(2, s);
    }

    #[allow(dead_code)]
    pub fn info(&mut self, s: impl Into<String>) {
        self.add_log(3, s);
    }

    #[allow(dead_code)]
    pub fn debug(&mut self, s: impl Into<String>) {
        self.add_log(4, s);
    }

    #[allow(dead_code)]
    pub fn trace(&mut self, s: impl Into<String>) {
        self.add_log(5, s);
    }

    pub fn into_vec(self) -> Vec<NewVersionIngestLog<'a>> {
        self.logs
    }
}


pub async fn ingest(
    ingest_id: i64,
    kind: &'static str,
    chron_fetch_page_size: usize,
    stage_1_chunks_page_size: usize,
    stage_2_chunks_page_size: usize,
    pg_url: String,
    abort: CancellationToken,
    get_start_cursor: impl Fn(&mut PgConnection) -> QueryResult<Option<(NaiveDateTime, String)>> + Copy + Send + Sync + 'static,
    ingest_versions_page: impl Fn(&Taxa, Vec<ChronEntity<serde_json::Value>>, &mut PgConnection, usize) -> miette::Result<i32> + Copy + Send + Sync + 'static,
) -> miette::Result<()> {
    let notify = Arc::new(Notify::new());
    // Finish tells the task "once there are no more players to process, exit", while
    // abort tells the task "exit immediately, even if there are more players to process"
    let finish = CancellationToken::new();

    // Don't increase this past 1 until players are dispatched to tasks by ID. The DB insert
    // logic requires that the versions for a given player are inserted in order.
    // Also need to handle NewIngestCounts correctly
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
                    ingest_id,
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
    ingest_id: i64,
    kind: &'static str,
    stage_2_chunks_page_size: usize,
    url: String,
    abort: CancellationToken,
    notify: Arc<Notify>,
    finish: CancellationToken,
    worker_id: usize,
    get_start_cursor: impl Fn(&mut PgConnection) -> QueryResult<Option<(NaiveDateTime, String)>>,
    ingest_versions_page: impl Fn(&Taxa, Vec<ChronEntity<serde_json::Value>>, &mut PgConnection, usize) -> miette::Result<i32>,
) -> miette::Result<()> {
    let result = ingest_stage_2_internal(ingest_id, kind, stage_2_chunks_page_size, &url, abort, notify, finish, worker_id, get_start_cursor, ingest_versions_page).await;
    if let Err(err) = &result {
        error!("Error in {kind} Stage 2 ingest: {}. ", err);
    }
    result
}
// 2025-08-11T04:50:48Z DEBUG
async fn ingest_stage_2_internal(
    ingest_id: i64,
    kind: &'static str,
    stage_2_chunks_page_size: usize,
    url: &str,
    abort: CancellationToken,
    notify: Arc<Notify>,
    finish: CancellationToken,
    worker_id: usize,
    get_start_cursor: impl Fn(&mut PgConnection) -> QueryResult<Option<(NaiveDateTime, String)>>,
    ingest_versions_page: impl Fn(&Taxa, Vec<ChronEntity<serde_json::Value>>, &mut PgConnection, usize) -> miette::Result<i32>,
) -> miette::Result<()> {
    info!("{kind} stage 2 ingest worker launched");
    let mut async_conn = AsyncPgConnection::establish(url).await.into_diagnostic()?;
    let mut conn = PgConnection::establish(url).into_diagnostic()?;
    let taxa = Taxa::new(&mut conn).into_diagnostic()?;

    if let Some(dt) = ROLL_BACK_INGEST_TO_DATE {
        let dt = DateTime::parse_from_rfc3339(dt).unwrap().naive_utc();
        info!("Rolling back ingest to {}", dt);
        db::roll_back_ingest_to_date(&mut conn, dt).into_diagnostic()?;
    }

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

        let mut num_skipped = 0;
        let mut num_ingested = 0;
        let mut entity_cache = HashMap::<String, serde_json::Value>::new();
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
            .filter(|result| futures::future::ready(match result {
                Ok(entity) => {
                    let data_without_stats = match &entity.data {
                        serde_json::Value::Object(obj) => {
                            obj.iter()
                                // This is a bit dangerous to do in a function that's generic
                                // over `kind`. Hopefully this doesn't come back to bite me.
                                .filter(|(k, _)| *k != "Stats" && *k != "SeasonStats")
                                .map(|(k, v)| (k.clone(), v.clone()))
                                .collect()
                        }
                        other => other.clone(),
                    };
                    match entity_cache.entry_ref(&entity.entity_id) {
                        EntryRef::Occupied(mut entry) => {
                            if entry.get() == &data_without_stats {
                                num_skipped += 1;
                                if num_skipped % 100000 == 0 {
                                    debug!(
                                        "{num_skipped} duplicate {kind} versions skipped so far. \
                                        {} entities in the cache.",
                                        entity_cache.len(),
                                    );
                                }
                                false // Skip
                            } else {
                                entry.insert(data_without_stats);
                                true // This was a changed version; don't skip
                            }
                        }
                        EntryRef::Vacant(entry) => {
                            entry.insert(data_without_stats);
                            true // This was a new version, don't skip
                        }
                    }
                }
                Err(_) => true, // This was an error, don't skip
            }))
            .try_chunks(stage_2_chunks_page_size)
            .map(|raw_versions| {
                // when an error occurs, try_chunks gives us the successful portion of the chunk
                // and then the error. we could ingest the successful part, but we don't (yet).
                let raw_versions = raw_versions.map_err(|err| err.1).into_diagnostic()?;

                info!(
                    "Processing batch of {} {kind} on worker {worker_id}",
                    raw_versions.len()
                );
                num_ingested += ingest_versions_page(
                    &taxa,
                    raw_versions,
                    &mut conn,
                    worker_id,
                )?;
                
                db::update_num_ingested(&mut conn, ingest_id, kind, num_ingested).into_diagnostic()?;
                
                Ok::<_, miette::Report>(())
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