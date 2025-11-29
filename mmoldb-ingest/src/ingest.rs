use crate::config::IngestibleConfig;
use chron::{Chron, ChronEntity, ChronStreamError};
use chrono::{DateTime, NaiveDateTime, Utc};
use futures::{pin_mut, Stream, StreamExt, TryStreamExt};
use hashbrown::hash_map::Entry;
use hashbrown::HashMap;
use itertools::{Either, Itertools};
use log::{debug, error, info, warn};
use miette::Diagnostic;
use mmoldb_db::models::NewVersionIngestLog;
use mmoldb_db::taxa::Taxa;
use mmoldb_db::{async_db, db, AsyncConnection, AsyncPgConnection, ConnectionPool, PgConnection, QueryError, QueryResult};
use num::integer::Integer;
use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;
use std::iter;
use std::num::NonZero;
use std::pin::Pin;
use std::sync::Arc;
use serde::Deserialize;
use thiserror::Error;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{Notify};
use tokio::task::JoinError;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Error, Diagnostic)]
pub enum IngestFatalError {
    #[error("error in chron stream")]
    ChronStreamError(#[from] ChronStreamError),

    #[error(transparent)]
    DeserializeError(#[from] serde_json::Error),

    #[error("entity id was not ASCII: {0}")]
    NonAsciiEntityId(#[source] ascii::AsAsciiStrError),

    #[error("trailing digits of entity id were not parseable as hex: {0}")]
    NonHexEntityId(#[source] std::num::ParseIntError),

    #[error(transparent)]
    DbError(#[from] QueryError),

    #[error("couldn't spawn task")]
    TaskSpawnError(#[source] std::io::Error),

    #[error("couldn't get a database connection")]
    DbPoolError(#[from] mmoldb_db::PoolError),

    #[error("couldn't get an async database connection")]
    AsyncDbPoolError(#[from] mmoldb_db::ConnectionError),

    #[error("couldn't join ingest task")]
    JoinError(#[source] JoinError),

    #[error("couldn't dispatch version to worker")]
    SendFailed(#[source] SendError<ChronEntity<serde_json::Value>>),
}

pub struct VersionIngestLogs<'a> {
    pub kind: &'a str,
    pub entity_id: &'a str,
    pub valid_from: NaiveDateTime,
    logs: Vec<NewVersionIngestLog<'a>>,
}

impl<'a> VersionIngestLogs<'a> {
    pub fn new(kind: &'a str, entity_id: &'a str, valid_from: DateTime<Utc>) -> Self {
        Self {
            kind,
            entity_id,
            valid_from: valid_from.naive_utc(),
            logs: Vec::new(),
        }
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

pub struct Ingestor {
    pool: ConnectionPool,
    ingest_id: i64,
    abort: CancellationToken,
}

impl Ingestor {
    pub fn new(
        pool: ConnectionPool,
        ingest_id: i64,
        abort: CancellationToken,
    ) -> Ingestor {
        Ingestor {
            pool,
            ingest_id,
            abort,
        }
    }

    pub async fn ingest<I: Ingestable + 'static>(&self, ingestible: I, use_local_cheap_cashews: bool) -> Result<(), IngestFatalError> {
        let config = ingestible.config();
        if !config.enable {
            return Ok(());
        }

        let parallelism = config.ingest_parallelism
            .unwrap_or_else(|| {
                std::thread::available_parallelism()
                    .unwrap_or_else(|e| {
                        warn!("Can't get hardware parallelism. Defaulting to 1. {e}");
                        NonZero::new(1).unwrap()
                    })
            });

        info!("Beginning {} ingest", I::KIND);

        let stages_with_task_names = ingestible.stages()
            .into_iter()
            .map(|stage| {
                let name = format!("{} {} ingest coordinator", I::KIND, stage.name());
                (stage, name)
            })
            .collect_vec();

        let mut waker = None;
        let running_stages = stages_with_task_names.iter()
            .map(|(stage, task_name)| {
                let prev_stage_finished = CancellationToken::new();
                let task = tokio::task::Builder::new()
                    .name(task_name)
                    .spawn(stage.clone().run({
                        let wake_next_stage = Arc::new(Notify::new());
                        let waker_from_prev_stage = waker.replace(wake_next_stage.clone());
                        StageArgs {
                            use_local_cheap_cashews,
                            config,
                            parallelism,
                            pool: self.pool.clone(),
                            ingest_id: self.ingest_id,
                            wake_next_stage,
                            waker_from_prev_stage,
                            prev_stage_finished: prev_stage_finished.clone(),
                        }
                    }))?;
                Ok((task, prev_stage_finished))
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(IngestFatalError::TaskSpawnError)?;

        debug!("Waiting for {} {} ingest stages to finish", running_stages.len(), I::KIND);

        for (stage, prev_stage_finished) in running_stages {
            prev_stage_finished.cancel();
            // TODO intelligently handle concurrent errors
            stage.await.map_err(IngestFatalError::JoinError)??;
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct StageArgs {
    use_local_cheap_cashews: bool,
    config: &'static IngestibleConfig,
    parallelism: NonZero<usize>,
    pool: ConnectionPool,
    ingest_id: i64,
    wake_next_stage: Arc<Notify>,
    waker_from_prev_stage: Option<Arc<Notify>>,
    prev_stage_finished: CancellationToken,
}

pub trait IngestStage {
    fn name(&self) -> &'static str;

    fn run(self: Arc<Self>, args: StageArgs) -> Pin<Box<dyn Future<Output=Result<(), IngestFatalError>> + Send>>;
}

pub trait Ingestable {
    const KIND: &'static str;

    fn config(&self) -> &'static IngestibleConfig;

    fn stages(&self) -> Vec<Arc<dyn IngestStage>>;
}

pub struct VersionStage1Ingest {
    kind: &'static str,
}

impl VersionStage1Ingest {
    pub fn new(kind: &'static str) -> VersionStage1Ingest {
        VersionStage1Ingest { kind }
    }

    async fn run(self: Arc<Self>, args: StageArgs) -> Result<(), IngestFatalError> {
        let mut conn = args.pool.get()?;
        let chron = Chron::new(args.config.chron_fetch_batch_size);

        let start_cursor = db::get_latest_raw_version_cursor(&mut conn, self.kind)?
            .map(|(dt, id)| (dt.and_utc(), id));
        let start_date = start_cursor.as_ref().map(|(dt, _)| *dt);

        info!("{} fetch will start from date {:?}", self.kind, start_date, );

        let stream = chron
            .versions(self.kind, start_date, 3, args.use_local_cheap_cashews)
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

                futures::future::ready(skip_this)
            })
            .try_chunks(args.config.insert_raw_entity_batch_size);
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

            info!("{} stage 1 ingest saving {} {}(s)", self.kind, chunk.len(), self.kind);
            let inserted = match db::insert_versions(&mut conn, &chunk) {
                Ok(x) => Ok(x),
                Err(err) => {
                    error!("Error in stage 1 ingest write: {err}");
                    Err(err)
                }
            }?;
            info!("{} stage 1 ingest saved {} {}(s)", self.kind, inserted, self.kind);

            args.wake_next_stage.notify_one();
            
            if let Some(err) = maybe_err {
                Err(err)?;
            }
        }

        info!("{} stage 1 ingest finished", self.kind);
        Ok(())
    }
}

impl IngestStage for VersionStage1Ingest {
    fn name(&self) -> &'static str {
        "Stage 1"
    }

    // Treat this as boilerplate
    fn run(self: Arc<Self>, args: StageArgs) -> Pin<Box<dyn Future<Output=Result<(), IngestFatalError>> + Send>> {
        Box::pin(VersionStage1Ingest::run(self, args))
    }
}

pub struct FeedEventVersionStage1Ingest {
    kind: &'static str,
    embedded_kind: &'static str,
}

async fn insert_feed_event_versions_from_stream(
    async_conn: &mut AsyncPgConnection,
    conn: &mut PgConnection,
    event_cache: &mut HashMap<(String, i32), serde_json::Value>,
    insert_as_kind: &str,
    kind: &str,
    kind_description: &str,
    batch_size: usize,
    until: Option<NaiveDateTime>,
    wake_next_stage: Arc<Notify>,
) -> Result<(), IngestFatalError> {
    let start_cursor = db::get_latest_raw_feed_event_version_cursor(conn, insert_as_kind)
        .map_err(|e| { info!("Error {e}"); e })?
        .map(|(dt, id, _)| (dt, id))
        .unwrap_or_else(|| (
            // This is the date of the first record with a feed, rounded down a bit
            NaiveDateTime::parse_from_str("2025-06-09 11:50:00.000000", "%Y-%m-%d %H:%M:%S%.f")
                .expect("Hard-coded date must not fail to parse"),
            "6805db0cac48194de3cd3ff7".to_string(),
        ));

    info!("Migrating feed event versions from {kind_description} starting from {:?}", start_cursor);

    let version_chunks_stream = async_db::stream_versions_at_cursor_until(
        async_conn,
        kind,
        Some(start_cursor),
        until,
    ).await?
        .flat_map(|result| futures::stream::iter(match result {
            Ok(version) => {
                #[derive(Deserialize)]
                struct SomethingWithFeed {
                    // It has a capital F when it's embedded in an entity (team or player)
                    // and lowercase f when it's alone
                    #[serde(alias = "Feed")]
                    feed: Option<Vec<serde_json::Value>>,
                }

                match serde_json::from_value::<SomethingWithFeed>(version.data) {
                    Ok(des) => {
                        if let Some(feed) = des.feed {
                            let dt = version.valid_from.naive_utc();
                            Either::Left(Either::Right(
                                feed.into_iter()
                                    .enumerate()
                                    .map(move |(idx, item)| Ok((version.entity_id.clone(), idx as i32, dt, item)))
                            ))
                        } else {
                            Either::Right(Either::Left(iter::empty()))
                        }
                    },
                    Err(err) => {
                        Either::Left(Either::Left(iter::once(Err(IngestFatalError::DeserializeError(err)))))
                    }
                }
            }
            Err(err) => {
                Either::Right(Either::Right(iter::once(Err(IngestFatalError::DbError(err)))))
            }
        }
        ))
        .filter(|result| futures::future::ready(filter_cached(event_cache, result)))
        .chunks(batch_size);
    pin_mut!(version_chunks_stream);

    while let Some(version_chunk) = version_chunks_stream.next().await {
        let chunk_len = version_chunk.len();
        let versions = version_chunk.into_iter().collect::<Result<Vec<_>, _>>()?;
        let (_, _, date, _): &(String, i32, NaiveDateTime, serde_json::Value) = versions.last()
            .expect("This vec cannot be empty");
        let human_time_ago = chrono_humanize::HumanTime::from(date.and_utc())
            .to_text_en(chrono_humanize::Accuracy::Precise, chrono_humanize::Tense::Past);
        debug!(
            "Inserting {chunk_len} {kind_description} event versions. Currently processing \
            {kind_description}s from {human_time_ago}",
        );
        let chunk_insert_start = Utc::now();
        let n_inserted = db::insert_feed_event_versions(conn, insert_as_kind, &versions)?;
        let chunk_insert_duration = (Utc::now() - chunk_insert_start).as_seconds_f64();
        debug!("Inserted {n_inserted} of {chunk_len} {kind_description} event versions in {chunk_insert_duration:.2}s");
        wake_next_stage.notify_one();
    }

    debug!("Finished processing legacy {kind_description} versions");
    Ok(())
}

fn filter_cached(event_cache: &mut HashMap<(String, i32), serde_json::Value>, result: &Result<(String, i32, NaiveDateTime, serde_json::Value), IngestFatalError>) -> bool {
    match result {
        Ok((id, idx, _, item)) => {
            match event_cache.entry((id.clone(), *idx)) {
                Entry::Occupied(mut entry) => {
                    if entry.get() == item {
                        false // matches the cached version, don't process
                    } else {
                        entry.insert(item.clone());
                        true // doesn't match the cached version, must process
                    }
                },
                Entry::Vacant(entry) => {
                    entry.insert(item.clone());
                    true // there was no cached version, must process
                }
            }
        },
        Err(_) => true, // always process errors
    }
}

impl FeedEventVersionStage1Ingest {
    pub fn new(kind: &'static str, embedded_kind: &'static str) -> FeedEventVersionStage1Ingest {
        FeedEventVersionStage1Ingest { kind, embedded_kind }
    }

    async fn run(self: Arc<Self>, args: StageArgs) -> Result<(), IngestFatalError> {
        let mut conn = args.pool.get()?;

        let url = mmoldb_db::postgres_url_from_environment();
        let mut async_conn = AsyncPgConnection::establish(&url).await?;
        let mut event_cache = HashMap::new();

        insert_feed_event_versions_from_stream(
            &mut async_conn,
            &mut conn,
            &mut event_cache,
            self.kind,
            self.embedded_kind,
            &format!("embedded {} feed", self.embedded_kind),
            args.config.insert_raw_entity_batch_size,
            Some(NaiveDateTime::parse_from_str("2025-07-29 00:00:00.000000", "%Y-%m-%d %H:%M:%S%.f").expect("Hard-coded date must not fail to parse")),
            args.wake_next_stage.clone(),
        ).await?;

        insert_feed_event_versions_from_stream(
            &mut async_conn,
            &mut conn,
            &mut event_cache,
            self.kind,
            self.kind,
            self.kind,
            args.config.insert_raw_entity_batch_size,
            None,
            args.wake_next_stage.clone(),
        ).await?;

        // Need to re-acquire the cursor after inserting embedded feed events
        // (this could be skipped if we know we didn't insert any feed events)
        let start_cursor = db::get_latest_raw_feed_event_version_cursor(&mut conn, self.kind)?
            .map(|(dt, id, _)| (dt, id));
        let start_cursor_utc = start_cursor.as_ref().map(|(dt, id)| (dt.and_utc(), id));

        let start_date = start_cursor.as_ref().map(|(dt, _)| dt.and_utc());
        info!("{} fetch will start from date {:?}", self.kind, start_date, );
        let chron = Chron::new(args.config.chron_fetch_batch_size);

        let stream = chron
            .versions(self.kind, start_date, 3, args.use_local_cheap_cashews)
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
                    start_cursor_utc
                        .as_ref()
                        .is_some_and(|(start_valid_from, start_entity_id)| {
                            result.as_ref().is_ok_and(|entity| {
                                (entity.valid_from, &entity.entity_id)
                                    <= (*start_valid_from, start_entity_id)
                            })
                        });

                futures::future::ready(skip_this)
            })
            .flat_map(|result| futures::stream::iter(match result {
                Ok(version) => {
                    #[derive(Deserialize)]
                    struct SomethingWithFeed {
                        // It has a capital F when it's embedded in an entity (team or player)
                        // and lowercase f when it's alone
                        #[serde(alias = "Feed")]
                        feed: Option<Vec<serde_json::Value>>,
                    }

                    match serde_json::from_value::<SomethingWithFeed>(version.data) {
                        Ok(des) => {
                            if let Some(feed) = des.feed {
                                let dt = version.valid_from.naive_utc();
                                Either::Left(Either::Right(feed.into_iter().enumerate().map(move |(idx, item)| Ok((version.entity_id.clone(), idx as i32, dt, item)))))
                            } else {
                                Either::Right(Either::Left(iter::empty()))
                            }
                        },
                        Err(err) => {
                            Either::Left(Either::Left(iter::once(Err(IngestFatalError::DeserializeError(err)))))
                        }
                    }
                }
                Err(err) => {
                    Either::Right(Either::Right(iter::once(Err(IngestFatalError::ChronStreamError(err)))))
                }
            }
            ))
            .filter(|result| futures::future::ready(filter_cached(&mut event_cache, result)))
            .try_chunks(args.config.insert_raw_entity_batch_size);
        pin_mut!(stream);

        while let Some(chunk) = stream.next().await {
            // When a chunked stream encounters an error, it returns the portion
            // of the chunk that was collected before the error and the error
            // itself. We want to insert the successful portion of the chunk,
            // _then_ propagate any error.
            let (chunk, maybe_err): (Vec<(String, i32, NaiveDateTime, serde_json::Value)>, _) = match chunk {
                Ok(chunk) => (chunk, None),
                Err(err) => (err.0, Some(err.1)),
            };

            info!("{} stage 1 ingest saving {} {}(s)", self.kind, chunk.len(), self.kind);
            let inserted = match db::insert_feed_event_versions(&mut conn, self.kind, &chunk) {
                Ok(x) => Ok(x),
                Err(err) => {
                    error!("Error in stage 1 ingest write: {err}");
                    Err(err)
                }
            }?;
            info!("{} stage 1 ingest saved {} {}(s)", self.kind, inserted, self.kind);

            args.wake_next_stage.notify_one();

            if let Some(err) = maybe_err {
                Err(err)?;
            }
        }

        info!("{} stage 1 ingest finished", self.kind);
        Ok(())
    }
}

impl IngestStage for FeedEventVersionStage1Ingest {
    fn name(&self) -> &'static str {
        "Stage 1"
    }

    // Treat this as boilerplate
    fn run(self: Arc<Self>, args: StageArgs) -> Pin<Box<dyn Future<Output=Result<(), IngestFatalError>> + Send>> {
        Box::pin(FeedEventVersionStage1Ingest::run(self, args))
    }
}

pub struct Stage2Ingest<VersionIngest> {
    kind: &'static str,
    version_ingest: VersionIngest,
}

pub trait IngestibleFromVersions {
    type Entity: serde::de::DeserializeOwned + Send;

    fn get_start_cursor(conn: &mut PgConnection) -> QueryResult<Option<(NaiveDateTime, String)>>;
    fn trim_unused(version: &serde_json::Value) -> serde_json::Value;
    fn insert_batch(conn: &mut PgConnection, taxa: &Taxa, versions: &Vec<ChronEntity<Self::Entity>>) -> QueryResult<usize>;
    fn stream_versions_at_cursor(
        conn: &mut AsyncPgConnection,
        kind: &str,
        cursor: Option<(NaiveDateTime, String)>,
    ) -> impl Future<Output = QueryResult<impl Stream<Item = QueryResult<ChronEntity<serde_json::Value>>> + Send>> + Send;
}

impl<VersionIngest: IngestibleFromVersions + Send + Sync + 'static> Stage2Ingest<VersionIngest>  {
    pub fn new(kind: &'static str, version_ingest: VersionIngest) -> Self {
        Stage2Ingest { kind, version_ingest }
    }

    async fn run(self: Arc<Self>, args: StageArgs) -> Result<(), IngestFatalError> {
        // Compute how many least significant hexits we need to accurately compute
        // the modulus between an arbitrary hex number and the given parallelism
        let trailing_hexits_for_modulus = args.parallelism.get().lcm(&16);

        // Task names have to outlive their tasks, so we build then in advance
        let task_names_and_nums = (0..=args.parallelism.get())
            .map(|worker_idx| (format!("{} Stage 2 worker {}", self.kind, worker_idx), worker_idx))
            .collect_vec();

        let tasks = task_names_and_nums.iter()
            .map(|(name, worker_idx)| {
                // There's not a principled reason `parallelism` is used as the buffer size,
                // it's just that the more parallelism you want the more buffer you probably
                // also want.
                let (send, recv) = tokio::sync::mpsc::channel(args.parallelism.get());
                let task = tokio::task::Builder::new()
                    .name(name)
                    .spawn(self.clone().worker(args.clone(), recv, *worker_idx))?;
                Ok((send, task))
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(IngestFatalError::TaskSpawnError)?;

        let mut start_cursor = {
            let mut conn = args.pool.get()?;
            VersionIngest::get_start_cursor(&mut conn)?
        };
        // TODO Pool this too?
        let url = mmoldb_db::postgres_url_from_environment();
        let mut async_conn = AsyncPgConnection::establish(&url).await?;


        // Probably not all of this needs to be in the loop but I'm tired, boss
        loop {
            let versions_stream = VersionIngest::stream_versions_at_cursor(
                &mut async_conn,
                self.kind,
                start_cursor.clone(),
            ).await?;
            pin_mut!(versions_stream);

            while let Some(version_result) = versions_stream.next().await {
                let version = version_result?;
                let ascii_id = ascii::AsciiStr::from_ascii(version.entity_id.as_bytes())
                    .map_err(IngestFatalError::NonAsciiEntityId)?;
                let start_idx = ascii_id.len() - trailing_hexits_for_modulus;
                let hex_for_modulus = if start_idx > 0 {
                    usize::from_str_radix(ascii_id[start_idx..].as_str(), 16)
                        .map_err(IngestFatalError::NonHexEntityId)?
                } else {
                    usize::from_str_radix(ascii_id.as_str(), 16)
                        .map_err(IngestFatalError::NonHexEntityId)?
                };
                let assigned_worker = hex_for_modulus % args.parallelism.get();
                // This panics on OOB, which is correct
                let (pipe, _) = &tasks[assigned_worker];
                let new_cursor = (version.valid_from.naive_utc(), version.entity_id.clone());

                // If the send fails it's probably because a child errored. Propagate child
                // errors first
                if let Err(pipe_err) = pipe.send(version).await {
                    warn!("Got a pipe error, which probably means there's an error in a child task. Joining child tasks...");
                    for (pipe, task) in tasks.into_iter() {
                        drop(pipe); // Signals child to exit
                        task.await.map_err(IngestFatalError::JoinError)??;
                    }
                    warn!("No child tasks exited with errors. Propagating the pipe error instead.");
                    return Err(IngestFatalError::SendFailed(pipe_err));
                }
                start_cursor = Some(new_cursor);
            }

            info!("{} stage 2 ingest coordinator has processed all available versions. Waiting to be waken up or exited...", self.kind);
            if let Some(notify) = &args.waker_from_prev_stage {
                tokio::select! {
                    biased; // We want to always
                    _ = notify.notified() => {
                        info!("{} stage 2 ingest coordinator was woken up", self.kind);
                    }
                    _ = args.prev_stage_finished.cancelled() => {
                        info!("{} stage 2 ingest coordinator exiting because prev_stage_finished was set at the end of the loop", self.kind);
                        break;
                    }
                }
            } else {
                info!("{} stage 2 ingest coordinator exiting because there is no waker from the previous stage", self.kind);
                break;
            }
        }

        // Drop all the senders. This causes the receivers to output None, which is the
        // signal the workers use to know when to exit.
        let tasks = tasks.into_iter()
            .map(|(_, task)| task)
            .collect_vec();

        debug!("All versions for {} Stage 2 are dispatched to workers. Waiting for workers to exit.", self.kind);

        for task in tasks {
            task.await.map_err(IngestFatalError::JoinError)??;
        }

        debug!("All workers for {} finished. Exiting coordinator and notifying next stage.", self.kind);
        args.wake_next_stage.notify_one();

        Ok(())
    }

    // Worker could probably take a conn instead of the whole pool, but the cost is negligible
    async fn worker(self: Arc<Self>, args: StageArgs, version_recv: Receiver<ChronEntity<serde_json::Value>>, worker_idx: usize) -> Result<(), IngestFatalError> {
        let val = self.worker_internal(args, version_recv, worker_idx).await;
        if let Err(err) = &val {
            error!("Error in stage 2 ingest worker: {err}");
        }
        val
    }
    
    async fn worker_internal(self: Arc<Self>, args: StageArgs, version_recv: Receiver<ChronEntity<serde_json::Value>>, worker_idx: usize) -> Result<(), IngestFatalError> {
        info!("{} stage 2 ingest worker {} launched", self.kind, worker_idx);
        let mut conn = args.pool.get()?;
        let taxa = Taxa::new(&mut conn)?;

        let mut cache = HashMap::new();
        let chunk_stream = tokio_stream::wrappers::ReceiverStream::new(version_recv)
            .filter_map(|version| std::future::ready({
                let trimmed_version = VersionIngest::trim_unused(&version.data);

                match cache.entry(version.entity_id.clone()) {
                    Entry::Vacant(vacant) => {
                        // We store the trimmed version for future comparisons
                        vacant.insert(trimmed_version);
                        // We need to return the untrimmed version, or else deserialize will fail
                        Some(version)
                    },
                    Entry::Occupied(mut occupied) => {
                        if occupied.get() == &trimmed_version {
                            // This is a duplicate -- no need to return it
                            None
                        } else {
                            occupied.insert(trimmed_version);
                            Some(version)
                        }
                    }
                }
            }))
            .chunks(args.config.process_batch_size);
        pin_mut!(chunk_stream);

        let mut num_ingested = 0;
        while let Some(raw_versions) = chunk_stream.next().await {
            num_ingested += self.ingest_page(&taxa, raw_versions, &mut conn, worker_idx)?;

            db::update_num_ingested(&mut conn, args.ingest_id, self.kind, num_ingested)?;
        }

        debug!("{} stage 2 ingest worker {} is exiting", self.kind, worker_idx);

        Ok(())
    }

    fn ingest_page(&self, taxa: &Taxa, raw_versions: Vec<ChronEntity<serde_json::Value>>, conn: &mut PgConnection, worker_id: usize) -> Result<i32, IngestFatalError> {
        debug!(
            "Starting ingest of {} {}(s) on worker {worker_id}",
            raw_versions.len(), self.kind
        );
        let save_start = Utc::now();

        let deserialize_start = Utc::now();
        let (entities, deserialize_errors): (Vec<_>, Vec<_>) = raw_versions
            .into_par_iter()
            .partition_map(|entity| {
                if entity.kind != self.kind {
                    warn!("{} ingest task got a {} entity!", self.kind, entity.kind);
                }

                match serde_json::from_value(entity.data) {
                    Ok(data) => Either::Left(ChronEntity {
                        kind: entity.kind,
                        entity_id: entity.entity_id,
                        valid_from: entity.valid_from,
                        valid_to: entity.valid_to,
                        data,
                    }),
                    Err(err) => {
                        Either::Right((err, entity.entity_id, entity.valid_from))
                    },
                }
            });
        let deserialize_duration = (Utc::now() - deserialize_start).as_seconds_f64();
        debug!(
            "Deserialized {} {}(s) in {:.2} seconds on worker {}. {} deserialize errors.",
            entities.len(),
            self.kind,
            deserialize_duration,
            worker_id,
            deserialize_errors.len(),
        );

        let new_ingest_logs = deserialize_errors.iter()
            .map(|(err, entity_id, valid_from)| NewVersionIngestLog {
                kind: self.kind,
                entity_id,
                valid_from: valid_from.naive_utc(),
                log_index: 0, // Deserialize error is always the 0th log item for that version
                log_level: 0, // Critical error
                log_text: format!("{:?}", err), // Not sure whether this should be debug
            })
            .collect();

        let inserted = db::insert_ingest_logs(conn, new_ingest_logs)?;
        debug!("Saved {inserted} deserialize errors");

        let latest_time = entities
            .last()
            .map(|version| version.valid_from)
            .unwrap_or(Utc::now());
        let time_ago = latest_time.signed_duration_since(Utc::now());
        let human_time_ago = chrono_humanize::HumanTime::from(time_ago);

        // The handling of valid_until is entirely in the database layer, but its logic
        // requires that a given batch of {kind}s does not have the same team twice. We
        // provide that guarantee here.
        let entities_len = entities.len();
        let mut total_inserted = 0;
        for batch in batch_by_entity(entities) {
            let to_insert = batch.len();
            info!(
                "Sending {} new {} versions out of {} to the database.",
                to_insert, self.kind, entities_len,
            );

            let inserted = VersionIngest::insert_batch(conn, taxa, &batch)?;
            total_inserted += inserted as i32;

            info!(
                "Sent {} new {} versions out of {} to the database. \
                {inserted} versions were actually inserted, the rest were duplicates. \
                Currently processing {} versions from {human_time_ago}.",
                to_insert,
                self.kind,
                entities_len,
                self.kind,
            );
        }

        let save_duration = (Utc::now() - save_start).as_seconds_f64();

        info!(
            "Ingested page of {} {} versions in {save_duration:.3} seconds.",
            entities_len,
            self.kind,
        );

        Ok(total_inserted)
    }

}

impl<VersionIngest: IngestibleFromVersions + Send + Sync + 'static> IngestStage for Stage2Ingest<VersionIngest> {
    fn name(&self) -> &'static str {
        "Stage 2"
    }

    fn run(self: Arc<Self>, args: StageArgs) -> Pin<Box<dyn Future<Output=Result<(), IngestFatalError>> + Send>> {
        Box::pin(Stage2Ingest::run(self, args))
    }
}

pub fn batch_by_entity<T>(
    versions: Vec<ChronEntity<T>>,
) -> impl Iterator<Item = Vec<ChronEntity<T>>> {
    let mut pending_versions = versions;
    iter::from_fn(move || {
        if pending_versions.is_empty() {
            return None; // End iteration
        }

        let mut this_batch = HashMap::new();

        // Pull out all players who don't yet appear
        let mut remaining_versions = std::mem::take(&mut pending_versions)
            .into_iter()
            .flat_map(|version| {
                match this_batch.entry(version.entity_id.clone()) {
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
