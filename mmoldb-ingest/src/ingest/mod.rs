mod fetch;
mod processing;

use crate::config::{IngestConfig, IngestibleConfig};
use crate::partitioner::Partitioner;
use chron::{ChronEntity, ChronStreamError};
use chrono::{DateTime, NaiveDateTime, Utc};
pub use fetch::ChronFetchArgs;
use futures::{Stream, StreamExt, pin_mut};
use hashbrown::HashMap;
use hashbrown::hash_map::Entry;
use itertools::{Either, Itertools};
use miette::Diagnostic;
use mmoldb_db::models::NewVersionIngestLog;
use mmoldb_db::taxa::Taxa;
use mmoldb_db::{
    AsyncConnection, AsyncPgConnection, ConnectionPool, PgConnection, QueryError, QueryResult, db,
};
pub use processing::ProcessingArgs;
use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;
use std::fmt::{Debug, Display, Formatter};
use std::hash::Hash;
use std::iter;
use std::num::NonZero;
use std::sync::Arc;
use std::time::Duration;
use serde::de::IntoDeserializer;
use thiserror::Error;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::error::SendError;
use tokio::task::JoinError;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, error, info, info_span, warn};

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

    #[error(transparent)]
    IoError(#[from] std::io::Error),
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

#[derive(Debug, Error, Diagnostic)]
#[error("in stage {stage_name}: {error}")]
pub struct IngestStageError {
    pub stage_name: String,
    #[source]
    pub error: IngestFatalError,
}

#[derive(Debug, Error, Diagnostic, Default)]
#[error("error(s) in ingest")]
pub struct IngestErrorContainer {
    #[related]
    pub errors: Vec<IngestStageError>,
}

impl IntoIterator for IngestErrorContainer {
    type Item = IngestStageError;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.errors.into_iter()
    }
}

impl IngestErrorContainer {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn from_result(result: Result<(), IngestFatalError>, stage_name: String) -> Self {
        let mut me = Self::new();
        if let Err(error) = result {
            me.errors.push(IngestStageError { stage_name, error });
        }
        me
    }

    pub fn push<T>(&mut self, result: Result<T, IngestStageError>) -> Option<T> {
        match result {
            Ok(value) => Some(value),
            Err(error) => {
                self.errors.push(error);
                None
            }
        }
    }

    pub fn push_err(&mut self, err: IngestStageError) {
        self.errors.push(err);
    }

    pub fn extend(&mut self, other: impl IntoIterator<Item = IngestStageError>) {
        self.errors.extend(other);
    }
}

pub trait IngestStage {
    fn name(&self) -> &'static str;
}

pub trait Ingestable {
    const KIND: &'static str;

    fn config(&self) -> &'static IngestibleConfig;

    fn stages(&self) -> Vec<Arc<dyn IngestStage>>;
}

pub struct Stage2Ingest<VersionIngest> {
    kind: &'static str,
    // Kind of using this like a PhantomData
    #[allow(unused)]
    version_ingest: VersionIngest,
}

pub trait IngestibleFromVersions {
    type Entity: serde::de::DeserializeOwned + Send;
    type BatchSplitKey: Clone + Debug + Eq + Hash + Send;

    fn trim_unused(version: &serde_json::Value) -> serde_json::Value;
    fn batch_split_key(entity: &ChronEntity<Self::Entity>) -> Self::BatchSplitKey;
    fn insert_batch(
        conn: &mut PgConnection,
        taxa: &Taxa,
        versions: &Vec<ChronEntity<Self::Entity>>,
    ) -> QueryResult<(usize, usize)>;
    fn stream_unprocessed_versions(
        conn: &mut AsyncPgConnection,
        kind: &str,
    ) -> impl Future<
        Output = QueryResult<
            impl Stream<Item = QueryResult<ChronEntity<serde_json::Value>>> + Send,
        >,
    > + Send;
}

impl<VersionIngest: IngestibleFromVersions + Send + Sync + 'static> Stage2Ingest<VersionIngest> {
    pub fn new(kind: &'static str, version_ingest: VersionIngest) -> Self {
        Stage2Ingest {
            kind,
            version_ingest,
        }
    }

    pub fn full_name(&self) -> String {
        format!("{} Stage 2", self.kind)
    }

    async fn run(self: Arc<Self>, args: ProcessingArgs) -> Result<(), IngestFatalError> {
        let partitioner = Partitioner::new(args.parallelism);

        // Task names have to outlive their tasks, so we build then in advance
        let task_names_and_nums = (0..args.parallelism.get())
            .map(|worker_idx| {
                (
                    format!("{} Stage 2 worker {}", self.kind, worker_idx),
                    worker_idx,
                )
            })
            .collect_vec();

        let tasks = task_names_and_nums
            .iter()
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

        // TODO Pool this too?
        let url = mmoldb_db::postgres_url_from_environment();
        let mut async_conn = AsyncPgConnection::establish(&url).await?;

        // Probably not all of this needs to be in the loop but I'm tired, boss
        loop {
            let versions_stream = VersionIngest::stream_unprocessed_versions(
                &mut async_conn,
                self.kind,
            )
            .await?;
            pin_mut!(versions_stream);

            while let Some(version_result) = versions_stream.next().await {
                let version = version_result?;
                let assigned_worker = partitioner.partition_for(&version.entity_id)?;
                // This panics on OOB, which is correct
                let (pipe, _) = &tasks[assigned_worker];

                // If the send fails it's probably because a child errored. Propagate child
                // errors first
                if let Err(pipe_err) = pipe.send(version).await {
                    warn!(
                        "Got a pipe error, which probably means there's an error in a child task. Joining child tasks..."
                    );
                    for (pipe, task) in tasks.into_iter() {
                        drop(pipe); // Signals child to exit
                        task.await.map_err(IngestFatalError::JoinError)??;
                    }
                    warn!("No child tasks exited with errors. Propagating the pipe error instead.");
                    return Err(IngestFatalError::SendFailed(pipe_err));
                }
            }

            info!(
                "{} stage 2 ingest coordinator has processed all available versions. Waiting to be waken up or exited...",
                self.kind
            );
            // if let Some(notify) = &args.waker_from_prev_stage {
            //     tokio::select! {
            //         biased; // We want to always
            //         _ = notify.notified() => {
            //             info!("{} stage 2 ingest coordinator was woken up", self.kind);
            //         }
            //         _ = args.prev_stage_finished.cancelled() => {
            //             info!("{} stage 2 ingest coordinator exiting because prev_stage_finished was set at the end of the loop", self.kind);
            //             break;
            //         }
            //     }
            // } else {
            info!(
                "{} stage 2 ingest coordinator exiting because there is no waker from the previous stage",
                self.kind
            );
            break;
            // }
        }

        // Drop all the senders. This causes the receivers to output None, which is the
        // signal the workers use to know when to exit.
        let tasks = tasks.into_iter().map(|(_, task)| task).collect_vec();

        debug!(
            "All versions for {} Stage 2 are dispatched to workers. Waiting for workers to exit.",
            self.kind
        );

        for task in tasks {
            task.await.map_err(IngestFatalError::JoinError)??;
        }

        debug!(
            "All workers for {} finished. Exiting coordinator.",
            self.kind
        );
        // args.wake_next_stage.notify_one();

        Ok(())
    }

    // Worker could probably take a conn instead of the whole pool, but the cost is negligible
    async fn worker(
        self: Arc<Self>,
        args: ProcessingArgs,
        version_recv: Receiver<ChronEntity<serde_json::Value>>,
        worker_idx: usize,
    ) -> Result<(), IngestFatalError> {
        let val = self.worker_internal(args, version_recv, worker_idx).await;
        if let Err(err) = &val {
            error!("Error in stage 2 ingest worker: {err}");
        }
        val
    }

    async fn worker_internal(
        self: Arc<Self>,
        args: ProcessingArgs,
        version_recv: Receiver<ChronEntity<serde_json::Value>>,
        worker_idx: usize,
    ) -> Result<(), IngestFatalError> {
        info!(
            "{} stage 2 ingest worker {} launched",
            self.kind, worker_idx
        );
        let mut conn = args.pool.get()?;
        let taxa = Taxa::new(&mut conn)?;

        let mut last_print = Utc::now();
        let mut cache = HashMap::new();
        let chunk_stream = tokio_stream::wrappers::ReceiverStream::new(version_recv)
            .filter_map(|version| {
                std::future::ready({
                    let trimmed_version = VersionIngest::trim_unused(&version.data);

                    let now = Utc::now();
                    if last_print + chrono::Duration::seconds(5) < now {
                        info!(
                            "{} cache has {} items and occupies {} bytes",
                            self.kind,
                            cache.len(),
                            cache.allocation_size()
                        );
                        last_print = now;
                    }

                    match cache.entry(version.entity_id.clone()) {
                        Entry::Vacant(vacant) => {
                            // We store the trimmed version for future comparisons
                            vacant.insert(trimmed_version);
                            // We need to return the untrimmed version, or else deserialize will fail
                            Some(version)
                        }
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
                })
            })
            .chunks(args.process_batch_size.into());
        pin_mut!(chunk_stream);

        while let Some(raw_versions) = chunk_stream.next().await {
            self.ingest_page(
                &taxa,
                raw_versions,
                &mut conn,
                worker_idx,
                args.debug_db_insert_delay,
            )?;
        }

        debug!(
            "{} stage 2 ingest worker {} is exiting",
            self.kind, worker_idx
        );

        Ok(())
    }

    fn ingest_page(
        &self,
        taxa: &Taxa,
        raw_versions: Vec<ChronEntity<serde_json::Value>>,
        conn: &mut PgConnection,
        worker_id: usize,
        debug_db_insert_delay: f64,
    ) -> Result<i32, IngestFatalError> {
        debug!(
            "Starting ingest of {} {}(s) on worker {worker_id}",
            raw_versions.len(),
            self.kind
        );
        let save_start = Utc::now();

        let deserialize_start = Utc::now();
        let (entities, deserialize_errors): (Vec<_>, Vec<_>) =
            raw_versions.into_par_iter().partition_map(|entity| {
                if entity.kind != self.kind {
                    warn!("{} ingest task got a {} entity!", self.kind, entity.kind);
                }

                let des = entity.data.into_deserializer();
                match serde_path_to_error::deserialize(des) {
                    Ok(data) => Either::Left(ChronEntity {
                        kind: entity.kind,
                        entity_id: entity.entity_id,
                        valid_from: entity.valid_from,
                        valid_to: entity.valid_to,
                        data,
                    }),
                    Err(err) => Either::Right((err, entity.entity_id, entity.valid_from)),
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

        let new_ingest_logs = deserialize_errors
            .iter()
            .map(|(err, entity_id, valid_from)| NewVersionIngestLog {
                kind: self.kind,
                entity_id,
                valid_from: valid_from.naive_utc(),
                log_index: 0, // Deserialize error is always the 0th log item for that version
                log_level: 0, // Critical error
                log_text: format!("Error deserializing: {:?}", err), // Not sure whether this should be debug
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

        // Insert a layer of Option so we can remove entries from the middle without
        // shifting indices around
        let mut entities = entities.into_iter().map(Some).collect_vec();

        let entity_ids_indices = entities
            .iter()
            .enumerate()
            .map(|(index, version)| {
                let version = version
                    .as_ref()
                    .expect("Every element should be a Some at this point");
                (VersionIngest::batch_split_key(version), index)
            })
            .collect_vec();

        // The handling of valid_until is entirely in the database layer, but its logic
        // requires that a given batch of {kind}s does not have the same team twice. We
        // provide that guarantee here.
        let entities_len = entities.len();
        let mut total_inserted = 0;
        for batch_indices in batch_by_entity(entity_ids_indices) {
            if debug_db_insert_delay > 0. {
                std::thread::sleep(std::time::Duration::from_secs_f64(debug_db_insert_delay));
            }
            let to_insert = batch_indices.len();
            info!(
                "Sending {} new {} versions out of {} to the database.",
                to_insert, self.kind, entities_len,
            );

            let batch = batch_indices
                .iter()
                .map(|index| {
                    std::mem::take(&mut entities[*index])
                        .expect("Batcher must only return each index once")
                })
                .collect_vec();

            let (total, inserted) = VersionIngest::insert_batch(conn, taxa, &batch)?;
            total_inserted += inserted as i32;

            info!(
                "Sent rows for {} new {} versions out of {} to the database. \
                {inserted}/{total} rows were actually inserted, the rest were \
                duplicates. Currently processing versions from {human_time_ago}.",
                to_insert, self.kind, entities_len,
            );
        }

        let save_duration = (Utc::now() - save_start).as_seconds_f64();

        info!(
            "Ingested page of {} {} versions in {save_duration:.3} seconds.",
            entities_len, self.kind,
        );

        Ok(total_inserted)
    }
}

impl<VersionIngest: IngestibleFromVersions + Send + Sync + 'static> IngestStage
    for Stage2Ingest<VersionIngest>
{
    fn name(&self) -> &'static str {
        "Stage 2"
    }
}

pub fn batch_by_entity<KeyT: Clone + Eq + Hash, DataT>(
    versions: Vec<(KeyT, DataT)>,
) -> impl Iterator<Item = Vec<DataT>> {
    let mut pending_versions = versions;
    iter::from_fn(move || {
        if pending_versions.is_empty() {
            return None; // End iteration
        }

        let mut this_batch = HashMap::new();

        // Pull out all players who don't yet appear
        let mut remaining_versions = std::mem::take(&mut pending_versions)
            .into_iter()
            .flat_map(|(key, version)| {
                match this_batch.entry(key.clone()) {
                    Entry::Occupied(_) => {
                        // Then retain this version for the next batch
                        Some((key, version))
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

#[derive(Debug, Copy, Clone)]
pub enum VersionedIngestKind {
    Team,
    Player,
}

impl VersionedIngestKind {
    fn as_kind(self) -> &'static str {
        match self {
            VersionedIngestKind::Team => "team",
            VersionedIngestKind::Player => "player",
        }
    }

    fn as_feed_event_kind(self) -> &'static str {
        match self {
            VersionedIngestKind::Team => "team_feed",
            VersionedIngestKind::Player => "player_feed",
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub enum EntityIngestKind {
    Game,
}

impl EntityIngestKind {
    fn as_kind(self) -> &'static str {
        match self {
            EntityIngestKind::Game => "game",
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub enum IngestKind {
    Versioned(VersionedIngestKind),
    Feed(VersionedIngestKind),
    Entity(EntityIngestKind),
}

impl Display for IngestKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            IngestKind::Versioned(k) => write!(f, "{}", k.as_kind()),
            IngestKind::Feed(k) => write!(f, "{}", k.as_feed_event_kind()),
            IngestKind::Entity(k) => write!(f, "{}", k.as_kind()),
        }
    }
}

#[derive(Debug)]
pub struct IngestForKind {
    kind: IngestKind,
    fetch_args: ChronFetchArgs,
    processing_args: ProcessingArgs,
}

impl IngestForKind {
    pub fn new(
        kind: IngestKind,
        fetch_args: ChronFetchArgs,
        processing_args: ProcessingArgs,
    ) -> Self {
        Self {
            kind,
            fetch_args,
            processing_args,
        }
    }

    pub fn kind(&self) -> IngestKind {
        self.kind
    }

    pub fn fetch_is_enabled(&self) -> bool {
        self.fetch_args.enabled
    }

    pub fn processing_is_enabled(&self) -> bool {
        self.processing_args.enabled
    }

    /// The indefinite fetch task. Repeats until canceled.
    pub async fn fetch_task(&self) -> Result<(), IngestFatalError> {
        let mut interval = tokio::time::interval(Duration::from_secs(
            self.fetch_args.chron_fetch_interval_seconds,
        ));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        while !self.fetch_args.shutdown_requested.is_cancelled() {
            debug!("Sleeping until it's time for the next {} fetch", self.kind);
            tokio::select! {
                biased;
                _ = self.fetch_args.shutdown_requested.cancelled() => {
                    break; // Shutdown requested, break and return immediately
                }
                _ = interval.tick() => {}, // Tick finishes, just proceed with the loop
            }

            info!("Beginning next {} fetch", self.kind);
            self.fetch_all_available().await?;
        }

        Ok(())
    }

    /// One single instance of fetch. Exits once Chron says we're caught up,
    /// or when canceled.
    async fn fetch_all_available(&self) -> Result<(), IngestFatalError> {
        match self.kind {
            IngestKind::Versioned(kind) => {
                fetch::fetch_version_kind(kind.as_kind(), self.fetch_args.clone())
                    .instrument(info_span!("fetch_task", kind = kind.as_kind()))
                    .await
            }
            IngestKind::Feed(kind) => {
                fetch::fetch_feed_event_version_kind(
                    kind.as_feed_event_kind(),
                    self.fetch_args.clone(),
                )
                .instrument(info_span!("fetch_task", kind = kind.as_feed_event_kind()))
                .await
            }
            IngestKind::Entity(kind) => {
                fetch::fetch_entity_kind(kind.as_kind(), self.fetch_args.clone())
                    .instrument(info_span!("fetch_task", kind = kind.as_kind()))
                    .await
            }
        }
    }

    /// The indefinite processing task. Repeats until canceled.
    pub async fn processing_task(&self) -> Result<(), IngestFatalError> {
        let mut interval = tokio::time::interval(Duration::from_secs(
            self.processing_args.processing_interval_seconds,
        ));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        while !self.processing_args.shutdown_requested.is_cancelled() {
            debug!(
                "Sleeping until it's time for the next {:?} processing",
                self.kind
            );
            tokio::select! {
                biased;
                _ = self.processing_args.shutdown_requested.cancelled() => {
                    break; // Shutdown requested, break and return immediately
                }
                _ = interval.tick() => {}, // Tick finishes, just proceed with the loop
            }

            info!("Beginning next {:?} processing", self.kind);
            self.processing_all_available().await?;
        }

        Ok(())
    }

    /// One single instance of processing. Exits once Chron says we're caught up,
    /// or when canceled.
    async fn processing_all_available(&self) -> Result<(), IngestFatalError> {
        match self.kind {
            IngestKind::Versioned(kind) => {
                processing::process_version_kind(kind.as_kind(), self.processing_args.clone())
                    .instrument(info_span!("processing_task", kind = kind.as_kind()))
                    .await
            }
            IngestKind::Feed(kind) => {
                processing::process_feed_event_version_kind(
                    kind.as_feed_event_kind(),
                    self.processing_args.clone(),
                )
                .instrument(info_span!(
                    "processing_task",
                    kind = kind.as_feed_event_kind()
                ))
                .await
            }
            IngestKind::Entity(kind) => {
                processing::process_entity_kind(kind.as_kind(), self.processing_args.clone())
                    .instrument(info_span!("processing_task", kind = kind.as_kind()))
                    .await
            }
        }
    }
}

pub fn ingest_kinds(
    shutdown_requested: &CancellationToken,
    pool: &ConnectionPool,
    config: &'static IngestConfig,
) -> Vec<Arc<IngestForKind>> {
    let kinds_configs = [
        (
            IngestKind::Versioned(VersionedIngestKind::Team),
            &config.team_ingest,
        ),
        (
            IngestKind::Feed(VersionedIngestKind::Team),
            &config.team_feed_ingest,
        ),
        (
            IngestKind::Versioned(VersionedIngestKind::Player),
            &config.player_ingest,
        ),
        (
            IngestKind::Feed(VersionedIngestKind::Player),
            &config.player_feed_ingest,
        ),
        (
            IngestKind::Entity(EntityIngestKind::Game),
            &config.game_ingest,
        ),
    ];

    kinds_configs
        .into_iter()
        .map(|(kind, kind_config)| {
            let fetch_args = ChronFetchArgs {
                shutdown_requested: shutdown_requested.clone(),
                pool: pool.clone(),
                use_local_cheap_cashews: config.use_local_cheap_cashews,
                enabled: kind_config.enable_fetch,
                chron_fetch_interval_seconds: kind_config.chron_fetch_interval_seconds,
                chron_fetch_batch_size: kind_config.chron_fetch_batch_size,
                insert_raw_entity_batch_size: kind_config.insert_raw_entity_batch_size,
            };

            let parallelism = kind_config.ingest_parallelism.unwrap_or_else(|| {
                NonZero::new(1).unwrap()
                // TODO Either restore parallelism or delete this
                // std::thread::available_parallelism().unwrap_or_else(|e| {
                //     warn!("Can't get hardware parallelism. Defaulting to 1. {e}");
                //     NonZero::new(1).unwrap()
                // })
            });

            let processing_args = ProcessingArgs {
                shutdown_requested: shutdown_requested.clone(),
                pool: pool.clone(),
                enabled: kind_config.enable_processing,
                processing_interval_seconds: kind_config.processing_interval_seconds,
                parallelism,
                process_batch_size: kind_config.process_batch_size,
                debug_db_insert_delay: kind_config.debug_db_insert_delay,
            };
            Arc::new(IngestForKind::new(kind, fetch_args, processing_args))
        })
        .collect()
}
