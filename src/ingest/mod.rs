mod sim;
mod chron;

// Reexports
pub use sim::{EventDetail, EventDetailFielder, EventDetailRunner, IngestLog};

// Third party dependencies
use chrono::{DateTime, TimeZone, Utc};
use chrono_humanize::HumanTime;
use log::{error, info, warn};
use mmolb_parsing::ParsedEventMessage;
use rocket::fairing::{Fairing, Info, Kind};
use rocket::tokio::sync::{Notify, RwLock};
use rocket::tokio::task::JoinHandle;
use rocket::{Orbit, Rocket, Shutdown, tokio, figment};
use serde::Deserialize;
use std::{iter, mem};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use diesel::PgConnection;
use futures::{future};
use itertools::{Itertools, izip};
use rocket_sync_db_pools::{Connection, ConnectionPool};
use thiserror::Error;

// First party dependencies
use crate::ingest::chron::{ChronEntities, ChronEntity, ChronGetError, GameExt};
use crate::db::{CompletedGameForDb, GameForDb, RowToEventError, Taxa, Timings};
use crate::ingest::sim::{Game, SimFatalError};
use crate::{Db, db};

fn default_ingest_period() -> u64 {
    30 * 60  // 30 minutes, expressed in seconds
}

fn default_page_size() -> usize {
    1000
}

fn default_ingest_parallelism() -> usize {
    match std::thread::available_parallelism() {
        Ok(parallelism) => parallelism.into(),
        Err(err) => {
            warn!("Unable to detect available parallelism (falling back to 1): {}", err);
            1
        }
    }
}

#[derive(Clone, Deserialize)]
// Not sure if I need this because I also depend on serde, but it
// probably doesn't hurt
#[serde(crate = "rocket::serde")]
struct IngestConfig {
    #[serde(default = "default_ingest_period")]
    ingest_period_sec: u64,
    #[serde(default)]
    reimport_all_games: bool,
    #[serde(default)]
    start_ingest_every_launch: bool,
    #[serde(default = "default_page_size")]
    game_list_page_size: usize,
    #[serde(default = "default_ingest_parallelism")]
    ingest_parallelism: usize,
    cache_path: PathBuf,
}

#[derive(Debug, Error)]
pub enum IngestSetupError {
    #[error("Database error during ingest setup: {0}")]
    DbSetupError(#[from] diesel::result::Error),

    #[error("Couldn't get a database connection")]
    CouldNotGetConnection,

    #[error("Ingest task transitioned away from NotStarted before liftoff")]
    LeftNotStartedTooEarly,
}

#[derive(Debug, Error)]
pub enum IngestFatalError {
    #[error("The ingest task was interrupted while manipulating the task state")]
    InterruptedManipulatingTaskState,

    #[error("Ingest task transitioned to NotStarted state from some other state")]
    ReenteredNotStarted,
    
    #[error(transparent)]
    OpenCacheDbError(#[from] chron::ChronOpenError),

    #[error("Couldn't get a database connection")]
    CouldNotGetConnection,

    #[error(transparent)]
    ChronError(#[from] ChronGetError),

    #[error(transparent)]
    DbError(#[from] diesel::result::Error),

    #[error(transparent)]
    JoinError(#[from] tokio::task::JoinError),
    
    #[error("User requested early termination")]
    ShutdownRequested,
}

// This is the publicly visible version of IngestTaskState
#[derive(Debug)]
pub enum IngestStatus {
    Starting,
    FailedToStart(String),
    Idle,
    Running,
    ExitedWithError(String),
    ShuttingDown,
}

#[derive(Debug)]
enum IngestTaskState {
    // Ingest is not yet started. This state should be short-lived.
    // Notify is used to notify the task when the state has
    // transitioned to Idle.
    NotStarted(Arc<Notify>),
    // Ingest failed to start. This is a terminal state.
    FailedToStart(IngestSetupError),
    // Ingest task is alive, but not doing anything. The ingest task
    // runner must still make sure the thread exits shortly after the
    // state transitions to ShutdownRequested.
    Idle(JoinHandle<()>),
    // Ingest task is and doing an ingest. The ingest task runner must
    // still sure the thread exits shortly after the state transitions
    // to ShutdownRequested. The exit should be graceful, stopping the
    // current ingest.
    Running(JoinHandle<()>),
    // Rocket has requested a shutdown (e.g. due to ctrl-c). If the
    // task doesn't stop within a small time window, it will be killed.
    ShutdownRequested,
    // The ingest task has exited with a fatal error. This is only for
    // tasks which make the ingest
    ExitedWithError(IngestFatalError),
}

#[derive(Clone)]
pub struct IngestTask {
    state: Arc<RwLock<IngestTaskState>>,
}

impl IngestTask {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(IngestTaskState::NotStarted(Arc::new(
                Notify::new(),
            )))),
        }
    }

    pub async fn state(&self) -> IngestStatus {
        let task_state = self.state.read().await;
        match &*task_state {
            IngestTaskState::NotStarted(_) => IngestStatus::Starting,
            IngestTaskState::FailedToStart(err) => IngestStatus::FailedToStart(err.to_string()),
            IngestTaskState::Idle(_) => IngestStatus::Idle,
            IngestTaskState::Running(_) => IngestStatus::Running,
            IngestTaskState::ShutdownRequested => IngestStatus::ShuttingDown,
            IngestTaskState::ExitedWithError(err) => IngestStatus::ExitedWithError(err.to_string()),
        }
    }
}

pub struct IngestFairing;

impl IngestFairing {
    pub fn new() -> Self {
        Self
    }
}

#[rocket::async_trait]
impl Fairing for IngestFairing {
    fn info(&self) -> Info {
        Info {
            name: "Ingest",
            kind: Kind::Liftoff | Kind::Shutdown,
        }
    }

    async fn on_liftoff(&self, rocket: &Rocket<Orbit>) {
        let Some(pool) = Db::pool(rocket) else {
            error!("Cannot launch ingest task: Rocket is not managing a database pool!");
            return;
        };

        let Some(task) = rocket.state::<IngestTask>() else {
            error!("Cannot launch ingest task: Rocket is not managing an IngestTask!");
            return;
        };

        let Some(cache_path) = figment::providers::Env::var("HTTP_CACHE_DIR") else {
            error!("Cannot launch ingest task: HTTP_CACHE_DIR environment variable is not set");
            return;
        };
        
        let augmented_figment = rocket.figment()
            .clone() // Might be able to get away without this but. eh
            .join(("cache_path", cache_path));
        let config = match augmented_figment.extract() {
            Ok(config) => config,
            Err(err) => {
                error!(
                    "Cannot launch ingest task: Failed to parse ingest configuration: {}",
                    err
                );
                return;
            }
        };

        let notify = {
            let mut task_status = task.state.write().await;
            if let IngestTaskState::NotStarted(notify) = &*task_status {
                notify.clone()
            } else {
                *task_status =
                    IngestTaskState::FailedToStart(IngestSetupError::LeftNotStartedTooEarly);
                return;
            }
        };

        let shutdown = rocket.shutdown();

        match launch_ingest_task(pool.clone(), config, task.clone(), shutdown).await {
            Ok(handle) => {
                *task.state.write().await = IngestTaskState::Idle(handle);
                notify.notify_waiters();
            }
            Err(err) => {
                log::error!("Failed to launch ingest task: {}", err);
                *task.state.write().await = IngestTaskState::FailedToStart(err);
            }
        };
    }

    async fn on_shutdown(&self, rocket: &Rocket<Orbit>) {
        if let Some(task) = rocket.state::<IngestTask>() {
            // Signal that we would like to shut down please
            let prev_state = {
                let mut state = task.state.write().await;
                mem::replace(&mut *state, IngestTaskState::ShutdownRequested)
            };
            // If we have a join handle, try to shut down gracefully
            if let IngestTaskState::Idle(handle) | IngestTaskState::Running(handle) = prev_state {
                info!("Shutting down Ingest task");
                if let Err(e) = handle.await {
                    // Not much I can do about a failed join
                    error!("Failed to shut down Ingest task: {}", e);
                }
            } else {
                error!(
                    "Ingest task is in non-Idle or Running state at shutdown: {:?}",
                    prev_state
                );
            }
        } else {
            error!("Rocket is not managing an IngestTask!");
        };
    }
}

// This function sets up the ingest task, then returns a JoinHandle for
// the ingest task. Errors in setup are propagated to the caller.
// Errors in the task itself are handled within the task.
async fn launch_ingest_task(
    pool: ConnectionPool<Db, PgConnection>,
    config: IngestConfig,
    task: IngestTask,
    shutdown: Shutdown,
) -> Result<JoinHandle<()>, IngestSetupError> {
    let Some(conn) = pool.get().await else {
        return Err(IngestSetupError::CouldNotGetConnection)
    };

    let (taxa, previous_ingest_start_time) = conn.run(move |conn| {
        let taxa = Taxa::new(conn)?;

        let previous_ingest_start_time = if config.start_ingest_every_launch {
            None
        } else {
            db::latest_ingest_start_time(conn)?
                .map(|t| Utc.from_utc_datetime(&t))
        };

        Ok::<_, IngestSetupError>((taxa, previous_ingest_start_time))
    }).await?;

    Ok(tokio::spawn(ingest_task_runner(
        pool,
        taxa,
        config,
        task,
        previous_ingest_start_time,
        shutdown,
    )))
}

async fn ingest_task_runner(
    pool: ConnectionPool<Db, PgConnection>,
    taxa: Taxa,
    config: IngestConfig,
    task: IngestTask,
    mut previous_ingest_start_time: Option<DateTime<Utc>>,
    mut shutdown: Shutdown,
) {
    let ingest_period = Duration::from_secs(config.ingest_period_sec);
    loop {
        let (tag, ingest_start) = if let Some(prev_start) = previous_ingest_start_time {
            let next_start = prev_start + ingest_period;
            let wait_duration_chrono = next_start - Utc::now();

            // std::time::Duration can't represent negative durations.
            // Conveniently, the conversion from chrono to std also
            // performs the negativity check we would be doing anyway.
            match wait_duration_chrono.to_std() {
                Ok(wait_duration) => {
                    info!("Next ingest {}", HumanTime::from(wait_duration_chrono));
                    tokio::select! {
                        _ = tokio::time::sleep(wait_duration) => {},
                        _ = &mut shutdown => { break; }
                    }

                    let tag = format!("after waiting {}", HumanTime::from(wait_duration_chrono));
                    (tag, next_start)
                }
                Err(_) => {
                    // Indicates the wait duration was negative
                    let tag = format!(
                        "immediately (next scheduled ingest was {})",
                        HumanTime::from(next_start)
                    );
                    (tag, Utc::now())
                }
            }
        } else {
            let tag = "immediately (this is the first ingest)".to_string();
            (tag, Utc::now())
        };

        // I put this outside the `if` cluster intentionally, so the
        // compiler will error if I miss a code path
        info!("Starting ingest {tag}");
        previous_ingest_start_time = Some(ingest_start);

        // Try to transition from idle to running, with lots of
        // recovery behaviors
        loop {
            let mut task_state = task.state.write().await;
            // We can't atomically move handle from idle to running,
            // but we can replace it with the error that should happen
            // if something were to interrupt us. This should never be
            // seen by outside code, since we have the state locked.
            // It's just defensive programming.
            let prev_state = mem::replace(
                &mut *task_state,
                IngestTaskState::ExitedWithError(
                    IngestFatalError::InterruptedManipulatingTaskState,
                ),
            );
            match prev_state {
                IngestTaskState::NotStarted(notify) => {
                    // Put the value back how it was, drop the mutex, and wait.
                    // Once the wait is done we'll go round the loop again and
                    // this time should be in Idle state
                    let my_notify = notify.clone();
                    *task_state = IngestTaskState::NotStarted(notify);
                    drop(task_state);
                    my_notify.notified().await;
                }
                IngestTaskState::FailedToStart(err) => {
                    // Being in this state here is an oxymoron. Replacing the
                    // value and terminating this task is the least surprising
                    // way to react
                    *task_state = IngestTaskState::FailedToStart(err);
                    return;
                }
                IngestTaskState::Idle(handle) => {
                    // This is the one we want!
                    *task_state = IngestTaskState::Running(handle);
                    break;
                }
                IngestTaskState::Running(handle) => {
                    // Shouldn't happen, but if it does recovery is easy
                    warn!("Ingest task state was Running at the top of the loop (expected Idle)");
                    *task_state = IngestTaskState::Running(handle);
                    break;
                }
                IngestTaskState::ShutdownRequested => {
                    // Shutdown requested? Sure, we can do that.
                    *task_state = IngestTaskState::ShutdownRequested;
                    return;
                }
                IngestTaskState::ExitedWithError(err) => {
                    // This one is also an oxymoron
                    *task_state = IngestTaskState::ExitedWithError(err);
                    return;
                }
            }
        }

        let ingest_result = tokio::select! {
            ingest_result = do_ingest(pool.clone(), &taxa, &config, ingest_start) => {
                ingest_result
            },
            _ = &mut shutdown => {
                info!("Graceful shutdown during ingest");
                return;
            }
        };

        let Some(conn) = pool.get().await else {
            let mut task_state = task.state.write().await;
            *task_state = IngestTaskState::ExitedWithError(
                IngestFatalError::CouldNotGetConnection,
            );
            return;
        };

        conn.run(move |conn| {
            match ingest_result {
                Ok((ingest_id, start_next_ingest_at_page)) => {
                    let ingest_end = Utc::now();
                    match db::mark_ingest_finished(conn, ingest_id, ingest_end, start_next_ingest_at_page.as_deref()) {
                        Ok(()) => {
                            info!(
                                "Marked ingest {ingest_id} finished {:#}.",
                                HumanTime::from(ingest_end - ingest_start),
                            );
                        }
                        Err(err) => {
                            warn!("Failed to mark game ingest as finished: {}", err);
                        }
                    }
                }
                Err((err, ingest_id)) => {
                    if let Some(ingest_id) = ingest_id {
                        warn!(
                            "Fatal error in game ingest: {}. This ingest is aborted.",
                            err
                        );
                        match db::mark_ingest_aborted(conn, ingest_id, Utc::now()) {
                            Ok(()) => {}
                            Err(err) => {
                                warn!("Failed to mark game ingest as aborted: {}", err);
                            }
                        }
                    } else {
                        warn!(
                            "Fatal error in game ingest before adding ingest to database: {}. \
                            This ingest is aborted.",
                            err,
                        );
                    }
                }
            }
        }).await;

        // Try to transition from running to idle, with lots of
        // recovery behaviors
        {
            let mut task_state = task.state.write().await;
            // We can't atomically move handle from idle to running,
            // but we can replace it with the error that should happen
            // if something were to interrupt us. This should never be
            // seen by outside code, since we have the state locked.
            // It's just defensive programming.
            let prev_state = mem::replace(
                &mut *task_state,
                IngestTaskState::ExitedWithError(
                    IngestFatalError::InterruptedManipulatingTaskState,
                ),
            );
            match prev_state {
                IngestTaskState::NotStarted(_) => {
                    *task_state =
                        IngestTaskState::ExitedWithError(IngestFatalError::ReenteredNotStarted);
                    return;
                }
                IngestTaskState::FailedToStart(err) => {
                    // Being in this state here is an oxymoron. Replacing the
                    // value and terminating this task is the least surprising
                    // way to react
                    *task_state = IngestTaskState::FailedToStart(err);
                    return;
                }
                IngestTaskState::Running(handle) => {
                    // This is the one we want!
                    *task_state = IngestTaskState::Idle(handle);
                }
                IngestTaskState::Idle(handle) => {
                    // Shouldn't happen, but if it does recovery is easy
                    warn!(
                        "Ingest task state was Idle at the bottom of the loop (expected Running)"
                    );
                    *task_state = IngestTaskState::Idle(handle);
                }
                IngestTaskState::ShutdownRequested => {
                    // Shutdown requested? Sure, we can do that.
                    *task_state = IngestTaskState::ShutdownRequested;
                    return;
                }
                IngestTaskState::ExitedWithError(err) => {
                    // This one is also an oxymoron
                    *task_state = IngestTaskState::ExitedWithError(err);
                    return;
                }
            }
        }
    }
}

async fn do_ingest(
    pool: ConnectionPool<Db, PgConnection>,
    taxa: &Taxa,
    config: &IngestConfig,
    ingest_start: DateTime<Utc>,
) -> Result<(i64, Option<String>), (IngestFatalError, Option<i64>)> {
    info!("Ingest at {ingest_start} starting");

    // The only thing that errors here is opening the http cache, which
    // could be worked around by just not caching. I don't currently support
    // running without a cache but it could be added fairly easily.
    let chron = chron::Chron::new(&config.cache_path, config.game_list_page_size)
        .map_err(|err| (err.into(), None))?;

    info!("Initialized chron");

    let conn= pool.get().await
        .ok_or_else(|| (IngestFatalError::CouldNotGetConnection, None))?;

    // For lifetime reasons
    let reimport_all_games = config.reimport_all_games;
    let (start_page, ingest_id) = conn.run(move |conn| {
        let start_page = if reimport_all_games {
            None
        } else {
            db::next_ingest_start_page(conn)
                .map_err(|e| (e.into(), None))?
        };

        info!("Got last completed page");

        let ingest_id = db::start_ingest(conn, ingest_start)
            .map_err(|e| (e.into(), None))?;

        info!("Inserted new ingest");

        Ok((start_page, ingest_id))
    }).await?;

    info!("Finished first db block");

    do_ingest_internal(
        pool,
        taxa,
        config,
        &chron,
        ingest_id,
        start_page,
    )
    .await
    .map_err(|e| (e.into(), Some(ingest_id)))
}

struct IngestStats {
    pub num_ongoing_games_skipped: usize,
    pub num_terminal_incomplete_games_skipped: usize,
    pub num_already_ingested_games_skipped: usize,
    pub num_games_imported: usize,
}

impl IngestStats {
    pub fn new() -> Self {
        Self {
            num_ongoing_games_skipped: 0,
            num_terminal_incomplete_games_skipped: 0,
            num_already_ingested_games_skipped: 0,
            num_games_imported: 0,
        }
    }

    pub fn add(&mut self, other: &IngestStats) {
        self.num_ongoing_games_skipped += other.num_ongoing_games_skipped;
        self.num_terminal_incomplete_games_skipped += other.num_terminal_incomplete_games_skipped;
        self.num_already_ingested_games_skipped += other.num_already_ingested_games_skipped;
        self.num_games_imported += other.num_games_imported;
    }
}

async fn fetch_games_page(
    chron: &chron::Chron,
    page: Option<String>,
    index: usize,
) -> Result<(ChronEntities<mmolb_parsing::Game>, usize, f64), ChronGetError> {
    let fetch_start = Utc::now();
    let page = chron.games_page(page.as_deref()).await?;
    let fetch_duration = (Utc::now() - fetch_start).as_seconds_f64();
    Ok((page, index, fetch_duration))
}

async fn do_ingest_internal(
    pool: ConnectionPool<Db, PgConnection>,
    taxa: &Taxa,
    config: &IngestConfig,
    chron: &chron::Chron,
    ingest_id: i64,
    start_page: Option<String>,
) -> Result<(i64, Option<String>), IngestFatalError> {
    info!("Recorded ingest start in database. Starting ingest...");

    // Track the page the next ingest should resume at. 
    let mut next_ingest_start_page = start_page.clone();
    // We have to stop updating start_next_ingest_at_page as soon as we
    // encounter any incomplete game so we don't skip it when it 
    // finishes.
    let mut freeze_next_index_start_page = false;
    
    // For logging purposes
    let mut pages_finished_before_freeze = 0;
    let mut pages_finished_after_freeze = 0;
    let mut stats = IngestStats::new();

    // TODO Encapsulate this better
    let mut finish_ingest_page = |page_stats, page_token| {
        stats.add(&page_stats);
        freeze_next_index_start_page = freeze_next_index_start_page || stats.num_ongoing_games_skipped > 0;
        if !freeze_next_index_start_page {
            // Note: although next_ingest_start_page is an Option, its None has a different
            // meaning than page_token's None so it's incorrect to just assign page_token to it.
            if let Some(token) = page_token {
                next_ingest_start_page = Some(token);
            }
            pages_finished_before_freeze += 1;
        } else {
            pages_finished_after_freeze += 1;
        }
    };

    let mut ingests_in_progress = VecDeque::new();
    let mut next_page_fut = Some(fetch_games_page(chron, start_page, 0));

    while let Some(page_fut) = next_page_fut.take() {
        let (page, index, fetch_duration) = page_fut.await?;
        if let Some(next_page) = page.next_page.clone() {
            next_page_fut = Some(fetch_games_page(chron, Some(next_page), index + 1));
        }

        while ingests_in_progress.len() >= config.ingest_parallelism {
            // Need to pop and process to make space in the queue
            let (page_stats, page_token) = ingests_in_progress.pop_front()
                .expect("We just checked the len")
                .await??;
            finish_ingest_page(page_stats, page_token);
        }

        // Now there's space in the queue to push a new ingest
        // tokio::spawn makes it start making progress before it's awaited
        let pool = pool.clone();
        let taxa = taxa.clone();
        let config = config.clone();
        ingests_in_progress.push_back(tokio::spawn(
            ingest_page_of_games(pool, taxa, config, ingest_id, index, fetch_duration, page)
        ));
    }

    while let Some(ingest_fut) = ingests_in_progress.pop_front() {
        let (page_stats, page_token) = ingest_fut.await??;
        finish_ingest_page(page_stats, page_token);
    }

    info!("{} games ingested", stats.num_games_imported);
    info!("{} ongoing games skipped", stats.num_ongoing_games_skipped);
    info!("{} terminal incomplete (bugged) games skipped", stats.num_terminal_incomplete_games_skipped);
    info!("{} games already ingested", stats.num_already_ingested_games_skipped);
    info!(
        "{} pages of games completed this time. \
        {} pages will be re-ingested next time to catch incomplete games.",
        pages_finished_before_freeze,
        pages_finished_after_freeze,
    );

    Ok((ingest_id, next_ingest_start_page))
}

fn prepare_completed_game_for_db(
    entity: &ChronEntity<mmolb_parsing::Game>,
) -> Result<CompletedGameForDb, SimFatalError> {
    let parsed_game = mmolb_parsing::process_game(&entity.data);

    // I'm adding enumeration to parsed, then stripping it out for
    // the iterator fed to Game::new, on purpose. I need the
    // counting to count every event, but I don't need the count
    // inside Game::new.
    let mut parsed = parsed_game.iter().zip(&entity.data.event_log).enumerate();

    let (mut game, mut all_logs) = {
        let mut parsed_for_game = (&mut parsed).map(|(_, (parsed, _))| parsed);

        Game::new(&entity.entity_id, &entity.data, &mut parsed_for_game)?
    };

    let detail_events = parsed
        .map(|(game_event_index, (parsed, raw))| {
            // Sim has a different IngestLogs... this made sense at the time
            let mut ingest_logs = sim::IngestLogs::new(game_event_index as i32);

            let unparsed = parsed.clone().unparse();
            if unparsed != raw.message {
                ingest_logs.error(format!(
                    "Round-trip of raw event through ParsedEvent produced a mismatch:\n\
                     Original: <pre>{:?}</pre>\n\
                     Through EventDetail: <pre>{:?}</pre>",
                    raw.message, unparsed,
                ));
            }

            let event = match game.next(game_event_index, &parsed, &raw, &mut ingest_logs) {
                Ok(result) => result,
                Err(e) => {
                    ingest_logs.critical(e.to_string());
                    None
                }
            };

            all_logs.push(ingest_logs.into_vec());

            event
        })
        .collect_vec();

    // Take the None values out of detail_events
    let events = detail_events
        .into_iter()
        .filter_map(|event| event)
        .collect_vec();

    Ok(CompletedGameForDb {
        id: &entity.entity_id,
        raw_game: &entity.data,
        events,
        logs: all_logs,
        parsed_game,
    })
}

async fn ingest_page_of_games<'t>(
    pool: ConnectionPool<Db, PgConnection>,
    taxa: Taxa,
    config: IngestConfig,
    ingest_id: i64,
    page_index: usize,
    fetch_duration: f64,
    mut page: ChronEntities<mmolb_parsing::Game>,
) -> Result<(IngestStats, Option<String>), IngestFatalError> {
    page.items.sort_by(|a, b| a.entity_id.cmp(&b.entity_id));

    let conn = pool.get().await
        .ok_or_else(|| IngestFatalError::CouldNotGetConnection)?;
    
    let page_bytes = allocative::size_of_unique(&page);
    info!("Starting ingest of {} page", humansize::format_size(page_bytes, humansize::DECIMAL));

    let stats = conn.run(move |conn| {
        let save_start = Utc::now();
        let filter_finished_games_start = Utc::now();
        let num_items = page.items.len();
        let raw_games = if !config.reimport_all_games {
            // Remove any games which are fully imported
            let mut is_finished = db::is_finished(conn, page.items.iter().map(|e| e.entity_id.as_str()).collect())?
                .into_iter()
                .peekable();
            page.items.into_iter()
                .filter_map(|game| {
                    if let Some((_, finished)) = is_finished.next_if(|(id, _)| id == &game.entity_id) {
                        if finished {
                            // Game is finished, don't import it again
                            None
                        } else {
                            // Game is not finished, do import it again
                            Some(game)
                        }
                    } else {
                        // Game is not in the db, import it for the first time
                        Some(game)
                    }
                })
                .collect()
        } else {
            page.items
        };
        let filter_finished_games_duration = (Utc::now() - filter_finished_games_start).as_seconds_f64();

        let parse_and_sim_start = Utc::now();
        let games = raw_games.iter()
            .filter_map(|entity| {
                Some(Ok::<_, IngestFatalError>(if !entity.data.is_terminal() {
                    GameForDb::Incomplete {
                        game_id: &entity.entity_id,
                        raw_game: &entity.data,
                    }
                } else if entity.data.is_completed() {
                    match prepare_completed_game_for_db(entity) {
                        Ok(game) => {
                            GameForDb::Completed(game)
                        }
                        Err(err) => {
                            // TODO Surface this error on the games with issues page
                            let description = format!(
                                "{} {} @ {} {} s{}d{}",
                                entity.data.away_team_emoji,
                                entity.data.away_team_name,
                                entity.data.home_team_emoji,
                                entity.data.home_team_name,
                                entity.data.season,
                                entity.data.day,
                            );
                            warn!("Sim fatal error importing {description}: {err}. This game will be skipped.");
                            GameForDb::Incomplete {
                                game_id: &entity.entity_id,
                                raw_game: &entity.data,
                            }
                        }
                    }
                } else {
                    return None
                }))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let num_ongoing_games_skipped = games.iter()
            .filter(|g| {
                match g {
                    GameForDb::Incomplete { .. } => { true }
                    GameForDb::Completed { .. } => { false }
                }
            })
            .count();
        let num_already_ingested_games_skipped = num_items - raw_games.len();
        let num_terminal_incomplete_games_skipped = raw_games.len() - games.len();
        let num_games_imported = games.len() - num_ongoing_games_skipped;
        info!(
            "Ingesting {num_games_imported} games, ignoring {num_already_ingested_games_skipped} \
            already-ingested games, ignoring {num_ongoing_games_skipped} games in progress, and \
            skipping {num_terminal_incomplete_games_skipped} terminal incomplete games.",
        );
        let parse_and_sim_duration = (Utc::now() - parse_and_sim_start).as_seconds_f64();

        let db_insert_start = Utc::now();
        db::insert_games(conn, &taxa, ingest_id, &games)?;
        let db_insert_duration = (Utc::now() - db_insert_start).as_seconds_f64();

        // Immediately turn around and fetch all the games we just inserted,
        // so we can verify that they round-trip correctly.
        // This step, and all the following verification steps, could be
        // skipped. However, my profiling shows that it's negligible
        // cost so I haven't added the capability.
        let db_fetch_for_check_start = Utc::now();
        let mmolb_game_ids = games.iter()
            .filter_map(|game| match game {
                GameForDb::Incomplete { .. } => { None }
                GameForDb::Completed(game) => { Some(game.id) }
            })
            .collect_vec();

        let (ingested_games, events_for_game_timings) = db::events_for_games(conn, &taxa, &mmolb_game_ids)?;
        assert_eq!(mmolb_game_ids.len(), ingested_games.len());
        let db_fetch_for_check_duration = (Utc::now() - db_fetch_for_check_start).as_seconds_f64();
        
        let check_round_trip_start = Utc::now();
        let additional_logs = games.iter()
            .filter_map(|game| match game {
                GameForDb::Incomplete { .. } => { None }
                GameForDb::Completed(game) => { Some(game) }
            })
            .zip(&ingested_games)
            .filter_map(|(game, (game_id, inserted_events))| {
                let detail_events = &game.events; 
                let mut extra_ingest_logs = IngestLogs::new();
                if inserted_events.len() != detail_events.len() {
                    error!(
                        "Number of events read from the db ({}) does not match number of events written to \
                        the db ({})",
                        inserted_events.len(),
                        detail_events.len(),
                    );
                }
                for (reconstructed_detail, original_detail) in izip!(inserted_events, detail_events) {
                    let index = original_detail.game_event_index;
                    let fair_ball_index = original_detail.fair_ball_event_index;
                
                    if let Some(index) = fair_ball_index {
                        check_round_trip(
                            index,
                            &mut extra_ingest_logs,
                            true,
                            &game.parsed_game[index],
                            &original_detail,
                            reconstructed_detail,
                        );
                    }
                
                    check_round_trip(
                        index,
                        &mut extra_ingest_logs,
                        false,
                        &game.parsed_game[index],
                        &original_detail,
                        reconstructed_detail,
                    );
                }
                let extra_ingest_logs = extra_ingest_logs.into_vec();
                if extra_ingest_logs.is_empty() {
                    None
                } else {
                    Some((*game_id, extra_ingest_logs))
                }
            })
            .collect_vec();
        let check_round_trip_duration = (Utc::now() - check_round_trip_start).as_seconds_f64();

        let insert_extra_logs_start = Utc::now();
        if !additional_logs.is_empty() {
            db::insert_additional_ingest_logs(conn, &additional_logs)?;
        }
        let insert_extra_logs_duration = (Utc::now() - insert_extra_logs_start).as_seconds_f64();
        let save_duration = (Utc::now() - save_start).as_seconds_f64();
        
        db::insert_timings(conn, ingest_id, page_index, Timings {
            fetch_duration,
            filter_finished_games_duration,
            parse_and_sim_duration,
            db_insert_duration,
            db_fetch_for_check_duration,
            events_for_game_timings,
            check_round_trip_duration,
            insert_extra_logs_duration,
            save_duration,
        })?;

        Ok::<_, IngestFatalError>(IngestStats {
            num_ongoing_games_skipped,
            num_terminal_incomplete_games_skipped,
            num_already_ingested_games_skipped,
            num_games_imported,
        })
    }).await?;

    Ok((stats, page.next_page))
}


// A utility to more conveniently build a Vec<IngestLog>
pub struct IngestLogs {
    logs: Vec<IngestLog>,
}

impl IngestLogs {
    pub fn new() -> Self {
        Self { logs: Vec::new() }
    }

    #[allow(dead_code)]
    pub fn critical(&mut self, game_event_index: usize, s: impl Into<String>) {
        self.logs.push(IngestLog {
            game_event_index: game_event_index as i32,
            log_level: 0,
            log_text: s.into(),
        });
    }

    pub fn error(&mut self, game_event_index: usize, s: impl Into<String>) {
        self.logs.push(IngestLog {
            game_event_index: game_event_index as i32,
            log_level: 1,
            log_text: s.into(),
        });
    }

    #[allow(dead_code)]
    pub fn warn(&mut self, game_event_index: usize, s: impl Into<String>) {
        self.logs.push(IngestLog {
            game_event_index: game_event_index as i32,
            log_level: 2,
            log_text: s.into(),
        });
    }

    #[allow(dead_code)]
    pub fn info(&mut self, game_event_index: usize, s: impl Into<String>) {
        self.logs.push(IngestLog {
            game_event_index: game_event_index as i32,
            log_level: 3,
            log_text: s.into(),
        });
    }

    #[allow(dead_code)]
    pub fn debug(&mut self, game_event_index: usize, s: impl Into<String>) {
        self.logs.push(IngestLog {
            game_event_index: game_event_index as i32,
            log_level: 4,
            log_text: s.into(),
        });
    }

    #[allow(dead_code)]
    pub fn trace(&mut self, game_event_index: usize, s: impl Into<String>) {
        self.logs.push(IngestLog {
            game_event_index: game_event_index as i32,
            log_level: 5,
            log_text: s.into(),
        });
    }

    pub fn into_vec(self) -> Vec<IngestLog> {
        self.logs
    }
}

fn log_if_error<'g, E: std::fmt::Display>(
    ingest_logs: &mut IngestLogs,
    index: usize,
    to_parsed_result: Result<ParsedEventMessage<&'g str>, E>,
    log_prefix: &str,
) -> Option<ParsedEventMessage<&'g str>> {
    match to_parsed_result {
        Ok(to_contact_result) => Some(to_contact_result),
        Err(err) => {
            ingest_logs.error(index, format!("{log_prefix}: {err}"));
            None
        }
    }
}

// The particular combination of &str and String type arguments is
// dictated by the caller
fn check_round_trip(
    index: usize,
    ingest_logs: &mut IngestLogs,
    is_contact_event: bool,
    parsed: &ParsedEventMessage<&str>,
    original_detail: &EventDetail<&str>,
    reconstructed_detail: &Result<EventDetail<String>, RowToEventError>,
) {
    let Some(parsed_through_detail) = (if is_contact_event {
        log_if_error(
            ingest_logs,
            index,
            original_detail.to_parsed_contact(),
            "Attempt to round-trip contact event through ParsedEventMessage -> EventDetail -> \
            ParsedEventMessage failed at the EventDetail -> ParsedEventMessage step with error",
        )
    } else {
        log_if_error(
            ingest_logs,
            index,
            original_detail.to_parsed(),
            "Attempt to round-trip event through ParsedEventMessage -> EventDetail -> \
            ParsedEventMessage failed at the EventDetail -> ParsedEventMessage step with error",
        )
    }) else {
        return;
    };

    if parsed != &parsed_through_detail {
        ingest_logs.error(
            index,
            format!(
                "Round-trip of {} through EventDetail produced a mismatch:\n\
                 Original: <pre>{:?}</pre>\
                 Through EventDetail: <pre>{:?}</pre>",
                if is_contact_event {
                    "contact event"
                } else {
                    "event"
                },
                parsed,
                parsed_through_detail,
            ),
        );
    }

    let reconstructed_detail = match reconstructed_detail {
        Ok(reconstructed_detail) => reconstructed_detail,
        Err(err) => {
            ingest_logs.error(
                index,
                format!(
                    "Attempt to round-trip {} ParsedEventMessage -> EventDetail -> database \
                    -> EventDetail -> ParsedEventMessage failed at the database -> EventDetail \
                    step with error: {err}",
                    if is_contact_event {
                        "contact event"
                    } else {
                        "event"
                    }
                ),
            );
            return;
        }
    };

    let Some(parsed_through_db) = (if is_contact_event {
        log_if_error(
            ingest_logs,
            index,
            reconstructed_detail.to_parsed_contact(),
            "Attempt to round-trip contact event through ParsedEventMessage -> EventDetail -> \
            database -> EventDetail -> ParsedEventMessage failed at the EventDetail -> \
            ParsedEventMessage step with error",
        )
    } else {
        log_if_error(
            ingest_logs,
            index,
            reconstructed_detail.to_parsed(),
            "Attempt to round-trip event through ParsedEventMessage -> EventDetail -> database \
            -> EventDetail -> ParsedEventMessage failed at the EventDetail -> ParsedEventMessage \
            step with error",
        )
    }) else {
        return;
    };

    if parsed != &parsed_through_db {
        ingest_logs.error(
            index,
            format!(
                "Round-trip of {} through database produced a mismatch:\n\
                 Original: <pre>{:?}</pre>\n\
                 Through database: <pre>{:?}</pre>",
                if is_contact_event {
                    "contact event"
                } else {
                    "event"
                },
                parsed,
                parsed_through_db,
            ),
        );
    }
}
