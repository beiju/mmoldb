mod check_round_trip;
mod sim;
mod worker;

// Reexports
pub use sim::{EventDetail, EventDetailFielder, EventDetailRunner, IngestLog};

// Third party dependencies
use chrono::{DateTime, TimeZone, Utc};
use chrono_humanize::HumanTime;
use diesel::PgConnection;
use log::{error, info, warn};
use rocket::fairing::{Fairing, Info, Kind};
use rocket::tokio::sync::{Notify, RwLock};
use rocket::tokio::task::JoinHandle;
use rocket::{figment, tokio, Orbit, Rocket, Shutdown};
use rocket_sync_db_pools::ConnectionPool;
use serde::Deserialize;
use std::collections::VecDeque;
use std::convert::Infallible;
use std::mem;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use miette::Diagnostic;
use thiserror::Error;

// First party dependencies
use chron::{ChronEntities, ChronStreamError};
use crate::db::Taxa;
use crate::ingest::worker::{IngestWorker, IngestWorkerInProgress};
use crate::{db, Db};

fn default_ingest_period() -> u64 {
    30 * 60 // 30 minutes, expressed in seconds
}

fn default_page_size() -> usize {
    1000
}

fn default_ingest_parallelism() -> usize {
    match std::thread::available_parallelism() {
        Ok(parallelism) => parallelism.into(),
        Err(err) => {
            warn!(
                "Unable to detect available parallelism (falling back to 1): {}",
                err
            );
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
    start_ingest_every_launch: bool,
    #[serde(default)]
    reimport_all_games: bool,
    #[serde(default = "default_page_size")]
    game_list_page_size: usize,
    #[serde(default = "default_ingest_parallelism")]
    ingest_parallelism: usize,
}

#[derive(Debug, Error, Diagnostic)]
pub enum IngestSetupError {
    #[error("Database error during ingest setup: {0}")]
    DbSetupError(#[from] diesel::result::Error),

    #[error("Couldn't get a database connection")]
    CouldNotGetConnection,

    #[error("Ingest task transitioned away from NotStarted before liftoff")]
    LeftNotStartedTooEarly,
}

#[derive(Debug, Error, Diagnostic)]
pub enum IngestFatalError {
    #[error("The ingest task was interrupted while manipulating the task state")]
    InterruptedManipulatingTaskState,

    #[error("Ingest task transitioned to NotStarted state from some other state")]
    ReenteredNotStarted,

    #[error("Couldn't open HTTP cache database: {0}")]
    OpenCacheDbError(#[from] sled::Error),

    #[error("Couldn't get a database connection")]
    CouldNotGetConnection,

    #[error(transparent)]
    ChronError(#[from] ChronStreamError),

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

        let augmented_figment = rocket
            .figment()
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
        return Err(IngestSetupError::CouldNotGetConnection);
    };

    let (taxa, previous_ingest_start_time) = conn
        .run(move |conn| {
            let taxa = Taxa::new(conn)?;

            let previous_ingest_start_time = if config.start_ingest_every_launch {
                None
            } else {
                db::latest_ingest_start_time(conn)?.map(|t| Utc.from_utc_datetime(&t))
            };

            Ok::<_, IngestSetupError>((taxa, previous_ingest_start_time))
        })
        .await?;

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
            *task_state = IngestTaskState::ExitedWithError(IngestFatalError::CouldNotGetConnection);
            return;
        };

        conn.run(move |conn| match ingest_result {
            Ok((ingest_id, start_next_ingest_at_page)) => {
                let ingest_end = Utc::now();
                match db::mark_ingest_finished(
                    conn,
                    ingest_id,
                    ingest_end,
                    start_next_ingest_at_page.as_deref(),
                ) {
                    Ok(()) => {
                        info!(
                            "Marked ingest {ingest_id} finished {:#}.",
                            HumanTime::from(ingest_end - ingest_start),
                        );
                    }
                    Err(err) => {
                        error!("Failed to mark game ingest as finished: {}", err);
                    }
                }
            }
            Err((err, ingest_id)) => {
                if let Some(ingest_id) = ingest_id {
                    error!(
                        "Fatal error in game ingest: {}. This ingest is aborted.",
                        err
                    );
                    match db::mark_ingest_aborted(conn, ingest_id, Utc::now()) {
                        Ok(()) => {}
                        Err(err) => {
                            error!("Failed to mark game ingest as aborted: {}", err);
                        }
                    }
                } else {
                    error!(
                        "Fatal error in game ingest before adding ingest to database: {}. \
                            This ingest is aborted.",
                        err,
                    );
                }
            }
        })
        .await;

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
    let chron = chron::Chron::new(config.game_list_page_size);

    info!("Initialized chron");

    let conn = pool
        .get()
        .await
        .ok_or_else(|| (IngestFatalError::CouldNotGetConnection, None))?;

    // For lifetime reasons
    let reimport_all_games = config.reimport_all_games;
    let (start_page, ingest_id) = conn
        .run(move |conn| {
            let start_page = if reimport_all_games {
                None
            } else {
                db::next_ingest_start_page(conn).map_err(|e| (e.into(), None))?
            };

            info!("Got last completed page");

            let ingest_id = db::start_ingest(conn, ingest_start).map_err(|e| (e.into(), None))?;

            info!("Inserted new ingest");

            Ok((start_page, ingest_id))
        })
        .await?;

    info!("Finished first db block");

    do_ingest_internal(pool, taxa, config, &chron, ingest_id, start_page)
        .await
        .map_err(|e| (e.into(), Some(ingest_id)))
}

struct IngestStats {
    pub num_ongoing_games_skipped: usize,
    pub num_terminal_incomplete_games_skipped: usize,
    pub num_already_ingested_games_skipped: usize,
    pub num_games_with_fatal_errors: usize,
    pub num_games_imported: usize,
}

impl IngestStats {
    pub fn new() -> Self {
        Self {
            num_ongoing_games_skipped: 0,
            num_terminal_incomplete_games_skipped: 0,
            num_already_ingested_games_skipped: 0,
            num_games_with_fatal_errors: 0,
            num_games_imported: 0,
        }
    }

    pub fn add(&mut self, other: &IngestStats) {
        self.num_ongoing_games_skipped += other.num_ongoing_games_skipped;
        self.num_terminal_incomplete_games_skipped += other.num_terminal_incomplete_games_skipped;
        self.num_already_ingested_games_skipped += other.num_already_ingested_games_skipped;
        self.num_games_with_fatal_errors += other.num_games_with_fatal_errors;
        self.num_games_imported += other.num_games_imported;
    }
}

async fn fetch_games_page(
    chron: &chron::Chron,
    page: Option<String>,
    index: usize,
) -> Result<(ChronEntities<mmolb_parsing::Game>, usize, f64), ChronStreamError> {
    let fetch_start = Utc::now();
    let page = todo!();
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
    info!(
        "Recorded ingest start in database. Starting ingest with up to {} workers.",
        config.ingest_parallelism,
    );

    struct PageProgress {
        // Track the page the next ingest should resume at.
        next_ingest_start_page: Option<String>,
        // Informational only
        pages_finished_before_freeze: usize,
        pages_finished_after_freeze: usize,
        stats: IngestStats,
    }

    impl PageProgress {
        fn update(&mut self, stats: IngestStats, page_token: Option<String>) {
            self.stats.add(&stats);
            // We have to stop updating start_next_ingest_at_page as soon as we
            // encounter any incomplete game so we don't skip it when it
            // finishes.
            if !stats.num_ongoing_games_skipped > 0 {
                // Note: although next_ingest_start_page is an Option, its None has a different
                // meaning than page_token's None so it's incorrect to just assign page_token to it.
                if let Some(token) = page_token {
                    self.next_ingest_start_page = Some(token);
                }
                self.pages_finished_before_freeze += 1;
            } else {
                self.pages_finished_after_freeze += 1;
            }
        }
    }

    let mut page_progress = PageProgress {
        next_ingest_start_page: start_page.clone(),
        pages_finished_before_freeze: 0,
        pages_finished_after_freeze: 0,
        stats: IngestStats::new(),
    };

    let mut ingests_in_progress: VecDeque<IngestWorkerInProgress> = VecDeque::new();
    let mut next_page_fut = Some(fetch_games_page(chron, start_page, 0));

    while let Some(page_fut) = next_page_fut.take() {
        // Get the next page of games
        let (page, index, fetch_duration) = page_fut.await?;
        if let Some(next_page) = page.next_page.clone() {
            // TODO I believe this doesn't start making progress until it gets await'ed, which
            //   isn't until the top of the next iteration of the loop. Ideally we would
            //   tokio::spawn it so it starts right away, but there's lifetime issues with that
            next_page_fut = Some(fetch_games_page(chron, Some(next_page), index + 1));
        }

        // Get a worker
        let worker = if ingests_in_progress.front().map_or(false, |i| i.is_ready()) {
            // If the frontmost ingest is already done, we can just reuse its worker
            let outcome = ingests_in_progress
                .pop_front()
                .expect("This branch should never be entered if ingests_in_progress is empty")
                .into_future()
                .await?;
            page_progress.update(outcome.stats, outcome.next_page);
            outcome.worker
        } else if ingests_in_progress.len() < config.ingest_parallelism {
            // Then we still have budget for more workers
            IngestWorker::new(pool.clone(), taxa.clone(), config.clone(), ingest_id)
        } else {
            // Wait for the frontmost worker to finish. It's possible that a
            // finished worker may be queued behind a non-finished worker and,
            // in principle, we could reuse it. However, it's important that
            // finish_ingest_page be called for each page *in order*, which
            // makes implementation of out-of-order worker reuse non-trivial.
            let outcome = ingests_in_progress
                .pop_front()
                .expect("There must be ingests in the queue if the worker limit has been reached")
                .into_future()
                .await?;
            page_progress.update(outcome.stats, outcome.next_page);
            outcome.worker
        };

        // Try to the last completed page in the db
        if let Some(conn) = pool.get().await {
            let page = page_progress.next_ingest_start_page.clone();
            if let Err(err) = conn
                .run(move |conn| db::update_next_ingest_start_page(conn, ingest_id, page))
                .await
            {
                warn!("Failed to update next ingest start page. Error: {:?}", err);
            }
        } else {
            warn!("Failed to update next ingest start page. Couldn't get a database connection.");
        }

        // Start the worker on the page of games
        ingests_in_progress.push_back(worker.ingest_page_of_games(index, fetch_duration, page));
    }

    let actual_parallelism = ingests_in_progress.len();
    while let Some(ingest_fut) = ingests_in_progress.pop_front() {
        let outcome = ingest_fut.into_future().await?;
        page_progress.update(outcome.stats, outcome.next_page);
        // Worker gets dropped here
    }

    info!(
        "Ingest finished using {actual_parallelism}/{} workers",
        config.ingest_parallelism
    );
    info!("{} games ingested", page_progress.stats.num_games_imported);
    info!(
        "{} ongoing games skipped",
        page_progress.stats.num_ongoing_games_skipped
    );
    info!(
        "{} terminal incomplete (bugged) games skipped",
        page_progress.stats.num_terminal_incomplete_games_skipped
    );
    info!(
        "{} games already ingested",
        page_progress.stats.num_already_ingested_games_skipped
    );
    info!(
        "{} pages of games finalized this time. \
        {} pages will be re-ingested next time to catch incomplete games.",
        page_progress.pages_finished_before_freeze, page_progress.pages_finished_after_freeze,
    );

    Ok((ingest_id, page_progress.next_ingest_start_page))
}
