mod http;
mod sim;

use crate::db::{RowToEventError, Taxa};
use crate::ingest::sim::{Game, SimFatalError};
use crate::{Db, db};
use chrono::{DateTime, TimeZone, Utc};
use chrono_humanize::HumanTime;
use log::{error, info, warn};
use mmolb_parsing::ParsedEventMessage;
use reqwest_middleware::ClientWithMiddleware;
use rocket::fairing::{Fairing, Info, Kind};
use rocket::tokio::sync::{Notify, RwLock};
use rocket::tokio::task::JoinHandle;
use rocket::{Orbit, Rocket, Shutdown, tokio};
use rocket_db_pools::Database;
use rocket_db_pools::diesel::AsyncPgConnection;
use serde::Deserialize;
pub use sim::{EventDetail, EventDetailFielder, EventDetailRunner, IngestLog, MinimalGameState};
use std::mem;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;

fn default_ingest_period() -> u64 {
    30 * 60  // 30 minutes, expressed in seconds
}

fn default_retries() -> u64 {
    3
}
fn default_fetch_rate_limit() -> u64 {
    10
}

#[derive(Deserialize)]
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
    #[serde(default)]
    cache_game_list_from_api: bool,
    #[serde(default = "default_retries")]
    fetch_game_list_retries: u64,
    #[serde(default)]
    cache_games_from_api: bool,
    #[serde(default = "default_retries")]
    fetch_games_retries: u64,
    #[serde(default = "default_fetch_rate_limit")]
    fetch_games_rate_limit_ms: u64,
}

#[derive(Debug, Error)]
pub enum IngestSetupError {
    #[error("Failed to get a database connection for ingest setup due to {0}")]
    DbPoolSetupError(#[from] rocket_db_pools::diesel::pooled_connection::deadpool::PoolError),

    #[error("Database error during ingest setup: {0}")]
    DbSetupError(#[from] diesel::result::Error),

    #[error("Ingest task transitioned away from NotStarted before liftoff")]
    LeftNotStartedTooEarly,
}

#[derive(Debug, Error)]
pub enum FetchGamesListError {
    #[error(transparent)]
    Network(#[from] reqwest_middleware::Error),

    #[error(transparent)]
    Deserialize(#[from] reqwest::Error),
}

#[derive(Debug, Error)]
pub enum IngestFatalError {
    #[error("The ingest task was interrupted while manipulating the task state")]
    InterruptedManipulatingTaskState,

    #[error("Ingest task transitioned to NotStarted state from some other state")]
    ReenteredNotStarted,

    #[error(transparent)]
    PoolError(#[from] rocket_db_pools::diesel::pooled_connection::deadpool::PoolError),

    #[error(transparent)]
    DbError(#[from] diesel::result::Error),

    #[error("Failed to fetch games list after {retries} retries: {err}")]
    FailedToFetchGamesList {
        retries: u64,
        #[source]
        err: FetchGamesListError,
    },

    #[error(transparent)]
    FetchGameError(#[from] FetchGameError),
}

#[derive(Debug, Error)]
pub enum IngestDbError {
    #[error(transparent)]
    PoolError(#[from] rocket_db_pools::diesel::pooled_connection::deadpool::PoolError),

    #[error(transparent)]
    DbError(#[from] diesel::result::Error),
}

#[derive(Debug, Error)]
pub enum GameIngestFatalError {
    #[error(transparent)]
    PoolError(#[from] rocket_db_pools::diesel::pooled_connection::deadpool::PoolError),

    #[error(transparent)]
    DbError(#[from] diesel::result::Error),

    #[error(transparent)]
    SimFatalError(#[from] SimFatalError),
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
        let Some(pool) = Db::fetch(&rocket).cloned() else {
            error!("Cannot launch ingest task: Rocket is not managing a database pool!");
            return;
        };

        let Some(task) = rocket.state::<IngestTask>() else {
            error!("Cannot launch ingest task: Rocket is not managing an IngestTask!");
            return;
        };

        // extract the entire config any `Deserialize` value
        let config = match rocket.figment().extract() {
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

        match launch_ingest_task(pool, config, task.clone(), shutdown).await {
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

#[derive(Deserialize, Eq, PartialEq, Debug)]
pub enum GameState {
    Complete,
    Pitch,
    Field,
    Change,
    NowBatting,
    Change2, // what
    MoundVisit,
    GameOver,
    Recordkeeping,
    LiveNow,
    PitchingMatchup,
    AwayLineup,
    HomeLineup,
    PlayBall,
    InningStart,
    InningEnd,
}

// This is incomplete, there is more data available in this response if necessary
#[derive(Debug, Deserialize)]
struct CashewsGameResponse {
    pub game_id: String,
    pub season: i64,
    pub day: i64,
    pub state: GameState,
}

#[derive(Deserialize)]

struct CashewsGamesResponse {
    items: Vec<CashewsGameResponse>,
    next_page: Option<String>,
}

// This function sets up the ingest task, then returns a JoinHandle for
// the ingest task. Errors in setup are propagated to the caller.
// Errors in the task itself are handled within the task.
async fn launch_ingest_task(
    pool: Db,
    config: IngestConfig,
    task: IngestTask,
    shutdown: Shutdown,
) -> Result<JoinHandle<()>, IngestSetupError> {
    let client = http::get_caching_http_client();

    let (taxa, previous_ingest_start_time) = {
        let mut conn = pool
            .get()
            .await
            .map_err(|err| IngestSetupError::DbPoolSetupError(err))?;

        let taxa = Taxa::new(&mut conn).await?;

        let previous_ingest_start_time = if config.start_ingest_every_launch {
            None
        } else {
            db::latest_ingest_start_time(&mut conn)
                .await?
                .map(|t| Utc.from_utc_datetime(&t))
        };

        (taxa, previous_ingest_start_time)
    };

    Ok(tokio::spawn(ingest_task_runner(
        pool,
        taxa,
        client,
        config,
        task,
        previous_ingest_start_time,
        shutdown,
    )))
}

async fn ingest_task_runner(
    pool: Db,
    taxa: Taxa,
    client: ClientWithMiddleware,
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

        // Get a db connection and do the ingest
        match pool.get().await {
            Ok(mut conn) => {
                let ingest_result = do_ingest(
                    &mut conn,
                    &taxa,
                    &client,
                    &config,
                    &mut shutdown,
                    ingest_start,
                )
                .await;
                if let Err((err, ingest_id)) = ingest_result {
                    if let Some(ingest_id) = ingest_id {
                        warn!(
                            "Fatal error in game ingest: {}. This ingest is aborted.",
                            err
                        );
                        match db::mark_ingest_aborted(&mut conn, ingest_id, Utc::now()).await {
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
            Err(err) => {
                warn!(
                    "Ingest task couldn't get a database connection. Skipping ingest. \
                    Error message: {err}"
                );
                continue;
            }
        };

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

#[derive(Debug, Error)]
pub enum FetchGameFromCacheError {
    #[error("Only-if-cached request failed with message {0}")]
    RequestError(reqwest_middleware::Error),

    #[error("Only-if-cached request returned non-success status code {0}")]
    NonSuccessStatusCode(reqwest::Error),

    #[error("Only-if-cached request failed to json decode with error {0}")]
    FailedJsonDecode(reqwest::Error),
}

async fn fetch_game_from_cache(
    client: &ClientWithMiddleware,
    url: &str,
) -> Result<mmolb_parsing::Game, FetchGameFromCacheError> {
    client
        .get(url)
        .with_extension(http_cache_reqwest::CacheMode::OnlyIfCached)
        .send()
        .await
        .map_err(FetchGameFromCacheError::RequestError)?
        .error_for_status()
        .map_err(FetchGameFromCacheError::NonSuccessStatusCode)?
        .json()
        .await
        .map_err(FetchGameFromCacheError::FailedJsonDecode)
}

#[derive(Debug, Error)]
enum FetchGamePartialError {
    #[error("Uncached request failed with message {0}")]
    RequestError(reqwest_middleware::Error),

    #[error("Uncached request returned non-success status code {0}")]
    NonSuccessStatusCode(reqwest::Error),

    #[error("Uncached request failed to json decode with error {0}")]
    FailedJsonDecode(reqwest::Error),
}

#[derive(Debug, Error)]
pub enum FetchGameError {
    #[error("Uncached request failed with message {0} after {1}")]
    RequestError(reqwest_middleware::Error, FetchGameFromCacheError),

    #[error("Uncached request returned non-success status code {0} after {1}")]
    NonSuccessStatusCode(reqwest::Error, FetchGameFromCacheError),

    #[error("Uncached request failed to json decode with error {0} after {1}")]
    FailedJsonDecode(reqwest::Error, FetchGameFromCacheError),
}

impl FetchGameError {
    fn from_partial(partial: FetchGamePartialError, cache_err: FetchGameFromCacheError) -> Self {
        match partial {
            FetchGamePartialError::RequestError(err) => {
                FetchGameError::RequestError(err, cache_err)
            }
            FetchGamePartialError::NonSuccessStatusCode(err) => {
                FetchGameError::NonSuccessStatusCode(err, cache_err)
            }
            FetchGamePartialError::FailedJsonDecode(err) => {
                FetchGameError::FailedJsonDecode(err, cache_err)
            }
        }
    }
}

async fn fetch_game_from_server(
    client: &ClientWithMiddleware,
    url: &str,
    cache_mode: http_cache_reqwest::CacheMode,
) -> Result<mmolb_parsing::Game, FetchGamePartialError> {
    client
        .get(url)
        .with_extension(cache_mode)
        .send()
        .await
        .map_err(FetchGamePartialError::RequestError)?
        .error_for_status()
        .map_err(FetchGamePartialError::NonSuccessStatusCode)?
        .json()
        .await
        .map_err(FetchGamePartialError::FailedJsonDecode)
}

async fn fetch_game(
    game_id: &str,
    client: &ClientWithMiddleware,
    config: &IngestConfig,
    rate_limiter: &tokio_utils::RateLimiter,
) -> Result<mmolb_parsing::Game, FetchGameError> {
    let url = format!("https://mmolb.com/api/game/{}", game_id);

    // First, attempt to grab data from the cache, and if we succeed,
    // return early. Otherwise record the fetch-from-cache error and
    // continue.
    let cache_err = match fetch_game_from_cache(client, &url).await {
        Ok(game) => {
            info!("Returning game from cache");
            return Ok(game);
        }
        Err(err) => err,
    };

    rate_limiter
        .throttle(|| async {
            info!("Fetching game from the server because of cache error: {cache_err}");

            fetch_with_retries(
                config.cache_games_from_api,
                config.fetch_games_retries,
                async |cache_mode| fetch_game_from_server(client, &url, cache_mode).await,
            )
            .await
            .map_err(|e| FetchGameError::from_partial(e, cache_err))
        })
        .await
}

async fn do_ingest(
    conn: &mut AsyncPgConnection,
    taxa: &Taxa,
    client: &ClientWithMiddleware,
    config: &IngestConfig,
    shutdown: &mut Shutdown,
    ingest_start: DateTime<Utc>,
) -> Result<(), (IngestFatalError, Option<i64>)> {
    info!("Ingest at {ingest_start} started");
    
    // Start at the season *after* the latest completed season, or season 0 if there isn't one 
    let start_at_season = db::latest_completed_season(conn).await
        .map_err(|e| (e.into(), None))?
        .map(|i| i + 1)
        .unwrap_or(0)
        .into();

    let ingest_id = db::start_ingest(conn, ingest_start)
        .await
        .map_err(|e| (e.into(), None))?;

    do_ingest_internal(
        conn,
        taxa,
        client,
        config,
        shutdown,
        ingest_id,
        ingest_start,
        start_at_season,
    )
    .await
    .map_err(|e| (e.into(), Some(ingest_id)))
}
async fn do_ingest_internal(
    conn: &mut AsyncPgConnection,
    taxa: &Taxa,
    client: &ClientWithMiddleware,
    config: &IngestConfig,
    shutdown: &mut Shutdown,
    ingest_id: i64,
    ingest_start: DateTime<Utc>,
    start_at_season: i64,
) -> Result<(), IngestFatalError> {
    info!("Recorded ingest start in database. Requesting games list...");

    let fetched_games = match fetch_games_list(client, config, start_at_season).await {
        Ok(value) => value,
        Err(err) => {
            return Err(IngestFatalError::FailedToFetchGamesList {
                retries: config.fetch_game_list_retries,
                err,
            });
        }
    };
    
    info!("Fetched games' latest completed season is {:?}", fetched_games.latest_completed_season);
    
    // Stuff that happens in the loop might invalidate this
    let mut latest_completed_season = fetched_games.latest_completed_season;

    info!(
        "Got games list. Starting ingest of {} total games.",
        fetched_games.games.len()
    );

    let game_api_hit_period = Duration::from_millis(config.fetch_games_rate_limit_ms);
    let rate_limiter = tokio_utils::RateLimiter::new(game_api_hit_period);

    let mut num_incomplete_games_skipped = 0;
    let mut num_already_ingested_games_skipped = 0;
    let mut num_games_imported = 0;

    for game_info in fetched_games.games {
        let total_start = Utc::now();
        if game_info.state != GameState::Complete {
            // If this incomplete game is in the supposed latest 
            // completed season, then the supposed latest completed 
            // season is not actually completed. Record a None, and
            // the next ingest will find the same value as this one.
            if Some(game_info.season) == latest_completed_season {
                info!(
                    "Clearing latest completed season because game {} in season {} isn't complete",
                    game_info.game_id,
                    game_info.season,
                );
                latest_completed_season = None;
            }
            
            num_incomplete_games_skipped += 1;
            continue;
        }

        let check_already_ingested_start = Utc::now();
        let already_ingested = if !config.reimport_all_games {
            db::has_game(conn, &game_info.game_id).await?
        } else {
            info!("Deleting {} if it exists", game_info.game_id);
            let num_deleted = db::delete_game(conn, &game_info.game_id).await?;

            if num_deleted > 0 {
                info!(
                    "In debug mode, deleted game {} and all its events",
                    game_info.game_id
                );
            }
            false
        };

        if already_ingested {
            num_already_ingested_games_skipped += 1;
            continue;
        }
        let check_already_ingested_duration =
            (Utc::now() - check_already_ingested_start).as_seconds_f64();

        let network_start = Utc::now();
        let game_data = tokio::select! {
            game_data = fetch_game(&game_info.game_id, client, config, &rate_limiter) => {
                game_data?
            }
            _ = &mut *shutdown => {
                // If an early shutdown was requested, we might not 
                // have processed all the games, which invalidates
                // latest_completed_season
                info!("Clearing latest completed season because early shutdown is requested");
                latest_completed_season = None;
                break;
            }
        };
        let network_duration = (Utc::now() - network_start).as_seconds_f64();

        info!(
            "Fetched {} {} @ {} {} s{}d{}: {}",
            game_data.away_team_emoji,
            game_data.away_team_name,
            game_data.home_team_emoji,
            game_data.home_team_name,
            game_data.season,
            game_data.day,
            format!("https://mmolb.com/watch/{}", game_info.game_id),
        );

        let result = ingest_game(
            conn,
            &taxa,
            ingest_id,
            &game_info,
            game_data,
            PreIngestTimings {
                total_start,
                check_already_ingested_duration,
                network_duration,
            },
        )
        .await;

        if result.is_err() {
            // If we return early, we might not have processed all the 
            // games, which invalidates latest_completed_season
            info!("Clearing latest completed season because there was an error");
            latest_completed_season = None;
            break;
        }

        num_games_imported += 1;
    }

    info!("{num_games_imported} games ingested");
    info!("{num_incomplete_games_skipped} incomplete games skipped");
    info!("{num_already_ingested_games_skipped} games already ingested");

    let ingest_end = Utc::now();
    db::mark_ingest_finished(conn, ingest_id, ingest_end, latest_completed_season).await?;
    info!(
        "Marked ingest {ingest_id} finished {:#}. Latest completed season is {:?}",
        HumanTime::from(ingest_end - ingest_start), latest_completed_season,
    );

    Ok(())
}

async fn fetch_with_retries<R, E, Fut, InnerFn>(
    cache: bool,
    max_retries: u64,
    mut inner_fn: InnerFn,
) -> Result<R, E>
where
    E: std::fmt::Display,
    Fut: Future<Output = Result<R, E>>,
    InnerFn: FnMut(http_cache_reqwest::CacheMode) -> Fut,
{
    let cache_mode = if cache {
        // ForceCache means to ignore staleness, but still go to the
        // network if it's not found in the cache
        http_cache_reqwest::CacheMode::ForceCache
    } else {
        // NoStore means don't use the cache at all
        http_cache_reqwest::CacheMode::NoStore
    };

    let mut tries = 0;
    Ok(loop {
        match inner_fn(cache_mode).await {
            Ok(r) => break r,
            Err(err) if tries < max_retries => {
                tries += 1;
                let wait_time = Duration::from_millis(100 * tries * tries);
                warn!(
                    "Retrying request after {}s due to error: {err}",
                    wait_time.as_secs_f64()
                );
                tokio::time::sleep(wait_time).await;
            }
            Err(err) => {
                return Err(err);
            }
        }
    })
}

async fn fetch_games_list(
    client: &ClientWithMiddleware,
    config: &IngestConfig,
    start_at_season: i64,
) -> Result<FetchedGames, FetchGamesListError> {
    let cache_mode = if config.cache_game_list_from_api {
        // ForceCache means to ignore staleness, but still go to the
        // network if it's not found in the cache
        http_cache_reqwest::CacheMode::ForceCache
    } else {
        // NoStore means don't use the cache at all
        http_cache_reqwest::CacheMode::NoStore
    };

    let mut tries = 0;
    Ok(loop {
        match fetch_games_list_base(client, cache_mode, start_at_season).await {
            Ok(r) => break r,
            Err(err) if tries < config.fetch_game_list_retries => {
                tries += 1;
                let wait_time = Duration::from_millis(100 * tries * tries);
                warn!(
                    "Error fetching games; retrying in {}s. Error: {err}",
                    wait_time.as_secs_f64()
                );
                warn!("Error debug: {err:?}");
                tokio::time::sleep(wait_time).await;
            }
            Err(err) => {
                return Err(err);
            }
        }
    })
}

struct FetchedGames {
    games: Vec<CashewsGameResponse>,
    latest_completed_season: Option<i64>,
}

async fn fetch_games_list_base(
    client: &ClientWithMiddleware,
    cache_mode: http_cache_reqwest::CacheMode,
    start_at_season: i64,
) -> Result<FetchedGames, FetchGamesListError> {
    // List of list of games, to be flattened later
    let mut games = Vec::new();
    let mut latest_season_with_any_games = None;

    // The cashews API does not conveniently support resuming in the 
    // middle of a season.
    // TODO: Ask cashews developers for an API that would allow resuming from where I left off
    for season in start_at_season.. {
        let mut has_any_games = false;
        let mut next_response: CashewsGamesResponse = client
            .get("https://freecashe.ws/api/games")
            .with_extension(cache_mode)
            .query(&[("season", season.to_string())])
            .send()
            .await?
            .json()
            .await?;

        loop {
            if next_response.items.is_empty() {
                break;
            }

            has_any_games = true;
            games.push(next_response.items);

            if let Some(next_page) = next_response.next_page {
                next_response = client
                    .get("https://freecashe.ws/api/games")
                    .with_extension(cache_mode)
                    .query(&[("season", season.to_string())])
                    .query(&[("page", next_page)])
                    .send()
                    .await?
                    .json()
                    .await?
            } else {
                break;
            }
        }

        if !has_any_games {
            break;
        } else {
            latest_season_with_any_games = Some(season);
        }
    }
    
    let latest_completed_season = latest_season_with_any_games
        .and_then(|season| if season == 0 { None } else { Some(season - 1) });

    Ok(FetchedGames {
        games: games.into_iter().flatten().collect(),
        latest_completed_season,
    })
}

// A utility to more conveniently build a Vec<IngestLog>
pub struct IngestLogs {
    logs: Vec<(usize, IngestLog)>,
}

impl IngestLogs {
    pub fn new() -> Self {
        Self { logs: Vec::new() }
    }

    #[allow(dead_code)]
    pub fn critical(&mut self, game_event_index: usize, s: impl Into<String>) {
        self.logs.push((
            game_event_index,
            IngestLog {
                log_level: 0,
                log_text: s.into(),
            },
        ));
    }

    pub fn error(&mut self, game_event_index: usize, s: impl Into<String>) {
        self.logs.push((
            game_event_index,
            IngestLog {
                log_level: 1,
                log_text: s.into(),
            },
        ));
    }

    #[allow(dead_code)]
    pub fn warn(&mut self, game_event_index: usize, s: impl Into<String>) {
        self.logs.push((
            game_event_index,
            IngestLog {
                log_level: 2,
                log_text: s.into(),
            },
        ));
    }

    #[allow(dead_code)]
    pub fn info(&mut self, game_event_index: usize, s: impl Into<String>) {
        self.logs.push((
            game_event_index,
            IngestLog {
                log_level: 3,
                log_text: s.into(),
            },
        ));
    }

    #[allow(dead_code)]
    pub fn debug(&mut self, game_event_index: usize, s: impl Into<String>) {
        self.logs.push((
            game_event_index,
            IngestLog {
                log_level: 4,
                log_text: s.into(),
            },
        ));
    }

    #[allow(dead_code)]
    pub fn trace(&mut self, game_event_index: usize, s: impl Into<String>) {
        self.logs.push((
            game_event_index,
            IngestLog {
                log_level: 5,
                log_text: s.into(),
            },
        ));
    }

    pub fn into_vec(self) -> Vec<(usize, IngestLog)> {
        self.logs
    }
}

struct PreIngestTimings {
    pub total_start: DateTime<Utc>,
    pub check_already_ingested_duration: f64,
    pub network_duration: f64,
}

async fn ingest_game(
    conn: &mut AsyncPgConnection,
    taxa: &Taxa,
    ingest_id: i64,
    game_info: &CashewsGameResponse,
    game_data: mmolb_parsing::Game,
    pre_ingest_timings: PreIngestTimings,
) -> Result<(), GameIngestFatalError> {
    let parse_start = Utc::now();
    let parsed_copy = mmolb_parsing::process_game(&game_data);
    let parse_duration = (Utc::now() - parse_start).as_seconds_f64();

    // I'm adding enumeration to parsed, then stripping it out for
    // the iterator fed to Game::new, on purpose. I need the
    // counting to count every event, but I don't need the count
    // inside Game::new.
    let mut parsed = parsed_copy.iter().zip(&game_data.event_log).enumerate();

    let sim_start = Utc::now();
    let (mut game, game_creation_ingest_logs) = {
        let mut parsed_for_game = (&mut parsed).map(|(_, (parsed, _))| parsed);

        Game::new(game_info, &game_data, &mut parsed_for_game)?
    };

    let (detail_events, ingest_logs): (Vec<_>, Vec<_>) = parsed
        .map(|(index, (parsed, raw))| {
            // Sim has a different IngestLogs... this made sense at the time
            let mut ingest_logs = sim::IngestLogs::new();

            let unparsed = parsed.clone().unparse();
            if unparsed != raw.message {
                ingest_logs.error(format!(
                    "Round-trip of raw event through ParsedEvent produced a mismatch:\n\
                     Original: <pre>{:?}</pre>\n\
                     Through EventDetail: <pre>{:?}</pre>",
                    raw.message, unparsed,
                ));
            }

            let event = match game.next(index, &parsed, &raw, &mut ingest_logs) {
                Ok(result) => result,
                Err(e) => {
                    ingest_logs.critical(e.to_string());
                    None
                }
            };

            (event, ingest_logs.into_vec())
        })
        .unzip();
    let sim_duration = (Utc::now() - sim_start).as_seconds_f64();

    // Take the None values out of detail_events
    let detail_events = detail_events
        .into_iter()
        .flat_map(|event| event)
        .collect::<Vec<_>>();

    // Scope to drop conn as soon as I'm done with it
    let db_start = Utc::now();
    let (
        game_id,
        (inserted_events, events_for_game_timings),
        db_insert_duration,
        db_fetch_for_check_duration,
    ) = {
        let db_insert_start = Utc::now();
        let game_id = db::insert_game(
            conn,
            &taxa,
            ingest_id,
            &game_info.game_id,
            &game_data,
            game_creation_ingest_logs.into_iter().chain(ingest_logs),
            &detail_events,
            MinimalGameState::from(&game),
        )
        .await?;
        let db_insert_duration = (Utc::now() - db_insert_start).as_seconds_f64();

        // We can rebuild them
        let db_fetch_for_check_start = Utc::now();
        let inserted_events = db::events_for_game(conn, &taxa, &game_info.game_id).await?;
        let db_fetch_for_check_duration = (Utc::now() - db_fetch_for_check_start).as_seconds_f64();

        (
            game_id,
            inserted_events,
            db_insert_duration,
            db_fetch_for_check_duration,
        )
    };
    let db_duration = (Utc::now() - db_start).as_seconds_f64();

    let check_round_trip_start = Utc::now();
    let mut extra_ingest_logs = IngestLogs::new();
    if inserted_events.len() != detail_events.len() {
        error!(
            "Number of events read from the db ({}) does not match number of events written to \
            the db ({})",
            inserted_events.len(),
            detail_events.len(),
        );
    }
    for (reconstructed_detail, original_detail) in inserted_events.iter().zip(detail_events) {
        let index = original_detail.game_event_index;
        let fair_ball_index = original_detail.fair_ball_event_index;

        if let Some(index) = fair_ball_index {
            check_round_trip(
                index,
                &mut extra_ingest_logs,
                true,
                &parsed_copy[index],
                &original_detail,
                reconstructed_detail,
            );
        }

        check_round_trip(
            index,
            &mut extra_ingest_logs,
            false,
            &parsed_copy[index],
            &original_detail,
            reconstructed_detail,
        );
    }
    let check_round_trip_duration = (Utc::now() - check_round_trip_start).as_seconds_f64();

    let insert_extra_logs_start = Utc::now();
    let extra_ingest_logs = extra_ingest_logs.into_vec();
    if !extra_ingest_logs.is_empty() {
        db::insert_additional_ingest_logs(conn, game_id, extra_ingest_logs).await?;
    }
    let insert_extra_logs_duration = (Utc::now() - insert_extra_logs_start).as_seconds_f64();

    {
        db::insert_timings(
            conn,
            game_id,
            db::Timings {
                check_already_ingested_duration: pre_ingest_timings.check_already_ingested_duration,
                network_duration: pre_ingest_timings.network_duration,
                parse_duration,
                sim_duration,
                db_insert_duration,
                db_fetch_for_check_duration,
                events_for_game_timings,
                db_duration,
                check_round_trip_duration,
                insert_extra_logs_duration,
                total_duration: (Utc::now() - pre_ingest_timings.total_start).as_seconds_f64(),
            },
        )
        .await?;
    }

    info!(
        "Finished importing {} {} @ {} {} s{}d{}",
        game_data.away_team_emoji,
        game_data.away_team_name,
        game_data.home_team_emoji,
        game_data.home_team_name,
        game_data.season,
        game_data.day,
    );

    Ok(())
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
