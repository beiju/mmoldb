mod config;
mod ingest;
pub mod ingest_feed_shared;
mod ingest_games;
mod ingest_player_feed;
mod ingest_players;
mod ingest_team_feed;
mod ingest_teams;
mod logging_setup;
mod partitioner;
mod signal;

use tokio::task::JoinHandle;
use tokio::signal::unix as tokio_signal;
use opentelemetry::global;
use crate::ingest_teams::TeamIngest;
use chrono::Utc;
use chrono_humanize::{Accuracy, HumanTime, Tense};
use config::IngestConfig;
use futures::{pin_mut, FutureExt, StreamExt};
use tracing::{debug, error, info};
use miette::{Context, GraphicalReportHandler, IntoDiagnostic};
use mmoldb_db::{ConnectionPool, db, QueryResult, PgConnection};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use crate::ingest_player_feed::PlayerFeedIngest;
use crate::ingest_players::PlayerIngest;
use crate::ingest_team_feed::TeamFeedIngest;
use cap::Cap;
pub use ingest::*;
use std::alloc;
use futures::stream::FuturesUnordered;

static MEMORY_TRACKING_PERIOD_MS: u64 = 10_000;

#[global_allocator]
static ALLOCATOR: Cap<alloc::System> = Cap::new(alloc::System, usize::MAX);

async fn memory_tracking_task(shutdown_requested: CancellationToken) {
    let gauge = global::meter("memory-meter")
        .u64_gauge("memory-gauge")
        .with_unit("bytes")
        .build();
    let mut interval = tokio::time::interval(Duration::from_millis(MEMORY_TRACKING_PERIOD_MS));

    loop {
        let allocated = ALLOCATOR.allocated();
        gauge.record(allocated as u64, &[]);

        tokio::select! {
            _ = interval.tick() => {}
            _ = shutdown_requested.cancelled() => { break; }
        }
    }
}

#[tokio::main]
async fn main() -> miette::Result<()> {
    // Logging first so it can capture as much as possible
    // This guard must be alive in order for otel logs to be processed
    let _otel_guard = logging_setup::init_tracing_subscriber();

    // Then all other setup tasks in approximate order of how quickly
    // they'll fail if they're going to fail
    let (sigterm, sigint) = get_signal_listeners()?;
    let config = get_config()?;
    let pool = mmoldb_db::get_pool(config.db_pool_size).into_diagnostic()?;
    {
        let mut conn = pool.get().into_diagnostic()?;
        set_statement_timeout(&mut conn, config.set_postgres_statement_timeout).into_diagnostic()?;
    }
    mmoldb_db::run_migrations().into_diagnostic()?;

    // Task coordination variables
    let shutdown_requested = tokio_util::sync::CancellationToken::new();
    // Writing out the full type for better error messages
    // TODO Get rid of errors. Handle all exceptional conditions without exiting.
    let tasks = FuturesUnordered::<JoinHandle<Result<(), IngestFatalError>>>::new();

    // Launch background, non-ingest tasks
    info!("Launching background memory tracking task");
    tasks.push(tokio::task::spawn(memory_tracking_task(shutdown_requested.clone()).map(Ok)));

    // Launch ingest tasks
    let ingest_kinds = ingest::ingest_kinds(&shutdown_requested, &pool, config);
    for ingest_kind in &ingest_kinds {
        if ingest_kind.fetch_is_enabled() {
            info!("Launching fetch task for {}", ingest_kind.kind());
            let ingest_kind = ingest_kind.clone();
            let task = tokio::task::spawn(async move { ingest_kind.fetch_task().await });
            tasks.push(task);
        } else {
            info!("Fetch task for {} is disabled", ingest_kind.kind());
        }
    }
    for ingest_kind in &ingest_kinds {
        if ingest_kind.processing_is_enabled() {
            info!("Launching processing task for {}", ingest_kind.kind());
            let ingest_kind = ingest_kind.clone();
            let task = tokio::task::spawn(async move { ingest_kind.processing_task().await });
            tasks.push(task);
        } else {
            info!("Processing task for {} is disabled", ingest_kind.kind());
        }
    }

    info!("Running {} task(s)", tasks.len());
    wait_until_shutdown(tasks, sigterm, sigint, shutdown_requested).await
}

fn get_signal_listeners() -> miette::Result<(tokio_signal::Signal, tokio_signal::Signal)> {
    let sigterm = tokio_signal::signal(tokio_signal::SignalKind::terminate())
        .into_diagnostic()
        .wrap_err("trying to set up SIGTERM listener")?;
    let sigint = tokio_signal::signal(tokio_signal::SignalKind::interrupt())
        .into_diagnostic()
        .wrap_err("trying to set up SIGINT listener")?;
    Ok((sigterm, sigint))
}

fn get_config() -> miette::Result<&'static IngestConfig> {
    let config = IngestConfig::config().into_diagnostic()?;
    let config = Box::new(config);
    let config = Box::<IngestConfig>::leak(config);
    Ok(config)
}

fn set_statement_timeout(conn: &mut PgConnection, statement_timeout: Option<i64>) -> QueryResult<()> {
    if let Some(statement_timeout) = statement_timeout {
        if statement_timeout < 0 {
            panic!("Negative STATEMENT_TIMEOUT_SEC not allowed ({statement_timeout})");
        } else if statement_timeout > 0 {
            info!(
                "Setting our account's statement timeout to {statement_timeout} ({})",
                HumanTime::from(chrono::Duration::seconds(statement_timeout))
                    .to_text_en(Accuracy::Precise, Tense::Present),
            );
        } else {
            // Postgres interprets 0 as no timeout
            info!("Setting our account's statement timeout to no timeout");
        }
        db::set_current_user_statement_timeout(conn, statement_timeout)?;;
    }

    Ok(())
}

async fn wait_until_shutdown(
    mut tasks: FuturesUnordered<JoinHandle<Result<(), IngestFatalError>>>,
    mut sigterm: tokio_signal::Signal,
    mut sigint: tokio_signal::Signal,
    shutdown_requested: CancellationToken,
) -> miette::Result<()> {

    // Wait for tasks
    tokio::select! {
        // Wait for sigint and sigterm to respond to them appropriately
        _ = sigterm.recv() => {
            info!("Got SIGTERM. Setting shutdown_requested and waiting for tasks to exit...");
        },
        _ = sigint.recv() => {
            info!("Got SIGINT. This may be changed to wait for ingest consistency in future. But for now, setting shutdown_requested and waiting for tasks to exit...");
        },
        // Wait for tasks to catch any that error, which is the only way any of these tasks should exit without being requested to.
        res = tasks.select_next_some() => {
            match &res {
                Ok(_) => panic!("Tasks shouldn't exit Ok(_) before a cancellation token is set"),
                Err(e) => error!("Setting shutdown_requested because a task failed: {}", e),
            }
        }
    };

    shutdown_requested.cancel();
    info!("Trying to shut down. Waiting for all tasks...");
    for task in tasks {
        match task.await {
            Ok(Ok(())) => {
                info!("Successfully shut down the task")
            },
            Ok(Err(internal_error)) => {
                error!("Ingest fatal error: {}", internal_error);
            },
            Err(join_error) => {
                error!("Error joining task: {}", join_error);
            }
        }
    }

    Ok(())
}

async fn run_one_ingest(pool: ConnectionPool, config: &'static IngestConfig) -> () {
    let mut errs = IngestErrorContainer::new();
    let ingest_start_time = Utc::now();

    let mut conn = match pool.get() {
        Ok(conn) => conn,
        Err(err) => {
            error!(
                "Error getting database connection for ingest. This ingest will be skipped. \
                Error: {:?}",
                err
            );
            return;
        }
    };

    let ingest_id = match db::start_ingest(&mut conn, ingest_start_time) {
        Ok(ingest_id) => ingest_id,
        Err(err) => {
            error!(
                "Error saving ingest to database. This ingest will be skipped. Error: {:?}",
                err
            );
            return;
        }
    };

    if let Some(statement_timeout) = config.set_postgres_statement_timeout {
        if statement_timeout < 0 {
            panic!("Negative STATEMENT_TIMEOUT_SEC not allowed ({statement_timeout})");
        } else if statement_timeout > 0 {
            info!(
                "Setting our account's statement timeout to {statement_timeout} ({})",
                HumanTime::from(chrono::Duration::seconds(statement_timeout))
                    .to_text_en(Accuracy::Precise, Tense::Present),
            );
        } else {
            // Postgres interprets 0 as no timeout
            info!("Setting our account's statement timeout to no timeout");
        }
        let result =
            db::set_current_user_statement_timeout(&mut conn, statement_timeout).map_err(|error| {
                IngestStageError {
                    stage_name: "Setting statement timeout".to_string(),
                    error: IngestFatalError::DbError(error),
                }
            });
        errs.push(result);
    }

    let abort = CancellationToken::new();
    let ingest_task = ingest_everything(pool.clone(), ingest_id, abort.clone(), config);
    pin_mut!(ingest_task);

    let is_aborted = tokio::select! {
        ingest_result = &mut ingest_task => {
            errs.extend(ingest_result);
            false
        }
        _ = signal::wait_for_signal() => {
            debug!("Signaling abort token");
            abort.cancel();
            true
        }
    };

    // Note: use abort.is_cancelled() instead of is_aborted because is_aborted
    // is also True if the task finished with a Result::Err(). We don't want to
    // await it in this case.
    if abort.is_cancelled() {
        debug!("Waiting for ingest task to shut down after abort signal");
        errs.extend(ingest_task.await);
    }

    let err_message = if errs.errors.is_empty() {
        None
    } else {
        let grh = GraphicalReportHandler::new()
            .with_cause_chain()
            .with_show_related_as_nested(true);
        let mut str = String::new();
        grh.render_report(&mut str, &errs).unwrap();
        Some(str)
    };

    let ingest_end_time = Utc::now();
    let duration = HumanTime::from(ingest_end_time - ingest_start_time);
    if is_aborted {
        match db::mark_ingest_aborted(
            &mut conn,
            ingest_id,
            ingest_end_time,
            err_message.as_deref(),
        ) {
            Ok(()) => {
                info!(
                    "Aborted ingest in {}",
                    duration.to_text_en(Accuracy::Precise, Tense::Present)
                );
            }
            Err(error) => {
                error!(
                    "Error marking ingest as aborted. The ingest is actually aborted, but the \
                    outcome won't be recorded in the database. Error: {error:?}"
                );
                if let Some(msg) = err_message {
                    error!("The error message we failed to save to the database is: {msg}")
                }
            }
        }
    } else {
        match db::mark_ingest_finished(
            &mut conn,
            ingest_id,
            ingest_end_time,
            err_message.as_deref(),
        ) {
            Ok(()) => {
                info!(
                    "Finished ingest in {}",
                    duration.to_text_en(Accuracy::Precise, Tense::Present)
                );
            }
            Err(error) => {
                error!(
                    "Error marking ingest as finished. The ingest is actually finished, but the \
                    outcome won't be recorded in the database. Error: {error:?}"
                );
                if let Some(msg) = err_message {
                    error!("The error message we failed to save to the database is: {msg}")
                }
            }
        }
    }

    // Don't return `errs` -- we already handled it, and returning it will
    // make the process exit
}

fn refresh_matviews(pool: ConnectionPool) -> IngestErrorContainer {
    let mut errs = IngestErrorContainer::new();

    let mut conn = match pool.get() {
        Ok(conn) => conn,
        Err(e) => {
            errs.push_err(IngestStageError {
                stage_name: "Refresh matviews".to_string(),
                error: IngestFatalError::DbPoolError(e),
            });
            return errs;
        }
    };

    for err in db::refresh_matviews(&mut conn) {
        errs.push_err(IngestStageError {
            stage_name: "Refresh matviews".to_string(),
            error: IngestFatalError::DbError(err),
        });
    }

    errs
}

fn block_until_exit(exit: Arc<(Mutex<bool>, Condvar)>, timeout: Duration) -> bool {
    let (exit, exit_cond) = &*exit;
    let (should_exit, _) = exit_cond
        .wait_timeout_while(exit.lock().unwrap(), timeout, |&mut should_exit| {
            !should_exit
        })
        .unwrap();

    *should_exit
}

fn refresh_entity_counting_matviews_repeatedly(
    pool: ConnectionPool,
    exit: Arc<(Mutex<bool>, Condvar)>,
) -> Result<(), IngestFatalError> {
    let mut conn = pool.get().map_err(IngestFatalError::DbPoolError)?;
    loop {
        if block_until_exit(exit.clone(), Duration::from_secs(10)) {
            break;
        }

        for err in db::refresh_entity_counting_matviews(&mut conn) {
            error!("Error in entity counting task: {err:?}");
        }
    }

    Ok(())
}

async fn ingest_everything(
    pool: ConnectionPool,
    ingest_id: i64,
    abort: CancellationToken,
    config: &'static IngestConfig,
) -> IngestErrorContainer {
    let stop_entity_counting = Arc::new((Mutex::new(false), Condvar::new()));
    let entity_counting_task = tokio::task::spawn_blocking({
        let pool = pool.clone();
        let stop_entity_counting = stop_entity_counting.clone();
        || refresh_entity_counting_matviews_repeatedly(pool, stop_entity_counting)
    });



    let mut errs = ingest_while_counting_task_runs(pool.clone(), ingest_id, abort, config).await;

    let (exit, exit_cond) = &*stop_entity_counting;
    {
        let mut should_exit = exit.lock().unwrap();
        *should_exit = true;
    }
    exit_cond.notify_all();
    info!("Waiting for entity counting task to finish");
    match entity_counting_task.await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => errs.errors.push(IngestStageError {
            stage_name: "Counting task".to_string(),
            error,
        }),
        Err(error) => errs.errors.push(IngestStageError {
            stage_name: "Counting task".to_string(),
            error: IngestFatalError::JoinError(error),
        }),
    }

    info!("Refreshing materialized views");
    errs.extend(refresh_matviews(pool));

    if errs.errors.is_empty() {
        info!("Ingest complete");
    } else {
        error!("Ingest complete with errors: {errs:?}");
    }

    errs
}

async fn ingest_while_counting_task_runs(
    pool: ConnectionPool,
    ingest_id: i64,
    abort: CancellationToken,
    config: &'static IngestConfig,
) -> IngestErrorContainer {
    let mut errs = IngestErrorContainer::new();
    let ingestor = Ingestor::new(pool.clone(), ingest_id, abort.clone());

    errs.extend(
        ingestor
            .ingest(
                TeamIngest::new(&config.team_ingest),
                config.use_local_cheap_cashews,
            )
            .await,
    );
    errs.extend(
        ingestor
            .ingest(
                TeamFeedIngest::new(&config.team_feed_ingest),
                config.use_local_cheap_cashews,
            )
            .await,
    );
    errs.extend(
        ingestor
            .ingest(
                PlayerIngest::new(&config.player_ingest),
                config.use_local_cheap_cashews,
            )
            .await,
    );
    errs.extend(
        ingestor
            .ingest(
                PlayerFeedIngest::new(&config.player_feed_ingest),
                config.use_local_cheap_cashews,
            )
            .await,
    );

    if config.fetch_known_missing_games {
        info!("Fetching any missing games in known-game-ids.txt");
        if let Err(error) = ingest_games::fetch_missed_games(pool.clone()).await {
            errs.errors.push(IngestStageError {
                stage_name: "Fetch known missing games".to_string(),
                error,
            });
        }
    }

    // if config.game_ingest.enable {
    //     info!("Beginning game ingest");
    //     errs.extend(
    //         ingest_games::ingest_games(
    //             pool.clone(),
    //             ingest_id,
    //             abort,
    //             config.use_local_cheap_cashews,
    //         )
    //         .await,
    //     );
    // }

    errs
}
