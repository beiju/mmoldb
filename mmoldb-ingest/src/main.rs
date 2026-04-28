mod config;
mod ingest;
pub mod ingest_feed_shared;
mod ingest_games;
mod ingest_player_feed;
mod ingest_players;
mod ingest_team_feed;
mod ingest_teams;
mod partitioner;

use tokio::task::JoinHandle;
use tokio::signal::unix as tokio_signal;
use chrono_humanize::{Accuracy, HumanTime, Tense};
use config::IngestConfig;
use futures::{FutureExt, StreamExt};
use tracing::{error, info, info_span, span, warn, Instrument, Level};
use miette::{Context, IntoDiagnostic};
use mmoldb_db::{ConnectionPool, db, QueryResult, PgConnection};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use cap::Cap;
pub use ingest::*;
use std::alloc;
use futures::stream::FuturesUnordered;

static MEMORY_TRACKING_PERIOD_MS: u64 = 10_000;
static ITEM_COUNTING_PERIOD_MS: u64 = 30_000;

#[global_allocator]
static ALLOCATOR: Cap<alloc::System> = Cap::new(alloc::System, usize::MAX);

async fn memory_tracking_task(shutdown_requested: CancellationToken) {
    let mut interval = tokio::time::interval(Duration::from_millis(MEMORY_TRACKING_PERIOD_MS));

    loop {
        let allocated = ALLOCATOR.allocated();
        info!(
            "Current process memory usage: {allocated} bytes ({})",
            humansize::format_size(allocated, humansize::DECIMAL),
        );

        tokio::select! {
            _ = interval.tick() => {}
            _ = shutdown_requested.cancelled() => { break; }
        }
    }
}

async fn counting_task(shutdown_requested: CancellationToken, pool: ConnectionPool) {
    let mut interval = tokio::time::interval(Duration::from_millis(ITEM_COUNTING_PERIOD_MS));

    loop {
        match pool.get() {
            Ok(mut conn) => {
                info!("Refreshing entity counting matviews");
                for err in db::refresh_entity_counting_matviews(&mut conn) {
                    warn!("Couldn't update entity counting matview: {err}");
                }
            }
            Err(e) => {
                warn!("Couldn't get connection to update entity counting matviews: {e}");
            }
        }

        tokio::select! {
            _ = interval.tick() => {}
            _ = shutdown_requested.cancelled() => { break; }
        }
    }
}

#[tokio::main]
async fn main() -> miette::Result<()> {
    // construct a subscriber that prints formatted traces to stdout
    let subscriber = tracing_subscriber::FmtSubscriber::new();
    // use that subscriber to process traces emitted after this point
    tracing::subscriber::set_global_default(subscriber).into_diagnostic()?;

    let _span = span!(Level::INFO, "root").entered();

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
    tasks.push(tokio::task::spawn(
        memory_tracking_task(shutdown_requested.clone()).map(Ok)
            .instrument(info_span!("memory_tracking"))
    ));
    info!("Launching background item counting task");
    tasks.push(tokio::task::spawn(
        counting_task(shutdown_requested.clone(), pool.clone()).map(Ok)
            .instrument(info_span!("counting"))
    ));

    if config.fetch_known_missing_games {
        warn!("Fetching known missing games is not currently implemented");
    }

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
        db::set_current_user_statement_timeout(conn, statement_timeout)?;
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
            info!(
                "Got SIGINT. This may be changed to wait for ingest consistency in future. \
                But for now, setting shutdown_requested and waiting for tasks to exit...",
            );
        },
        // Wait for tasks to catch any that error, which is the only way any of these tasks should exit without being requested to.
        res = tasks.select_next_some() => {
            match &res {
                Ok(Ok(())) => panic!("Tasks shouldn't exit Ok(_) before a cancellation token is set"),
                Ok(Err(task_err)) => error!("Setting shutdown_requested because a task failed: {}", task_err),
                Err(join_err) => error!("Setting shutdown_requested because a join failed: {}", join_err),
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
