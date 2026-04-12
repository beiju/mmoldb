mod config;
mod ingest;
pub mod ingest_feed_shared;
mod ingest_games;
mod ingest_player_feed;
mod ingest_players;
mod ingest_team_feed;
mod ingest_teams;
mod partitioner;
mod signal;

use crate::ingest_teams::TeamIngest;
use chrono::Utc;
use chrono_humanize::{Accuracy, HumanTime, Tense};
use config::IngestConfig;
use futures::pin_mut;
use log::{debug, error, info};
use miette::{GraphicalReportHandler, IntoDiagnostic};
use mmoldb_db::{ConnectionPool, db};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use crate::ingest_player_feed::PlayerFeedIngest;
use crate::ingest_players::PlayerIngest;
use crate::ingest_team_feed::TeamFeedIngest;
use cap::Cap;
use humansize::{BINARY, format_size};
pub use ingest::*;
use opentelemetry::{
    InstrumentationScope, KeyValue, global,
    trace::{TraceContextExt, Tracer},
};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_otlp::{LogExporter, MetricExporter, Protocol, SpanExporter};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::{
    logs::SdkLoggerProvider, metrics::SdkMeterProvider, trace::SdkTracerProvider,
};
use std::alloc;
use std::sync::OnceLock;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::prelude::*;

#[global_allocator]
static ALLOCATOR: Cap<alloc::System> = Cap::new(alloc::System, usize::MAX);

fn get_resource() -> Resource {
    static RESOURCE: OnceLock<Resource> = OnceLock::new();
    RESOURCE
        .get_or_init(|| Resource::builder().with_service_name("mmoldb").build())
        .clone()
}

fn init_logs() -> SdkLoggerProvider {
    let exporter = LogExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary)
        .build()
        .expect("Failed to create log exporter");

    SdkLoggerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(get_resource())
        .build()
}

fn init_traces() -> SdkTracerProvider {
    let exporter = SpanExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary) //can be changed to `Protocol::HttpJson` to export in JSON format
        .build()
        .expect("Failed to create trace exporter");

    SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(get_resource())
        .build()
}

fn init_metrics() -> SdkMeterProvider {
    let exporter = MetricExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary) //can be changed to `Protocol::HttpJson` to export in JSON format
        .build()
        .expect("Failed to create metric exporter");

    SdkMeterProvider::builder()
        .with_periodic_exporter(exporter)
        .with_resource(get_resource())
        .build()
}

#[tokio::main]
async fn main() -> miette::Result<()> {
    let logger_provider = init_logs();

    // Create a new OpenTelemetryTracingBridge using the above LoggerProvider.
    let otel_layer = OpenTelemetryTracingBridge::new(&logger_provider);

    // To prevent a telemetry-induced-telemetry loop, OpenTelemetry's own internal
    // logging is properly suppressed. However, logs emitted by external components
    // (such as reqwest, tonic, etc.) are not suppressed as they do not propagate
    // OpenTelemetry context. Until this issue is addressed
    // (https://github.com/open-telemetry/opentelemetry-rust/issues/2877),
    // filtering like this is the best way to suppress such logs.
    //
    // The filter levels are set as follows:
    // - Allow `info` level and above by default.
    // - Completely restrict logs from `hyper`, `tonic`, `h2`, and `reqwest`.
    //
    // Note: This filtering will also drop logs from these components even when
    // they are used outside of the OTLP Exporter.
    let filter_otel = EnvFilter::new("info")
        .add_directive("hyper=off".parse().unwrap())
        .add_directive("tonic=off".parse().unwrap())
        .add_directive("h2=off".parse().unwrap())
        .add_directive("reqwest=off".parse().unwrap());
    let otel_layer = otel_layer.with_filter(filter_otel);

    // Create a new tracing::Fmt layer to print the logs to stdout.
    let filter_fmt = EnvFilter::new("info");
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_thread_names(true)
        .with_filter(filter_fmt);

    // Initialize the tracing subscriber with the OpenTelemetry layer and the
    // Fmt layer.
    tracing_subscriber::registry()
        .with(otel_layer)
        .with(fmt_layer)
        .init();

    // At this point Logs (OTel Logs and Fmt Logs) are initialized, which will
    // allow internal-logs from Tracing/Metrics initializer to be captured.

    let tracer_provider = init_traces();
    // Set the global tracer provider using a clone of the tracer_provider.
    // Setting global tracer provider is required if other parts of the application
    // uses global::tracer() or global::tracer_with_version() to get a tracer.
    // Cloning simply creates a new reference to the same tracer provider. It is
    // important to hold on to the tracer_provider here, so as to invoke
    // shutdown on it when application ends.
    global::set_tracer_provider(tracer_provider.clone());

    let meter_provider = init_metrics();
    // Set the global meter provider using a clone of the meter_provider.
    // Setting global meter provider is required if other parts of the application
    // uses global::meter() or global::meter_with_version() to get a meter.
    // Cloning simply creates a new reference to the same meter provider. It is
    // important to hold on to the meter_provider here, so as to invoke
    // shutdown on it when application ends.
    global::set_meter_provider(meter_provider.clone());

    let config = IngestConfig::config().into_diagnostic()?;
    let config = Box::new(config);
    let config = Box::<IngestConfig>::leak(config);
    let pool = mmoldb_db::get_pool(config.db_pool_size).into_diagnostic()?;

    mmoldb_db::run_migrations().into_diagnostic()?;

    let mut previous_ingest_start_time = if config.start_ingest_every_launch {
        None
    } else {
        let mut conn = pool.get().into_diagnostic()?;
        db::latest_ingest_start_time(&mut conn)
            .into_diagnostic()?
            .map(|dt| dt.and_utc())
    };

    loop {
        let (why, ingest_start) = if let Some(prev_start) = previous_ingest_start_time {
            let next_start = prev_start + chrono::Duration::seconds(config.ingest_period);
            let wait_duration_chrono = next_start - Utc::now();

            // std::time::Duration can't represent negative durations.
            // Conveniently, the conversion from chrono to std also
            // performs the negativity check we would be doing anyway.
            match wait_duration_chrono.to_std() {
                Ok(wait_duration) => {
                    info!("Next ingest {}", HumanTime::from(wait_duration_chrono));
                    tokio::select! {
                        _ = tokio::time::sleep(wait_duration) => {},
                        _ = signal::wait_for_signal() => { break; }
                    }

                    let why = format!("after waiting {}", HumanTime::from(wait_duration_chrono));
                    (why, next_start)
                }
                Err(_) => {
                    // Indicates the wait duration was negative
                    let why = format!(
                        "immediately (next scheduled ingest was {})",
                        HumanTime::from(next_start)
                    );
                    (why, Utc::now())
                }
            }
        } else if config.start_ingest_every_launch {
            let why = "immediately (start_ingest_every_launch is true)".to_string();
            (why, Utc::now())
        } else {
            let why = "immediately (this is the first ingest)".to_string();
            (why, Utc::now())
        };

        // I put this outside the `if` cluster intentionally, so the
        // compiler will error if I miss a code path
        info!("Starting ingest {why}");
        previous_ingest_start_time = Some(ingest_start);

        run_one_ingest(pool.clone(), config).await;
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

    let memory_tracking_task = tokio::task::spawn_blocking({
        let mem_meter = global::meter("memory-meter");
        let mem_gauge = mem_meter
            .u64_gauge("memory-gauge")
            .with_unit("bytes")
            .build();
        let stop_entity_counting = stop_entity_counting.clone();
        move || {
            loop {
                if block_until_exit(stop_entity_counting.clone(), Duration::from_secs(10)) {
                    break;
                }

                let allocated = ALLOCATOR.allocated();
                println!("Total memory allocated: {}", format_size(allocated, BINARY));
                mem_gauge.record(allocated as u64, &[]);
            }
        }
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

    info!("Waiting for memory tracking task to finish");
    match memory_tracking_task.await {
        Ok(()) => {}
        Err(error) => {
            error!("Error joining memory counting task: {}", error)
        }
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

    if config.game_ingest.enable {
        info!("Beginning game ingest");
        errs.extend(
            ingest_games::ingest_games(
                pool.clone(),
                ingest_id,
                abort,
                config.use_local_cheap_cashews,
            )
            .await,
        );
    }

    errs
}
