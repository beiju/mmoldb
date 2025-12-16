mod ingest;
mod ingest_player_feed;
mod ingest_team_feed;
mod ingest_games;
mod ingest_players;
mod ingest_teams;
mod signal;
mod config;
mod partitioner;

use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use crate::ingest_teams::TeamIngest;
use chrono::Utc;
use chrono_humanize::{Accuracy, HumanTime, Tense};
use futures::pin_mut;
use log::{debug, info};
use miette::{Context, IntoDiagnostic};
use mmoldb_db::{db, ConnectionPool};
use tokio_util::sync::CancellationToken;
use config::IngestConfig;

pub use ingest::*;
use crate::ingest_player_feed::PlayerFeedIngest;
use crate::ingest_players::PlayerIngest;
use crate::ingest_team_feed::TeamFeedIngest;

#[tokio::main]
async fn main() -> miette::Result<()> {
    env_logger::init();
    console_subscriber::init();

    let config = IngestConfig::config().into_diagnostic()?;
    let config = Box::new(config);
    let config = Box::<IngestConfig>::leak(config);
    let pool = mmoldb_db::get_pool(config.db_pool_size)
        .into_diagnostic()?;

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

        run_one_ingest(pool.clone(), config).await?;
    }

    Ok(())
}

async fn run_one_ingest(pool: ConnectionPool, config: &'static IngestConfig) -> miette::Result<()> {
    let ingest_start_time = Utc::now();
    let mut conn = pool.get().into_diagnostic()?;

    if let Some(statement_timeout) = config.set_postgres_statement_timeout {
        if statement_timeout < 0 {
            panic!("Negative STATEMENT_TIMEOUT_SEC not allowed ({statement_timeout})");
        } else if statement_timeout > 0 {
            info!(
            "Setting our account's statement timeout to {statement_timeout} ({})",
            chrono_humanize::HumanTime::from(chrono::Duration::seconds(statement_timeout))
                .to_text_en(Accuracy::Precise, Tense::Present),
        );
        } else {
            // Postgres interprets 0 as no timeout
            info!("Setting our account's statement timeout to no timeout");
        }
        db::set_current_user_statement_timeout(&mut conn, statement_timeout).into_diagnostic()?;
    }

    let ingest_id = db::start_ingest(&mut conn, ingest_start_time).into_diagnostic()?;

    let abort = CancellationToken::new();
    let ingest_task = ingest_everything(pool.clone(), ingest_id, abort.clone(), config);
    pin_mut!(ingest_task);

    let (is_aborted, result) = tokio::select! {
        ingest_result = &mut ingest_task => {
            (ingest_result.is_err(), ingest_result)
        }
        _ = signal::wait_for_signal() => {
            debug!("Signaling abort token");
            abort.cancel();
            (true, Ok(()))
        }
    };

    // Note: use abort.is_cancelled() instead of is_aborted because is_aborted
    // is also True if the task finished with a Result::Err(). We don't want to
    // await it in this case.
    if abort.is_cancelled() {
        debug!("Waiting for ingest task to shut down after abort signal");
        ingest_task.await
            .wrap_err("While waiting for ingest task to shut down after abort signal")?;
    }

    let err_message = result.err().map(|err| format!("{:?}", err));

    let ingest_end_time = Utc::now();
    let duration = HumanTime::from(ingest_end_time - ingest_start_time);
    if is_aborted {
        db::mark_ingest_aborted(&mut conn, ingest_id, ingest_end_time, err_message.as_deref()).into_diagnostic()?;
        info!(
            "Aborted ingest in {}",
            duration.to_text_en(Accuracy::Precise, Tense::Present)
        );
    } else {
        db::mark_ingest_finished(&mut conn, ingest_id, ingest_end_time, err_message.as_deref()).into_diagnostic()?;
        info!(
            "Finished ingest in {}",
            duration.to_text_en(Accuracy::Precise, Tense::Present)
        );
    }

    // Don't return `result` -- we already handled it, and returning it will
    // make the process exit
    Ok(())
}

fn refresh_matviews(pool: ConnectionPool) -> miette::Result<()> {
    let mut conn = pool.get().into_diagnostic()?;
    db::refresh_matviews(&mut conn).into_diagnostic()
}

fn refresh_entity_counting_matviews_repeatedly(pool: ConnectionPool, exit: Arc<(Mutex<bool>, Condvar)>) -> miette::Result<()> {
    let mut conn = pool.get().into_diagnostic()?;
    loop {
        {
            let (exit, exit_cond) = &*exit;
            let (should_exit, _) = exit_cond.wait_timeout_while(
                exit.lock().unwrap(),
                Duration::from_secs(10),
                |&mut should_exit| !should_exit,
            ).unwrap();

            if *should_exit {
                break;
            }
        }

        db::refresh_entity_counting_matviews(&mut conn).into_diagnostic()?;
    }

    Ok(())
}

async fn ingest_everything(
    pool: ConnectionPool,
    ingest_id: i64,
    abort: CancellationToken,
    config: &'static IngestConfig,
) -> miette::Result<()> {
    let stop_entity_counting = Arc::new((Mutex::new(false), Condvar::new()));
    let entity_counting_task = tokio::task::spawn_blocking({
        let pool = pool.clone();
        let stop_entity_counting = stop_entity_counting.clone();
        || refresh_entity_counting_matviews_repeatedly(pool, stop_entity_counting)
    });

    let ingestor = Ingestor::new(pool.clone(), ingest_id, abort.clone());

    ingestor.ingest(TeamIngest::new(&config.team_ingest), config.use_local_cheap_cashews).await?;
    ingestor.ingest(TeamFeedIngest::new(&config.team_feed_ingest), config.use_local_cheap_cashews).await?;
    ingestor.ingest(PlayerIngest::new(&config.player_ingest), config.use_local_cheap_cashews).await?;
    ingestor.ingest(PlayerFeedIngest::new(&config.player_feed_ingest), config.use_local_cheap_cashews).await?;
    
    if config.fetch_known_missing_games {
        info!("Fetching any missing games in known-game-ids.txt");
        ingest_games::fetch_missed_games(pool.clone()).await?;
    }

    if config.game_ingest.enable {
        info!("Beginning game ingest");
        ingest_games::ingest_games(pool.clone(), ingest_id, abort, config.use_local_cheap_cashews).await?;
    }

    let (exit, exit_cond) = &*stop_entity_counting;
    {
        let mut should_exit = exit.lock().unwrap();
        *should_exit = true;
    }
    exit_cond.notify_all();
    info!("Waiting for entity counting task to finish");
    entity_counting_task.await.into_diagnostic()??;

    info!("Refreshing materialized views");
    refresh_matviews(pool)?;

    info!("Ingest complete");
    Ok(())
}
