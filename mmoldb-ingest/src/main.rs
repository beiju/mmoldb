mod ingest;
mod ingest_games;
mod ingest_players;
mod ingest_feed;
mod signal;

use chrono::Utc;
use chrono_humanize::{Accuracy, HumanTime, Tense};
use futures::pin_mut;
use log::{debug, info};
use miette::{Diagnostic, IntoDiagnostic};
use mmoldb_db::{Connection, PgConnection, db};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Error, Diagnostic)]
#[error(transparent)]
struct BoxedError(#[from] Box<dyn std::error::Error + Send + Sync + 'static>);

const START_INGEST_EVERY_LAUNCH: bool = true;
const INGEST_PERIOD_SEC: i64 = 30 * 60;
const STATEMENT_TIMEOUT_SEC: i64 = 0;
const ENABLE_PLAYER_INGEST: bool = true;

#[tokio::main]
async fn main() -> miette::Result<()> {
    env_logger::init();
    console_subscriber::init();

    info!("Waiting 5 seconds for db to launch (temporary)");
    // TODO Some more robust solution for handing db launch delay
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    let url = mmoldb_db::postgres_url_from_environment();

    debug!("START_INGEST_EVERY_LAUNCH={START_INGEST_EVERY_LAUNCH}");
    let mut previous_ingest_start_time = if START_INGEST_EVERY_LAUNCH {
        None
    } else {
        let mut conn = PgConnection::establish(&url).into_diagnostic()?;
        db::latest_ingest_start_time(&mut conn)
            .into_diagnostic()?
            .map(|dt| dt.and_utc())
    };

    loop {
        let (why, ingest_start) = if let Some(prev_start) = previous_ingest_start_time {
            let next_start = prev_start + chrono::Duration::seconds(INGEST_PERIOD_SEC);
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
        } else {
            let why = "immediately (this is the first ingest)".to_string();
            (why, Utc::now())
        };

        // I put this outside the `if` cluster intentionally, so the
        // compiler will error if I miss a code path
        info!("Starting ingest {why}");
        previous_ingest_start_time = Some(ingest_start);

        run_one_ingest(url.clone()).await?;
    }

    Ok(())
}

async fn run_one_ingest(url: String) -> miette::Result<()> {
    let ingest_start_time = Utc::now();
    let mut conn = PgConnection::establish(&url).into_diagnostic()?;

    if STATEMENT_TIMEOUT_SEC < 0 {
        panic!("Negative STATEMENT_TIMEOUT_SEC not allowed ({STATEMENT_TIMEOUT_SEC})");
    } else if STATEMENT_TIMEOUT_SEC > 0 {
        info!(
            "Setting our account's statement timeout to {STATEMENT_TIMEOUT_SEC} ({})",
            chrono_humanize::HumanTime::from(chrono::Duration::seconds(STATEMENT_TIMEOUT_SEC))
                .to_text_en(Accuracy::Precise, Tense::Present),
        );
    } else {
        // Postgres interprets 0 as no timeout
        info!("Setting our account's statement timeout to no timeout");
    }
    db::set_current_user_statement_timeout(&mut conn, STATEMENT_TIMEOUT_SEC).into_diagnostic()?;

    let ingest_id = db::start_ingest(&mut conn, ingest_start_time).into_diagnostic()?;

    let abort = CancellationToken::new();
    let ingest_task = ingest_everything(url.clone(), ingest_id, abort.clone());
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
        debug!("Waiting for ingest task to shut down after abort");
        ingest_task.await?;
    }

    let ingest_end_time = Utc::now();
    let duration = HumanTime::from(ingest_end_time - ingest_start_time);
    if is_aborted {
        db::mark_ingest_aborted(&mut conn, ingest_id, ingest_end_time).into_diagnostic()?;
        info!(
            "Aborted ingest in {}",
            duration.to_text_en(Accuracy::Precise, Tense::Present)
        );
    } else {
        // TODO Remove the start_next_ingest_at_page column from ingests
        db::mark_ingest_finished(&mut conn, ingest_id, ingest_end_time, None).into_diagnostic()?;
        info!(
            "Finished ingest in {}",
            duration.to_text_en(Accuracy::Precise, Tense::Present)
        );
    }

    result
}

async fn ingest_everything(
    pg_url: String,
    ingest_id: i64,
    abort: CancellationToken,
) -> miette::Result<()> {
    if ENABLE_PLAYER_INGEST {
        ingest_players::ingest_players(pg_url.clone(), abort.clone()).await?;
        // Player feed ingest has to start after all players with inbuilt feeds are processed
        ingest_feed::ingest_player_feeds(pg_url.clone(), abort.clone()).await?;
    }
    ingest_games::ingest_games(pg_url, ingest_id, abort).await
}
