mod ingest;
mod ingest_player_feed;
mod ingest_team_feed;
mod ingest_games;
mod ingest_players;
mod ingest_teams;
mod signal;
mod config;

use chrono::Utc;
use chrono_humanize::{Accuracy, HumanTime, Tense};
use futures::pin_mut;
use log::{debug, info, warn};
use miette::{Context, IntoDiagnostic};
use mmoldb_db::{db, Connection, PgConnection};
use tokio_util::sync::CancellationToken;
use config::IngestConfig;

#[tokio::main]
async fn main() -> miette::Result<()> {
    env_logger::init();
    console_subscriber::init();

    let config = IngestConfig::config().into_diagnostic()?;

    info!("Waiting 5 seconds for db to launch (temporary)");
    // TODO Some more robust solution for handing db launch delay
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    let url = mmoldb_db::postgres_url_from_environment();

    let mut previous_ingest_start_time = if config.start_ingest_every_launch {
        None
    } else {
        let mut conn = PgConnection::establish(&url).into_diagnostic()?;
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

        run_one_ingest(url.clone(), &config).await?;
    }

    Ok(())
}

async fn run_one_ingest(url: String, config: &IngestConfig) -> miette::Result<()> {
    let ingest_start_time = Utc::now();
    let mut conn = PgConnection::establish(&url).into_diagnostic()?;

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
    let ingest_task = ingest_everything(url.clone(), ingest_id, abort.clone(), config);
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
        // TODO Remove the start_next_ingest_at_page column from ingests
        db::mark_ingest_finished(&mut conn, ingest_id, ingest_end_time, None, err_message.as_deref()).into_diagnostic()?;
        info!(
            "Finished ingest in {}",
            duration.to_text_en(Accuracy::Precise, Tense::Present)
        );
    }

    // Don't return `result` -- we already handled it, and returning it will
    // make the process exit
    Ok(())
}

fn refresh_matviews(pg_url: String) -> miette::Result<()> {
    let mut conn = PgConnection::establish(&pg_url).into_diagnostic()?;
    db::refresh_matviews(&mut conn).into_diagnostic()
}

async fn ingest_everything(
    pg_url: String,
    ingest_id: i64,
    abort: CancellationToken,
    config: &IngestConfig,
) -> miette::Result<()> {
    if config.team_ingest.enable {
        info!("Beginning team ingest");
        ingest_teams::ingest_teams(ingest_id, pg_url.clone(), abort.clone(), &config.team_ingest).await?;
    }

    if config.team_feed_ingest.enable {
        // This could be parallelized with ingest_teams, since we never
        // process the embedded feeds in team objects
        info!("Beginning team feed ingest");
        ingest_team_feed::ingest_team_feeds(ingest_id, pg_url.clone(), abort.clone(), &config.team_feed_ingest).await?;
    }

    if config.player_feed_ingest.enable {
        info!("Beginning player ingest");
        ingest_players::ingest_players(ingest_id, pg_url.clone(), abort.clone(), &config.player_feed_ingest).await?;

        // Player feed ingest can't (currently) run without first running player ingest
        if config.player_feed_ingest.enable {
            info!("Beginning player feed ingest");
            // Player feed ingest has to start after all players with inbuilt feeds are processed
            ingest_player_feed::ingest_player_feeds(ingest_id, pg_url.clone(), abort.clone(), &config.player_feed_ingest).await?;
        }
    } else if config.player_feed_ingest.enable {
        warn!("Can't ingest player feeds without ingesting players");
    }

    if config.game_ingest.enable {
        info!("Beginning game ingest");
        ingest_games::ingest_games(pg_url.clone(), ingest_id, abort).await?;
    }

    info!("Refreshing materialized views");
    refresh_matviews(pg_url)?;

    info!("Ingest complete");
    Ok(())
}
