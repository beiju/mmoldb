use rocket::{State, get, uri};
use rocket_db_pools::Connection;
use rocket_db_pools::diesel::AsyncConnection;
use rocket_db_pools::diesel::scoped_futures::ScopedFutureExt;
use rocket_dyn_templates::{Template, context};
use serde::Serialize;

use crate::ingest::{IngestStatus, IngestTask};
use crate::web::error::AppError;
use crate::web::utility_contexts::{FormattedDateContext, GameContext};
use crate::{Db, db};

#[get("/game/<game_id>")]
pub async fn game_page(game_id: i64, mut db: Connection<Db>) -> Result<Template, AppError> {
    #[derive(Serialize)]
    struct LogContext {
        level: &'static str,
        text: String,
    }

    #[derive(Serialize)]
    struct EventContext {
        game_event_index: i32,
        text: String,
        logs: Vec<LogContext>,
    }

    #[derive(Serialize)]
    struct GameContext {
        id: String,
        watch_uri: String,
        api_uri: String,
        season: i32,
        day: i32,
        away_team_emoji: String,
        away_team_name: String,
        away_team_id: String,
        home_team_emoji: String,
        home_team_name: String,
        home_team_id: String,
        events: Vec<EventContext>,
    }

    let (game, events) = db::game_and_raw_events(&mut db, game_id).await?;
    let watch_uri = format!("https://mmolb.com/watch/{}", game.mmolb_game_id);
    let api_uri = format!("https://mmolb.com/api/game/{}", game.mmolb_game_id);
    let game = GameContext {
        id: game.mmolb_game_id,
        watch_uri,
        api_uri,
        season: game.season,
        day: game.day,
        away_team_emoji: game.away_team_emoji,
        away_team_name: game.away_team_name,
        away_team_id: game.away_team_id,
        home_team_emoji: game.home_team_emoji,
        home_team_name: game.home_team_name,
        home_team_id: game.home_team_id,
        events: events
            .into_iter()
            .map(|(event, logs)| EventContext {
                game_event_index: event.game_event_index,
                text: event.event_text,
                logs: logs
                    .into_iter()
                    .map(|log| LogContext {
                        level: match log.log_level {
                            0 => "critical",
                            1 => "error",
                            2 => "warning",
                            3 => "info",
                            4 => "debug",
                            5 => "trace",
                            _ => "unknown",
                        },
                        text: log.log_text,
                    })
                    .collect(),
            })
            .collect(),
    };

    Ok(Template::render(
        "game",
        context! {
            index_url: uri!(index_page()),
            game: game,
        },
    ))
}

#[get("/ingest/<ingest_id>")]
pub async fn ingest_page(ingest_id: i64, mut db: Connection<Db>) -> Result<Template, AppError> {
    #[derive(Serialize)]
    struct IngestContext {
        id: i64,
        started_at: FormattedDateContext,
        finished_at: Option<FormattedDateContext>,
        aborted_at: Option<FormattedDateContext>,
        games: Vec<GameContext>,
    }

    let (ingest, games) = db::ingest_with_games(&mut db, ingest_id).await?;
    let ingest = IngestContext {
        id: ingest.id,
        started_at: (&ingest.started_at).into(),
        finished_at: ingest.finished_at.as_ref().map(Into::into),
        aborted_at: ingest.aborted_at.as_ref().map(Into::into),
        games: GameContext::from_db(games, |game_id| uri!(game_page(game_id)).to_string()),
    };

    Ok(Template::render(
        "ingest",
        context! {
            index_url: uri!(index_page()),
            ingest: ingest,
        },
    ))
}

#[get("/games")]
pub async fn games_page(mut db: Connection<Db>) -> Result<Template, AppError> {
    let games = db
        .transaction(|conn| async move { db::all_games(conn).await }.scope_boxed())
        .await?;

    Ok(Template::render(
        "games",
        context! {
            index_url: uri!(index_page()),
            subhead: "Games",
            games: GameContext::from_db(games, |game_id| uri!(game_page(game_id)).to_string()),
        },
    ))
}

#[get("/games-with-issues")]
pub async fn games_with_issues_page(mut db: Connection<Db>) -> Result<Template, AppError> {
    // TODO Get rid of this query + filter and replace it with a query
    //   that only returns what I need
    let games = db
        .transaction(|conn| async move { db::all_games(conn).await }.scope_boxed())
        .await?;

    let games = games
        .into_iter()
        .filter(|(_, num_warnings, num_errors, num_critical)| {
            *num_warnings != 0 || *num_errors != 0 || *num_critical != 0
        });

    Ok(Template::render(
        "games",
        context! {
            index_url: uri!(index_page()),
            subhead: "Games with issues",
            games: GameContext::from_db(games, |game_id| uri!(game_page(game_id)).to_string()),
        },
    ))
}

#[get("/debug-no-games")]
pub async fn debug_no_games_page() -> Result<Template, AppError> {
    let games = Vec::new();

    Ok(Template::render(
        "games",
        context! {
            index_url: uri!(index_page()),
            subhead: "[debug] No games",
            games: GameContext::from_db(games, |game_id| uri!(game_page(game_id)).to_string()),
        },
    ))
}

#[get("/")]
pub async fn index_page(
    mut db: Connection<Db>,
    ingest_task: &State<IngestTask>,
) -> Result<Template, AppError> {
    #[derive(Serialize, Default)]
    struct IngestTaskContext {
        is_starting: bool,
        is_stopping: bool,
        // Running means actively ingesting. If it's idle this will be false.
        is_running: bool,
        error: Option<String>,
    }

    let ingest_task_status = match ingest_task.state().await {
        IngestStatus::Starting => IngestTaskContext {
            is_starting: true,
            ..Default::default()
        },
        IngestStatus::FailedToStart(err) => IngestTaskContext {
            error: Some(err),
            ..Default::default()
        },
        IngestStatus::Idle => IngestTaskContext::default(),
        IngestStatus::Running => IngestTaskContext {
            is_running: true,
            ..Default::default()
        },
        IngestStatus::ExitedWithError(err) => IngestTaskContext {
            error: Some(err),
            ..Default::default()
        },
        IngestStatus::ShuttingDown => IngestTaskContext {
            is_stopping: true,
            ..Default::default()
        },
    };

    #[derive(Serialize)]
    struct IngestContext {
        uri: String,
        num_games: i64,
        started_at: FormattedDateContext,
        finished_at: Option<FormattedDateContext>,
        aborted_at: Option<FormattedDateContext>,
    }

    // A transaction is probably overkill for this, but it's
    // TECHNICALLY the only correct way to make sure that the
    // value of number_of_ingests_not_shown is correct
    let (total_games, total_games_with_issues, total_num_ingests, displayed_ingests) = db
        .transaction::<_, diesel::result::Error, _>(|conn| {
            async move {
                let num_games = db::game_count(conn).await?;
                let num_games_with_issues = db::game_with_issues_count(conn).await?;

                let num_ingests = db::ingest_count(conn).await?;
                let latest_ingests = db::latest_ingests(conn).await?;
                Ok((
                    num_games,
                    num_games_with_issues,
                    num_ingests,
                    latest_ingests,
                ))
            }
            .scope_boxed()
        })
        .await?;

    let number_of_ingests_not_shown = total_num_ingests - displayed_ingests.len() as i64;
    let ingests: Vec<_> = displayed_ingests
        .into_iter()
        .map(|(ingest, num_games)| IngestContext {
            uri: uri!(ingest_page(ingest.id)).to_string(),
            num_games,
            started_at: (&ingest.started_at).into(),
            finished_at: ingest.finished_at.as_ref().map(Into::into),
            aborted_at: ingest.aborted_at.as_ref().map(Into::into),
        })
        .collect();

    let last_ingest_finished_at = ingests
        .first()
        .and_then(|ingest| ingest.finished_at.clone());

    Ok(Template::render(
        "index",
        context! {
            index_url: uri!(index_page()),
            games_page_url: uri!(games_page()),
            total_games: total_games,
            games_with_issues_page_url: uri!(games_with_issues_page()),
            total_games_with_issues: total_games_with_issues,
            task_status: ingest_task_status,
            last_ingest_finished_at: last_ingest_finished_at,
            ingests: ingests,
            number_of_ingests_not_shown: number_of_ingests_not_shown,
        },
    ))
}

#[get("/debug-always-error")]
pub async fn debug_always_error_page() -> Result<Template, AppError> {
    Err(AppError::TestError)
}
