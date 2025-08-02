use diesel::Connection;
use mmoldb_db::db;
use mmoldb_db::models::DbEventIngestLog;
use rocket::{get, uri};
use rocket_dyn_templates::{Template, context};
use serde::Serialize;

use super::docs_pages::*;
use crate::Db;
use crate::web::error::AppError;
use crate::web::utility_contexts::{DayContext, FormattedDateContext, GameContext};

const PAGE_OF_GAMES_SIZE: usize = 100;

#[get("/game/<mmolb_game_id>")]
pub async fn game_page(mmolb_game_id: String, db: Db) -> Result<Template, AppError> {
    #[derive(Serialize)]
    struct LogContext {
        level: &'static str,
        text: String,
    }

    impl From<DbEventIngestLog> for LogContext {
        fn from(value: DbEventIngestLog) -> Self {
            LogContext {
                level: match value.log_level {
                    0 => "critical",
                    1 => "error",
                    2 => "warning",
                    3 => "info",
                    4 => "debug",
                    5 => "trace",
                    _ => "unknown",
                },
                text: value.log_text,
            }
        }
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
        day: DayContext,
        away_team_emoji: String,
        away_team_name: String,
        away_team_mmolb_id: String,
        home_team_emoji: String,
        home_team_name: String,
        home_team_mmolb_id: String,
        game_wide_logs: Vec<LogContext>,
        events: Vec<EventContext>,
    }

    let full_game = db
        .run(move |conn| db::game_and_raw_events(conn, &mmolb_game_id))
        .await?;
    let watch_uri = format!("https://mmolb.com/watch/{}", full_game.game.mmolb_game_id);
    let api_uri = format!(
        "https://mmolb.com/api/game/{}",
        full_game.game.mmolb_game_id
    );
    let game = GameContext {
        id: full_game.game.mmolb_game_id,
        watch_uri,
        api_uri,
        season: full_game.game.season,
        day: (full_game.game.day, full_game.game.superstar_day).into(),
        away_team_emoji: full_game.game.away_team_emoji,
        away_team_name: full_game.game.away_team_name,
        away_team_mmolb_id: full_game.game.away_team_mmolb_id,
        home_team_emoji: full_game.game.home_team_emoji,
        home_team_name: full_game.game.home_team_name,
        home_team_mmolb_id: full_game.game.home_team_mmolb_id,
        game_wide_logs: full_game
            .game_wide_logs
            .into_iter()
            .map(Into::into)
            .collect(),
        events: full_game
            .raw_events_with_logs
            .into_iter()
            .map(|(raw_event, logs)| EventContext {
                game_event_index: raw_event.game_event_index,
                text: raw_event.event_text,
                logs: logs.into_iter().map(Into::into).collect(),
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
pub async fn ingest_page(ingest_id: i64, db: Db) -> Result<Template, AppError> {
    paginated_ingest(ingest_id, None, db).await
}

#[get("/ingest/<ingest_id>/page/<after_game_id>")]
pub async fn paginated_ingest_page(
    ingest_id: i64,
    after_game_id: String,
    db: Db,
) -> Result<Template, AppError> {
    paginated_ingest(ingest_id, Some(after_game_id), db).await
}

async fn paginated_ingest(
    ingest_id: i64,
    after_game_id: Option<String>,
    db: Db,
) -> Result<Template, AppError> {
    #[derive(Serialize)]
    struct IngestContext {
        id: i64,
        started_at: FormattedDateContext,
        finished_at: Option<FormattedDateContext>,
        aborted_at: Option<FormattedDateContext>,
        games: Vec<GameContext>,
    }

    let (ingest, games) = db
        .run(move |conn| {
            db::ingest_with_games(
                conn,
                ingest_id,
                PAGE_OF_GAMES_SIZE,
                after_game_id.as_deref(),
            )
        })
        .await?;

    let games_context = paginated_games_context(
        games,
        |game_id| uri!(paginated_ingest_page(ingest_id, game_id)).to_string(),
        || uri!(ingest_page(ingest_id)).to_string(),
    );

    let ingest = IngestContext {
        id: ingest.id,
        started_at: (&ingest.started_at).into(),
        finished_at: ingest.finished_at.as_ref().map(Into::into),
        aborted_at: ingest.aborted_at.as_ref().map(Into::into),
        games: games_context.games,
    };

    Ok(Template::render(
        "ingest",
        context! {
            index_url: uri!(index_page()),
            ingest: ingest,
            next_page_url: games_context.next_page_url,
            previous_page_url: games_context.previous_page_url,
        },
    ))
}

#[get("/games/page/<after_game_id>")]
pub async fn paginated_games_page(after_game_id: String, db: Db) -> Result<Template, AppError> {
    paginated_games(Some(after_game_id), db).await
}

#[get("/games")]
pub async fn games_page(db: Db) -> Result<Template, AppError> {
    paginated_games(None, db).await
}

async fn paginated_games(after_game_id: Option<String>, db: Db) -> Result<Template, AppError> {
    let page = db
        .run(move |conn| {
            conn.transaction(|conn| {
                db::page_of_games(conn, PAGE_OF_GAMES_SIZE, after_game_id.as_deref())
            })
        })
        .await?;

    Ok(Template::render(
        "games",
        paginated_games_context(
            page,
            |game_id| uri!(paginated_games_page(game_id)).to_string(),
            || uri!(games_page()).to_string(),
        ),
    ))
}

#[derive(Serialize)]
struct PaginatedGamesContext<'a> {
    index_url: String,
    subhead: &'a str,
    games: Vec<GameContext>,
    next_page_url: Option<String>,
    previous_page_url: Option<String>,
}

fn paginated_games_context(
    page: db::PageOfGames,
    paginated_uri_builder: impl Fn(&str) -> String,
    non_paginated_uri_builder: impl Fn() -> String,
) -> PaginatedGamesContext<'static> {
    PaginatedGamesContext {
        index_url: uri!(index_page()).to_string(),
        subhead: "Games",
        games: GameContext::from_db(page.games, |game_id| uri!(game_page(game_id)).to_string()),
        next_page_url: page.next_page.as_deref().map(&paginated_uri_builder),
        previous_page_url: page.previous_page.map(|previous_page| match previous_page {
            Some(page) => paginated_uri_builder(&page),
            None => non_paginated_uri_builder(),
        }),
    }
}

#[get("/games-with-issues/page/<after_game_id>")]
pub async fn paginated_games_with_issues_page(
    after_game_id: String,
    db: Db,
) -> Result<Template, AppError> {
    paginated_games_with_issues(Some(after_game_id), db).await
}

#[get("/games-with-issues")]
pub async fn games_with_issues_page(db: Db) -> Result<Template, AppError> {
    paginated_games_with_issues(None, db).await
}

async fn paginated_games_with_issues(
    after_game_id: Option<String>,
    db: Db,
) -> Result<Template, AppError> {
    let page = db
        .run(move |conn| {
            conn.transaction(|conn| {
                db::page_of_games_with_issues(conn, PAGE_OF_GAMES_SIZE, after_game_id.as_deref())
            })
        })
        .await?;

    Ok(Template::render(
        "games",
        paginated_games_context(
            page,
            |game_id| uri!(paginated_games_with_issues_page(game_id)).to_string(),
            || uri!(games_with_issues_page()).to_string(),
        ),
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

#[get("/status")]
pub async fn status_page(db: Db) -> Result<Template, AppError> {
    #[derive(Serialize, Default)]
    struct IngestTaskContext {
        is_starting: bool,
        is_stopping: bool,
        // Running means actively ingesting. If it's idle this will be false.
        is_running: bool,
        error: Option<String>,
    }

    // TODO Restore status report for ingest task
    let ingest_task_status = IngestTaskContext::default();

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
        .run(move |conn| {
            conn.transaction(|conn| {
                let num_games = db::game_count(conn)?;
                let num_games_with_issues = db::game_with_issues_count(conn)?;

                let num_ingests = db::ingest_count(conn)?;
                let latest_ingests = db::latest_ingests(conn)?;
                Ok::<_, AppError>((
                    num_games,
                    num_games_with_issues,
                    num_ingests,
                    latest_ingests,
                ))
            })
        })
        .await?;

    let number_of_ingests_not_shown = total_num_ingests - displayed_ingests.len() as i64;
    let ingests: Vec<_> = displayed_ingests
        .into_iter()
        .map(|ingest| IngestContext {
            uri: uri!(ingest_page(ingest.id)).to_string(),
            num_games: ingest.num_games,
            started_at: (&ingest.started_at).into(),
            finished_at: ingest.finished_at.as_ref().map(Into::into),
            aborted_at: ingest.aborted_at.as_ref().map(Into::into),
        })
        .collect();

    let last_ingest_finished_at = ingests
        .first()
        .and_then(|ingest| ingest.finished_at.clone());

    Ok(Template::render(
        "status",
        context! {
            index_url: uri!(index_page()),
            status_url: uri!(status_page()),
            docs_url: uri!(docs_page()),
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

#[get("/")]
pub async fn index_page() -> Template {
    Template::render(
        "index",
        context! {
            index_url: uri!(index_page()),
            status_url: uri!(status_page()),
            docs_url: uri!(docs_page()),
            // This markdown conversion could be cached
            changelog: markdown::to_html(
                include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../changelog.md")),
            ),
        },
    )
}

#[get("/debug-always-error")]
pub async fn debug_always_error_page() -> Result<Template, AppError> {
    Err(AppError::TestError)
}
