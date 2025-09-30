use chrono::{DateTime, Utc};
use diesel::{Connection, PgConnection};
use num_format::{Locale, ToFormattedString};
use mmoldb_db::db;
use mmoldb_db::models::DbEventIngestLog;
use rocket::{get, uri, State};
use rocket::http::uri::Origin;
use rocket_dyn_templates::{Template, context};
use serde::Serialize;
use lazy_static::lazy_static;
use log::warn;
use mmoldb_db::db::{GamesStats, PlayersStats, TeamsStats};
use super::docs_pages::*;
use crate::Db;
use crate::records_cache::{Record, RecordsCache};
use crate::web::error::AppError;
use crate::web::utility_contexts::{DayContext, FormattedDateContext, GameContext};

const PAGE_OF_GAMES_SIZE: usize = 100;

#[derive(Debug, Clone, Serialize)]
struct NavPage<'a> {
    name: &'a str,
    url: Origin<'a>,
}

impl NavPage<'_> {
    pub fn new<'a>(name: &'a str, url: Origin<'a>) -> NavPage<'a> {
        NavPage { name, url }
    }
}

lazy_static! {
    static ref PAGES: [NavPage<'static>; 5] = [
        NavPage::new("Home", uri!(index_page())),
        NavPage::new("Status", uri!(status_page())),
        NavPage::new("Health", uri!(health_page())),
        NavPage::new("Docs", uri!(docs_page())),
        NavPage::new("Records", uri!(records_page())),
    ];
}

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
        message: Option<String>,
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
            message: ingest.message,
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
            health_url: uri!(health_page()),
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

#[derive(Serialize)]
struct Stat {
    title: &'static str,
    number: String,
}

impl Stat {
    pub fn new(title: &'static str, number: i64) -> Self {
        Self {
            title,
            number: number.to_formatted_string(&Locale::en),
        }
    }
}

#[derive(Serialize)]
struct StatCategory {
    title: &'static str,
    stats: Vec<Stat>,
}

fn games_health(conn: &mut PgConnection) -> Result<StatCategory, AppError> {
    let GamesStats {
        num_games,
        num_events,
        num_baserunners,
        num_fielders,
        num_pitcher_changes,
        num_aurora_photos,
        num_ejections,
        num_parties,
        num_games_with_issues,
    } = db::games_stats(conn)?;

    Ok(StatCategory {
        title: "Games",
        stats: vec![
            Stat::new("Games", num_games),
            Stat::new("Events", num_events),
            Stat::new("Baserunners", num_baserunners),
            Stat::new("Fielders", num_fielders),
            Stat::new("Pitcher changes", num_pitcher_changes),
            Stat::new("Aurora photos", num_aurora_photos),
            Stat::new("Ejections", num_ejections),
            Stat::new("Parties", num_parties),
            Stat::new("Games with issues", num_games_with_issues),
        ],
    })
}

fn players_health(conn: &mut PgConnection) -> Result<StatCategory, AppError> {
    let PlayersStats {
        num_player_versions,
        num_player_modification_versions,
        num_player_equipment_versions,
        num_player_equipment_effect_versions,
        num_player_report_versions,
        num_player_report_attribute_versions,
        num_player_attribute_augments,
        num_player_paradigm_shifts,
        num_player_recompositions,
        num_player_feed_versions,
        num_players_with_issues,
    } = db::players_stats(conn)?;

    Ok(StatCategory {
        title: "Players",
        stats: vec![
            Stat::new("Player versions", num_player_versions),
            Stat::new("Player mod versions", num_player_modification_versions),
            Stat::new("Player equipment versions", num_player_equipment_versions),
            Stat::new("Player equipment effect versions", num_player_equipment_effect_versions),
            Stat::new("Player report versions", num_player_report_versions),
            Stat::new("Player report attribute version", num_player_report_attribute_versions),
            Stat::new("Player attribute augments", num_player_attribute_augments),
            Stat::new("Player paradigm shifts", num_player_paradigm_shifts),
            Stat::new("Player recompositions", num_player_recompositions),
            Stat::new("player feed versions", num_player_feed_versions),
            Stat::new("Players with issues", num_players_with_issues),
        ],
    })
}

fn teams_health(conn: &mut PgConnection) -> Result<StatCategory, AppError> {
    let TeamsStats {
        num_team_versions,
        num_team_player_versions,
        num_team_games_played,
        num_team_feed_versions,
        num_teams_with_issues,
    } = db::teams_stats(conn)?;

    Ok(StatCategory {
        title: "Teams",
        stats: vec![
            Stat::new("Team versions", num_team_versions),
            Stat::new("Team player versions", num_team_player_versions),
            Stat::new("Team games played", num_team_games_played),
            Stat::new("Team feed versions", num_team_feed_versions),
            Stat::new("Teams with issues", num_teams_with_issues),
        ],
    })
}

#[get("/health")]
pub async fn health_page(db: Db) -> Result<Template, AppError> {
    let stat_categories = db.run(|mut conn| Ok::<_, AppError>(vec![
        games_health(&mut conn)?,
        players_health(&mut conn)?,
        teams_health(&mut conn)?,
    ])).await?;

    Ok(Template::render(
        "health",
        context! {
            index_url: uri!(index_page()),
            status_url: uri!(status_page()),
            health_url: uri!(health_page()),
            docs_url: uri!(docs_page()),
            stat_categories: stat_categories,
        },
    ))
}

#[get("/records")]
pub async fn records_page(records: &State<RecordsCache>) -> Result<Template, AppError> {
    let update = records.update_date()
        .map(|d| FormattedDateContext::from(&d.naive_utc()));
    let error = records.update_error();
    let records = records.latest();

    #[derive(Serialize)]
    struct LatestGameContext {
        date: DateTime<Utc>,
        date_display: String,
        mmolb_season: i32,
        mmolb_day_display: String,
    }

    #[derive(Serialize)]
    struct RecordContext {
        title: String,
        description: Option<String>,
        mmolb_team_id: String,
        team_emoji: String,
        team_location: String,
        team_name: String,
        mmolb_player_id: String,
        player_name: String,
        mmolb_game_id: String,
        game_event_index: i32,
        record: String,
    }

    #[derive(Serialize)]
    struct RecordsContext {
        latest_game: Option<LatestGameContext>,
        records: Vec<Record>,
    }

    let records_context = records.map(|r| RecordsContext {
        latest_game: r.latest_game.map(|g| LatestGameContext {
            date: g.time,
            date_display: g.time.format("%Y %b %e, %T UTC").to_string(),
            mmolb_season: g.season,
            mmolb_day_display: match (g.day, g.superstar_day) {
                (None, None) => "Unknown day".to_string(),
                (Some(day), None) => format!("Day {day}"),
                (None, Some(superstar_day)) => format!("Superstar Day {superstar_day}"),
                (Some(day), Some(superstar_day)) => {
                    warn!("Latest game had both a day and a superstar day");
                    format!("Day {day} Superstar Day {superstar_day}")
                },
            },
        }),
        records: r.records
    });

    Ok(Template::render(
        "records",
        context! {
            index_url: uri!(index_page()),
            pages: &*PAGES,
            error: error,
            update: update,
            records: records_context,
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
            health_url: uri!(health_page()),
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
