mod raw;
mod to_db_format;
mod weather;

use std::collections::HashMap;
// Reexports
pub use raw::*;
pub use to_db_format::RowToEventError;

// Third-party imports
use chrono::{DateTime, NaiveDateTime, Utc};
use diesel::query_builder::SqlQuery;
use diesel::{PgConnection, prelude::*, sql_query, sql_types::*};
use itertools::{Either, Itertools};
use log::warn;
use mmolb_parsing::ParsedEventMessage;
use mmolb_parsing::enums::Day;
use serde::Serialize;
use std::iter;
use thiserror::Error;
// First-party imports
use crate::event_detail::{EventDetail, IngestLog};
use crate::models::{
    DbEvent, DbEventIngestLog, DbFielder, DbGame, DbIngest, DbRawEvent, DbRunner,
    NewEventIngestLog, NewGame, NewGameIngestTimings, NewIngest, NewRawEvent, RawDbColumn,
    RawDbTable,
};
use crate::taxa::{Taxa};

pub fn ingest_count(conn: &mut PgConnection) -> QueryResult<i64> {
    use crate::info_schema::info::ingests::dsl;

    dsl::ingests.count().get_result(conn)
}

pub fn is_ongoing(conn: &mut PgConnection, ids: &[&str]) -> QueryResult<Vec<(String, bool)>> {
    use crate::data_schema::data::games::dsl;

    dsl::games
        .filter(dsl::mmolb_game_id.eq_any(ids))
        .select((dsl::mmolb_game_id, dsl::is_ongoing))
        .order_by(dsl::mmolb_game_id)
        .get_results(conn)
}

pub fn game_count(conn: &mut PgConnection) -> QueryResult<i64> {
    use crate::data_schema::data::games::dsl::*;

    games.count().get_result(conn)
}

pub fn game_with_issues_count(conn: &mut PgConnection) -> QueryResult<i64> {
    use crate::info_schema::info::event_ingest_log::dsl;

    dsl::event_ingest_log
        .filter(dsl::log_level.lt(3)) // Selects warnings and higher
        .select(diesel::dsl::count_distinct(dsl::game_id))
        .get_result(conn)
}

pub fn latest_ingest_start_time(conn: &mut PgConnection) -> QueryResult<Option<NaiveDateTime>> {
    use crate::info_schema::info::ingests::dsl::*;

    ingests
        .select(started_at)
        .order_by(started_at.desc())
        .first(conn)
        .optional()
}

pub fn next_ingest_start_page(conn: &mut PgConnection) -> QueryResult<Option<String>> {
    use crate::info_schema::info::ingests::dsl::*;

    ingests
        .filter(start_next_ingest_at_page.is_not_null())
        .select(start_next_ingest_at_page)
        .order_by(started_at.desc())
        .first(conn)
        .optional()
        .map(Option::flatten)
}

pub fn update_next_ingest_start_page(
    conn: &mut PgConnection,
    ingest_id: i64,
    next_ingest_start_page: Option<String>,
) -> QueryResult<usize> {
    use crate::info_schema::info::ingests::dsl as ingests_dsl;

    diesel::update(ingests_dsl::ingests)
        .filter(ingests_dsl::id.eq(ingest_id))
        .set(ingests_dsl::start_next_ingest_at_page.eq(next_ingest_start_page))
        .execute(conn)
}

#[derive(QueryableByName)]
pub struct IngestWithGameCount {
    #[diesel(sql_type = Int8)]
    pub id: i64,
    #[diesel(sql_type = Timestamp)]
    pub started_at: NaiveDateTime,
    #[diesel(sql_type = Nullable<Timestamp>)]
    pub finished_at: Option<NaiveDateTime>,
    #[diesel(sql_type = Nullable<Timestamp>)]
    pub aborted_at: Option<NaiveDateTime>,
    #[diesel(sql_type = Int8)]
    pub num_games: i64,
}

pub fn latest_ingests(conn: &mut PgConnection) -> QueryResult<Vec<IngestWithGameCount>> {
    sql_query(
        "
        select i.id, i.started_at, i.finished_at, i.aborted_at, count(g.mmolb_game_id) as num_games
        from info.ingests i
             left join data.games g on g.ingest = i.id
        group by i.id, i.started_at
        order by i.started_at desc
        limit 25
    ",
    )
    .load::<IngestWithGameCount>(conn)
}

pub fn start_ingest(conn: &mut PgConnection, at: DateTime<Utc>) -> QueryResult<i64> {
    use crate::info_schema::info::ingests::dsl::*;

    NewIngest {
        started_at: at.naive_utc(),
    }
    .insert_into(ingests)
    .returning(id)
    .get_result(conn)
}

pub fn mark_ingest_finished(
    conn: &mut PgConnection,
    ingest_id: i64,
    at: DateTime<Utc>,
    start_next_ingest_at_page: Option<&str>,
) -> QueryResult<()> {
    use crate::info_schema::info::ingests::dsl;

    diesel::update(dsl::ingests.filter(dsl::id.eq(ingest_id)))
        .set((
            dsl::finished_at.eq(at.naive_utc()),
            dsl::start_next_ingest_at_page.eq(start_next_ingest_at_page),
        ))
        .execute(conn)
        .map(|_| ())
}

// It is assumed that an aborted ingest won't have a later
// latest_completed_season than the previous ingest. That is not
// necessarily true, but it's inconvenient to refactor the code to
// support it.
pub fn mark_ingest_aborted(
    conn: &mut PgConnection,
    ingest_id: i64,
    at: DateTime<Utc>,
) -> QueryResult<()> {
    use crate::info_schema::info::ingests::dsl::*;

    diesel::update(ingests.filter(id.eq(ingest_id)))
        .set(aborted_at.eq(at.naive_utc()))
        .execute(conn)
        .map(|_| ())
}

macro_rules! log_only_assert {
    ($e: expr, $($msg:tt)*) => {
        if !$e {
            log::error!($($msg)*)
        }
    };
}

#[derive(QueryableByName)]
#[diesel(table_name = crate::data_schema::data::games)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct GameWithIssueCounts {
    #[diesel(embed)]
    pub game: DbGame,
    #[diesel(sql_type = Int8)]
    pub warnings_count: i64,
    #[diesel(sql_type = Int8)]
    pub errors_count: i64,
    #[diesel(sql_type = Int8)]
    pub critical_count: i64,
}

pub fn games_list_base() -> SqlQuery {
    sql_query(
        "
        with counts as (select
                l.game_id,
                sum(case when l.log_level = 0 then 1 else 0 end) as critical_count,
                sum(case when l.log_level = 1 then 1 else 0 end) as errors_count,
                sum(case when l.log_level = 2 then 1 else 0 end) as warnings_count
            from info.event_ingest_log l
            where l.log_level < 3
            group by l.game_id
        )
        select
            g.*,
            coalesce(counts.critical_count, 0) as critical_count,
            coalesce(counts.errors_count, 0) as errors_count,
            coalesce(counts.warnings_count, 0) as warnings_count
        from data.games g
            left join counts on g.id = counts.game_id
    ",
    )
}

pub fn games_list() -> SqlQuery {
    // Just get the query into a context where you can "and" on where
    games_list_base().sql("where 1=1")
}

pub fn games_with_issues_list() -> SqlQuery {
    games_list_base().sql(
        "where (counts.critical_count > 0 or counts.errors_count > 0 or counts.warnings_count > 0)",
    )
}

pub fn games_from_ingest_list(ingest_id: i64) -> SqlQuery {
    // TODO This is bad! This should be a prepared query! But with a
    //   prepared query I can't bind a value and then keep appending
    //   more sql. The TODO here is to figure out how to get rid of
    //   this format! without making the code way more complicated.
    games_list_base().sql(format!("where g.ingest = {ingest_id}"))
}

pub fn ingest_with_games(
    conn: &mut PgConnection,
    for_ingest_id: i64,
    page_size: usize,
    after_game_id: Option<&str>,
) -> QueryResult<(DbIngest, PageOfGames)> {
    use crate::info_schema::info::ingests::dsl as ingest_dsl;

    let ingest = ingest_dsl::ingests
        .filter(ingest_dsl::id.eq(for_ingest_id))
        .get_result::<DbIngest>(conn)?;

    let games = page_of_games_generic(
        conn,
        page_size,
        after_game_id,
        games_from_ingest_list(for_ingest_id),
    )?;

    Ok((ingest, games))
}

pub struct PageOfGames {
    pub games: Vec<GameWithIssueCounts>,
    pub next_page: Option<String>,
    // Nested option: The outer layer is whether there is a previous page. The inner
    // layer is whether that previous page is the first page, whose token is None
    pub previous_page: Option<Option<String>>,
}
pub fn page_of_games_generic(
    conn: &mut PgConnection,
    page_size: usize,
    after_game_id: Option<&str>,
    base_query: SqlQuery,
) -> QueryResult<PageOfGames> {
    // Get N + 1 games so we know if this is the last page or not
    let (mut games, previous_page) = if let Some(after_game_id) = after_game_id {
        // base_query must have left off in the middle of a `where`
        let games = base_query
            .clone()
            .sql(
                "
            and g.mmolb_game_id > $1
            order by g.mmolb_game_id asc
            limit $2
        ",
            )
            .bind::<Text, _>(after_game_id)
            .bind::<Integer, _>(page_size as i32 + 1)
            .get_results::<GameWithIssueCounts>(conn)?;

        // Previous page is the one page_size games before this
        // Get N + 1 games so we know if this is the first page or not
        let preceding_pages = base_query
            .sql(
                "
            and g.mmolb_game_id <= $1
            order by g.mmolb_game_id desc
            limit $2
        ",
            )
            .bind::<Text, _>(after_game_id)
            .bind::<Integer, _>(page_size as i32 + 1)
            .get_results::<GameWithIssueCounts>(conn)?;

        let preceding_page = if preceding_pages.len() > page_size {
            // Then the preceding page is not the first page
            Some(
                preceding_pages
                    .into_iter()
                    .last()
                    .map(|g| g.game.mmolb_game_id),
            )
        } else {
            // Then the preceding page is the first page
            Some(None)
        };

        (games, preceding_page)
    } else {
        let games = base_query
            .sql(
                "
            order by g.mmolb_game_id asc
            limit $1
        ",
            )
            .bind::<Integer, _>(page_size as i32 + 1)
            .get_results::<GameWithIssueCounts>(conn)?;

        // None after_game_id => this is the first page => there is no previous page
        (games, None)
    };

    let next_page = if games.len() > page_size {
        // Then this is not the last page
        games.truncate(page_size);
        // The page token is the last game that is actually shown
        games.last().map(|g| g.game.mmolb_game_id.clone())
    } else {
        // Then this is the last page
        None
    };

    Ok(PageOfGames {
        games,
        next_page,
        previous_page,
    })
}

pub fn page_of_games(
    conn: &mut PgConnection,
    page_size: usize,
    after_game_id: Option<&str>,
) -> QueryResult<PageOfGames> {
    page_of_games_generic(conn, page_size, after_game_id, games_list())
}

// This function names means "page of games that have issues", not "page of `GameWithIssues`s".
pub fn page_of_games_with_issues(
    conn: &mut PgConnection,
    page_size: usize,
    after_game_id: Option<&str>,
) -> QueryResult<PageOfGames> {
    page_of_games_generic(conn, page_size, after_game_id, games_with_issues_list())
}

pub struct EventsForGameTimings {
    pub get_game_ids_duration: f64,
    pub get_events_duration: f64,
    pub group_events_duration: f64,
    pub get_runners_duration: f64,
    pub group_runners_duration: f64,
    pub get_fielders_duration: f64,
    pub group_fielders_duration: f64,
    pub post_process_duration: f64,
}

pub fn events_for_games(
    conn: &mut PgConnection,
    taxa: &Taxa,
    for_game_ids: &[&str],
) -> QueryResult<(
    Vec<(i64, Vec<Result<EventDetail<String>, RowToEventError>>)>,
    EventsForGameTimings,
)> {
    use crate::data_schema::data::event_baserunners::dsl as runner_dsl;
    use crate::data_schema::data::event_fielders::dsl as fielder_dsl;
    use crate::data_schema::data::events::dsl as events_dsl;
    use crate::data_schema::data::games::dsl as games_dsl;

    let get_game_ids_start = Utc::now();
    let game_ids = games_dsl::games
        .filter(games_dsl::mmolb_game_id.eq_any(for_game_ids))
        .select(games_dsl::id)
        .order_by(games_dsl::id.asc())
        .get_results::<i64>(conn)?;
    let get_game_ids_duration = (Utc::now() - get_game_ids_start).as_seconds_f64();

    let get_events_start = Utc::now();
    let db_events = events_dsl::events
        .filter(events_dsl::game_id.eq_any(&game_ids))
        .order_by(events_dsl::game_id.asc())
        .then_order_by(events_dsl::game_event_index.asc())
        .select(DbEvent::as_select())
        .load(conn)?;
    let all_event_ids = db_events.iter().map(|event| event.id).collect_vec();
    let get_events_duration = (Utc::now() - get_events_start).as_seconds_f64();

    let group_events_start = Utc::now();
    let mut db_events_iter = db_events.into_iter().peekable();
    let db_games_events = game_ids
        .iter()
        .map(|id| {
            let mut game_events = Vec::new();
            while let Some(event) = db_events_iter.next_if(|e| e.game_id == *id) {
                game_events.push(event);
            }
            game_events
        })
        .collect_vec();
    let group_events_duration = (Utc::now() - group_events_start).as_seconds_f64();

    let get_runners_start = Utc::now();
    let mut db_runners_iter = runner_dsl::event_baserunners
        .filter(runner_dsl::event_id.eq_any(&all_event_ids))
        .order_by((
            runner_dsl::event_id.asc(),
            runner_dsl::base_before.desc().nulls_last(),
        ))
        .select(DbRunner::as_select())
        .load(conn)?
        .into_iter()
        .peekable();
    let get_runners_duration = (Utc::now() - get_runners_start).as_seconds_f64();

    let group_runners_start = Utc::now();
    let db_runners = db_games_events
        .iter()
        .map(|game_events| {
            game_events
                .iter()
                .map(|game_event| {
                    let mut runners = Vec::new();
                    while let Some(runner) =
                        db_runners_iter.next_if(|f| f.event_id == game_event.id)
                    {
                        runners.push(runner);
                    }
                    runners
                })
                .collect_vec()
        })
        .collect_vec();
    assert_eq!(db_runners_iter.count(), 0);
    let group_runners_duration = (Utc::now() - group_runners_start).as_seconds_f64();

    let get_fielders_start = Utc::now();
    let mut db_fielders_iter = fielder_dsl::event_fielders
        .filter(fielder_dsl::event_id.eq_any(&all_event_ids))
        .order_by((fielder_dsl::event_id, fielder_dsl::play_order))
        .select(DbFielder::as_select())
        .load(conn)?
        .into_iter()
        .peekable();
    let get_fielders_duration = (Utc::now() - get_fielders_start).as_seconds_f64();

    let group_fielders_start = Utc::now();
    let db_fielders = db_games_events
        .iter()
        .map(|game_events| {
            game_events
                .iter()
                .map(|game_event| {
                    let mut fielders = Vec::new();
                    while let Some(fielder) =
                        db_fielders_iter.next_if(|f| f.event_id == game_event.id)
                    {
                        fielders.push(fielder);
                    }
                    fielders
                })
                .collect_vec()
        })
        .collect_vec();
    assert_eq!(db_fielders_iter.count(), 0);
    let group_fielders_duration = (Utc::now() - group_fielders_start).as_seconds_f64();

    let post_process_start = Utc::now();
    let result = itertools::izip!(game_ids, db_games_events, db_runners, db_fielders)
        .map(|(game_id, events, runners, fielders)| {
            // Note: This should stay a vec of results. The individual results for each
            // entry are semantically meaningful.
            let detail_events = itertools::izip!(events, runners, fielders)
                .map(|(event, runners, fielders)| {
                    to_db_format::row_to_event(taxa, event, runners, fielders)
                })
                .collect_vec();
            (game_id, detail_events)
        })
        .collect_vec();
    let post_process_duration = (Utc::now() - post_process_start).as_seconds_f64();

    Ok((
        result,
        EventsForGameTimings {
            get_game_ids_duration,
            get_events_duration,
            group_events_duration,
            get_runners_duration,
            group_runners_duration,
            get_fielders_duration,
            group_fielders_duration,
            post_process_duration,
        },
    ))
}

pub struct CompletedGameForDb<'g> {
    pub id: &'g str,
    pub raw_game: &'g mmolb_parsing::Game,
    pub events: Vec<EventDetail<&'g str>>,
    pub logs: Vec<Vec<IngestLog>>,
    // This is used for verifying the round trip
    pub parsed_game: Vec<ParsedEventMessage<&'g str>>,
    pub stadium_name: Option<&'g str>,
}

pub enum GameForDb<'g> {
    Ongoing {
        game_id: &'g str,
        from_version: DateTime<Utc>,
        raw_game: &'g mmolb_parsing::Game,
    },
    ForeverIncomplete {
        game_id: &'g str,
        from_version: DateTime<Utc>,
        raw_game: &'g mmolb_parsing::Game,
    },
    Completed {
        game: CompletedGameForDb<'g>,
        from_version: DateTime<Utc>,
    },
    FatalError {
        game_id: &'g str,
        from_version: DateTime<Utc>,
        raw_game: &'g mmolb_parsing::Game,
        error_message: String,
    },
}

impl<'g> GameForDb<'g> {
    pub fn raw(&self) -> (&'g str, DateTime<Utc>, &'g mmolb_parsing::Game) {
        match self {
            GameForDb::Ongoing { game_id, from_version, raw_game } => (*game_id, *from_version, raw_game),
            GameForDb::ForeverIncomplete { game_id, from_version, raw_game } => (*game_id, *from_version, raw_game),
            GameForDb::Completed { game, from_version } => (&game.id, *from_version, &game.raw_game),
            GameForDb::FatalError {
                game_id, from_version, raw_game, ..
            } => (*game_id, *from_version, raw_game),
        }
    }

    pub fn is_ongoing(&self) -> bool {
        match self {
            GameForDb::Ongoing { .. } => true,
            GameForDb::ForeverIncomplete { .. } => false,
            GameForDb::Completed { .. } => false,
            // We only produce a fatal error on a completed game.
            GameForDb::FatalError { .. } => false,
        }
    }
}

pub struct InsertGamesTimings {
    pub delete_old_games_duration: f64,
    pub update_weather_table_duration: f64,
    pub insert_games_duration: f64,
    pub insert_raw_events_duration: f64,
    pub insert_logs_duration: f64,
    pub insert_events_duration: f64,
    pub get_event_ids_duration: f64,
    pub insert_baserunners_duration: f64,
    pub insert_fielders_duration: f64,
}

pub fn insert_games(
    conn: &mut PgConnection,
    taxa: &Taxa,
    ingest_id: i64,
    games: &[GameForDb],
) -> QueryResult<InsertGamesTimings> {
    conn.transaction(|conn| insert_games_internal(conn, taxa, ingest_id, games))
}

fn insert_games_internal<'e>(
    conn: &mut PgConnection,
    taxa: &Taxa,
    ingest_id: i64,
    games: &[GameForDb],
) -> QueryResult<InsertGamesTimings> {
    use crate::data_schema::data::event_baserunners::dsl as baserunners_dsl;
    use crate::data_schema::data::event_fielders::dsl as fielders_dsl;
    use crate::data_schema::data::events::dsl as events_dsl;
    use crate::data_schema::data::games::dsl as games_dsl;
    use crate::info_schema::info::event_ingest_log::dsl as event_ingest_log_dsl;
    use crate::info_schema::info::raw_events::dsl as raw_events_dsl;

    // First delete all games. If particular debug settings are turned on this may happen for every
    // game, but even in release mode we may need to delete partial games and replace them with
    // full games.
    let delete_old_games_start = Utc::now();
    let game_mmolb_ids = games
        .iter()
        .map(GameForDb::raw)
        .map(|(id, _, _)| id)
        .collect_vec();

    diesel::delete(games_dsl::games)
        .filter(games_dsl::mmolb_game_id.eq_any(game_mmolb_ids))
        .execute(conn)?;
    let delete_old_games_duration = (Utc::now() - delete_old_games_start).as_seconds_f64();

    let update_weather_table_start = Utc::now();
    let weather_table = weather::create_weather_table(conn, games)?;
    let update_weather_table_duration = (Utc::now() - update_weather_table_start).as_seconds_f64();

    let insert_games_start = Utc::now();
    let new_games = games
        .iter()
        .map(|game| {
            let (game_id, from_version, raw_game) = game.raw();
            let Some(weather_id) = weather_table.get(&(
                raw_game.weather.name.as_str(),
                raw_game.weather.emoji.as_str(),
                raw_game.weather.tooltip.as_str(),
            )) else {
                panic!(
                    "Weather was not found in weather_table. This is a bug: preceding code should \
                    have populated weather_table with any new weathers in this batch of games.",
                );
            };

            let (day, superstar_day) = match &raw_game.day {
                Ok(Day::SuperstarBreak) => {
                    // TODO Convert this to a gamewide ingest log warning
                    warn!("A game happened on a non-numbered Superstar Break day.");
                    (None, None)
                }
                Ok(Day::Holiday) => {
                    // TODO Convert this to a gamewide ingest log warning
                    warn!("A game happened on a Holiday.");
                    (None, None)
                }
                Ok(Day::Day(day)) => (Some(*day), None),
                Ok(Day::SuperstarDay(day)) => (None, Some(*day)),
                Ok(Day::Election) => {
                    // TODO Convert this to a gamewide ingest log warning
                    warn!("A game happened on a Election.");
                    (None, None)
                },
                Ok(Day::PostseasonPreview) => {
                    // TODO Convert this to a gamewide ingest log warning
                    warn!("A game happened on Postseason Preview.");
                    (None, None)
                },
                Ok(Day::Preseason) => {
                    // TODO Convert this to a gamewide ingest log warning
                    warn!("A game happened on a Preseason.");
                    (None, None)
                },
                Ok(Day::PostseasonRound(_)) => {
                    // TODO Convert this to a gamewide ingest log warning
                    warn!("A game happened on a Postseason Round (so far this type of day only shows up on player's birthdays).");
                    (None, None)
                },
                Err(error) => {
                    // TODO Convert this to a gamewide ingest log error
                    warn!("Day was not recognized: {error}");
                    (None, None)
                }
            };

            let (away_team_final_score, home_team_final_score) =
                if let GameForDb::Completed { game: complete_game, .. } = game {
                    complete_game
                        .events
                        .last()
                        .map(|event| {
                            (
                                Some(event.away_team_score_after as i32),
                                Some(event.home_team_score_after as i32),
                            )
                        })
                        .unwrap_or((None, None))
                } else {
                    (None, None)
                };

            let stadium_name = match game {
                GameForDb::Completed { game, .. } => { game.stadium_name }
                _ => { None }
            };

            NewGame {
                ingest: ingest_id,
                mmolb_game_id: game_id,
                season: raw_game.season as i32,
                day: day.map(Into::into),
                superstar_day: superstar_day.map(Into::into),
                weather: *weather_id,
                away_team_emoji: &raw_game.away_team_emoji,
                away_team_name: &raw_game.away_team_name,
                away_team_mmolb_id: &raw_game.away_team_id,
                away_team_final_score,
                home_team_emoji: &raw_game.home_team_emoji,
                home_team_name: &raw_game.home_team_name,
                home_team_mmolb_id: &raw_game.home_team_id,
                home_team_final_score,
                is_ongoing: game.is_ongoing(),
                stadium_name,
                from_version: from_version.naive_utc(),
            }
        })
        .collect_vec();

    let n_games_to_insert = new_games.len();
    let game_ids = diesel::insert_into(games_dsl::games)
        .values(&new_games)
        .returning(games_dsl::id)
        .get_results::<i64>(conn)?;

    log_only_assert!(
        n_games_to_insert == game_ids.len(),
        "Games insert should have inserted {} rows, but it inserted {}",
        n_games_to_insert,
        game_ids.len(),
    );

    // From now on, we don't need unfinished games
    let (completed_games, game_wide_logs): (Vec<_>, Vec<_>) = iter::zip(&game_ids, games)
        .flat_map(|(game_id, game)| match game {
            GameForDb::Ongoing { .. } => None,
            GameForDb::ForeverIncomplete { .. } => {
                Some(Either::Right(NewEventIngestLog {
                    game_id: *game_id,
                    game_event_index: None, // None => applies to the entire game
                    log_index: 0,           // there's only ever one
                    log_level: 3,           // info
                    log_text: "This is a bugged terminally-incomplete game. It will never be ingested.",
                }))
            },
            GameForDb::Completed { game, .. } => Some(Either::Left((*game_id, game))),
            GameForDb::FatalError { error_message, .. } => {
                Some(Either::Right(NewEventIngestLog {
                    game_id: *game_id,
                    game_event_index: None, // None => applies to the entire game
                    log_index: 0,           // there's only ever one
                    log_level: 0,           // critical
                    log_text: error_message,
                }))
            }
        })
        .partition_map(|x| x);

    let insert_games_duration = (Utc::now() - insert_games_start).as_seconds_f64();

    let insert_raw_events_start = Utc::now();
    let new_raw_events = completed_games
        .iter()
        .flat_map(|(game_id, game)| {
            game.raw_game
                .event_log
                .iter()
                .enumerate()
                .map(|(index, raw_event)| NewRawEvent {
                    game_id: *game_id,
                    game_event_index: index as i32,
                    event_text: &raw_event.message,
                })
        })
        .collect::<Vec<_>>();

    let n_raw_events_to_insert = new_raw_events.len();
    let n_raw_events_inserted = diesel::copy_from(raw_events_dsl::raw_events)
        .from_insertable(&new_raw_events)
        .execute(conn)?;

    log_only_assert!(
        n_raw_events_to_insert == n_raw_events_inserted,
        "Raw events insert should have inserted {} rows, but it inserted {}",
        n_raw_events_to_insert,
        n_raw_events_inserted,
    );
    let insert_raw_events_duration = (Utc::now() - insert_raw_events_start).as_seconds_f64();

    let insert_logs_start = Utc::now();
    let new_logs = completed_games
        .iter()
        .flat_map(|(game_id, game)| {
            game.logs
                .iter()
                .enumerate()
                .flat_map(move |(game_event_index, logs)| {
                    logs.iter().enumerate().map(move |(log_index, log)| {
                        assert_eq!(game_event_index as i32, log.game_event_index);
                        NewEventIngestLog {
                            game_id: *game_id,
                            game_event_index: Some(log.game_event_index),
                            log_index: log_index as i32,
                            log_level: log.log_level,
                            log_text: &log.log_text,
                        }
                    })
                })
        })
        .chain(game_wide_logs)
        .collect_vec();

    let n_logs_to_insert = new_logs.len();
    let n_logs_inserted = diesel::copy_from(event_ingest_log_dsl::event_ingest_log)
        .from_insertable(&new_logs)
        .execute(conn)?;

    log_only_assert!(
        n_logs_to_insert == n_logs_inserted,
        "Event ingest logs insert should have inserted {} rows, but it inserted {}",
        n_logs_to_insert,
        n_logs_inserted,
    );
    let insert_logs_duration = (Utc::now() - insert_logs_start).as_seconds_f64();

    let insert_events_start = Utc::now();
    let new_events: Vec<_> = completed_games
        .iter()
        .flat_map(|(game_id, game)| {
            game.events
                .iter()
                .map(|event| to_db_format::event_to_row(taxa, *game_id, event))
        })
        .collect();

    let n_events_to_insert = new_events.len();
    let n_events_inserted = diesel::copy_from(events_dsl::events)
        .from_insertable(&new_events)
        .execute(conn)?;

    log_only_assert!(
        n_events_to_insert == n_events_inserted,
        "Events insert should have inserted {} rows, but it inserted {}",
        n_events_to_insert,
        n_events_inserted,
    );
    let insert_events_duration = (Utc::now() - insert_events_start).as_seconds_f64();

    let get_event_ids_start = Utc::now();
    // Postgres' copy doesn't support returning ids, but we need them, so we query them from scratch
    let event_ids = events_dsl::events
        .filter(events_dsl::game_id.eq_any(&game_ids))
        .select((events_dsl::game_id, events_dsl::id))
        .order_by(events_dsl::game_id)
        .then_order_by(events_dsl::game_event_index)
        .get_results::<(i64, i64)>(conn)?;

    let event_ids_by_game = event_ids
        .into_iter()
        .chunk_by(|(game_id, _)| *game_id)
        .into_iter()
        .map(|(game_id, group)| (game_id, group.map(|(_, event_id)| event_id).collect_vec()))
        .collect_vec();
    let get_event_ids_duration = (Utc::now() - get_event_ids_start).as_seconds_f64();

    let insert_baserunners_start = Utc::now();
    let new_baserunners = iter::zip(&event_ids_by_game, &completed_games)
        .flat_map(|((game_id_a, event_ids), (game_id_b, game))| {
            assert_eq!(game_id_a, game_id_b);
            // Within this closure we're acting on all events in a single game
            iter::zip(event_ids, &game.events).flat_map(|(event_id, event)| {
                // Within this closure we're acting on a single event
                to_db_format::event_to_baserunners(taxa, *event_id, event)
            })
        })
        .collect_vec();

    let n_baserunners_to_insert = new_baserunners.len();
    let n_baserunners_inserted = diesel::copy_from(baserunners_dsl::event_baserunners)
        .from_insertable(&new_baserunners)
        .execute(conn)?;

    log_only_assert!(
        n_baserunners_to_insert == n_baserunners_inserted,
        "Event baserunners insert should have inserted {} rows, but it inserted {}",
        n_baserunners_to_insert,
        n_baserunners_inserted,
    );
    let insert_baserunners_duration = (Utc::now() - insert_baserunners_start).as_seconds_f64();

    let insert_fielders_start = Utc::now();
    let new_fielders = iter::zip(&event_ids_by_game, &completed_games)
        .flat_map(|((game_id_a, event_ids), (game_id_b, game))| {
            assert_eq!(game_id_a, game_id_b);
            // Within this closure we're acting on all events in a single game
            iter::zip(event_ids, &game.events).flat_map(|(event_id, event)| {
                // Within this closure we're acting on a single event
                to_db_format::event_to_fielders(taxa, *event_id, event)
            })
        })
        .collect_vec();

    let n_fielders_to_insert = new_fielders.len();
    let n_fielders_inserted = diesel::copy_from(fielders_dsl::event_fielders)
        .from_insertable(&new_fielders)
        .execute(conn)?;

    log_only_assert!(
        n_fielders_to_insert == n_fielders_inserted,
        "Event fielders insert should have inserted {} rows, but it inserted {}",
        n_fielders_to_insert,
        n_fielders_inserted,
    );
    let insert_fielders_duration = (Utc::now() - insert_fielders_start).as_seconds_f64();

    Ok(InsertGamesTimings {
        delete_old_games_duration,
        update_weather_table_duration,
        insert_games_duration,
        insert_raw_events_duration,
        insert_logs_duration,
        insert_events_duration,
        get_event_ids_duration,
        insert_baserunners_duration,
        insert_fielders_duration,
    })
}

pub fn insert_additional_ingest_logs(
    conn: &mut PgConnection,
    extra_ingest_logs: &[(i64, Vec<IngestLog>)],
) -> QueryResult<()> {
    use crate::info_schema::info::event_ingest_log::dsl as event_ingest_log_dsl;

    let game_ids = extra_ingest_logs
        .iter()
        .map(|(game_id, _)| game_id)
        .collect_vec();

    // Get the highest log_index for each event
    // TODO Only select the game event indices we care about
    let mut highest_log_indices: HashMap<_, _> = event_ingest_log_dsl::event_ingest_log
        .group_by((
            event_ingest_log_dsl::game_id,
            event_ingest_log_dsl::game_event_index,
        ))
        .select((
            event_ingest_log_dsl::game_id,
            event_ingest_log_dsl::game_event_index,
            diesel::dsl::max(event_ingest_log_dsl::log_index),
        ))
        .filter(event_ingest_log_dsl::game_id.eq_any(&game_ids))
        .order_by(event_ingest_log_dsl::game_id.asc())
        .then_order_by(event_ingest_log_dsl::game_event_index.asc())
        .get_results::<(i64, Option<i32>, Option<i32>)>(conn)?
        .into_iter()
        .filter_map(|(game_id, game_event_index, highest_log_order)| {
            highest_log_order.map(|n| ((game_id, game_event_index), n))
        })
        .collect();

    let new_logs = extra_ingest_logs
        .into_iter()
        .flat_map(|(game_id, ingest_logs)| {
            ingest_logs
                .iter()
                .map(|ingest_log| {
                    let log_index = highest_log_indices
                        .entry((*game_id, Some(ingest_log.game_event_index)))
                        .or_default();
                    *log_index += 1;

                    NewEventIngestLog {
                        game_id: *game_id,
                        game_event_index: Some(ingest_log.game_event_index),
                        log_index: *log_index,
                        log_level: ingest_log.log_level,
                        log_text: &ingest_log.log_text,
                    }
                })
                // The intermediate vec is for lifetime reasons
                .collect_vec()
        })
        .collect_vec();

    diesel::copy_from(event_ingest_log_dsl::event_ingest_log)
        .from_insertable(new_logs)
        .execute(conn)?;

    Ok(())
}

pub struct DbFullGameWithLogs {
    pub game: DbGame,
    pub game_wide_logs: Vec<DbEventIngestLog>,
    pub raw_events_with_logs: Vec<(DbRawEvent, Vec<DbEventIngestLog>)>,
}

pub fn game_and_raw_events(
    conn: &mut PgConnection,
    mmolb_game_id: &str,
) -> QueryResult<DbFullGameWithLogs> {
    use crate::data_schema::data::games::dsl as games_dsl;
    use crate::info_schema::info::event_ingest_log::dsl as event_ingest_log_dsl;
    use crate::info_schema::info::raw_events::dsl as raw_events_dsl;

    let game = games_dsl::games
        .filter(games_dsl::mmolb_game_id.eq(mmolb_game_id))
        .select(DbGame::as_select())
        .get_result::<DbGame>(conn)?;

    let raw_events = DbRawEvent::belonging_to(&game)
        .order_by(raw_events_dsl::game_event_index.asc())
        .load::<DbRawEvent>(conn)?;

    // This would be another belonging_to but diesel doesn't seem to support
    // compound foreign keys in associations
    let mut raw_logs = event_ingest_log_dsl::event_ingest_log
        .filter(event_ingest_log_dsl::game_id.eq(game.id))
        .order_by(event_ingest_log_dsl::game_event_index.asc().nulls_first())
        .then_order_by(event_ingest_log_dsl::log_index.asc())
        .get_results::<DbEventIngestLog>(conn)?
        .into_iter()
        .peekable();

    let mut game_wide_logs = Vec::new();
    while let Some(event) = raw_logs.next_if(|log| log.game_event_index.is_none()) {
        game_wide_logs.push(event);
    }

    let logs_by_event = raw_events
        .iter()
        .map(|raw_event| {
            let mut events = Vec::new();
            while let Some(event) =
                raw_logs.next_if(|log| log.game_event_index.expect("All logs with a None game_event_index should have been extracted before this loop began") == raw_event.game_event_index)
            {
                events.push(event);
            }
            events
        })
        .collect_vec();

    assert!(raw_logs.next().is_none(), "Failed to map all raw logs");

    let raw_events_with_logs = raw_events
        .into_iter()
        .zip(logs_by_event)
        .collect::<Vec<_>>();

    Ok(DbFullGameWithLogs {
        game,
        game_wide_logs,
        raw_events_with_logs,
    })
}

pub struct Timings {
    pub get_batch_to_process_duration: f64,
    pub deserialize_games_duration: f64,
    pub filter_finished_games_duration: f64,
    pub parse_and_sim_duration: f64,
    pub db_insert_duration: f64,
    pub db_insert_timings: InsertGamesTimings,
    pub db_fetch_for_check_duration: f64,
    pub events_for_game_timings: EventsForGameTimings,
    pub check_round_trip_duration: f64,
    pub insert_extra_logs_duration: f64,
    pub save_duration: f64,
}

pub fn insert_timings(
    conn: &mut PgConnection,
    ingest_id: i64,
    index: usize,
    timings: Timings,
) -> QueryResult<()> {
    NewGameIngestTimings {
        ingest_id,
        index: index as i32,
        get_batch_to_process_duration: timings.get_batch_to_process_duration,
        deserialize_games_duration: timings.deserialize_games_duration,
        filter_finished_games_duration: timings.filter_finished_games_duration,
        parse_and_sim_duration: timings.parse_and_sim_duration,
        db_fetch_for_check_get_game_id_duration: timings
            .events_for_game_timings
            .get_game_ids_duration,
        db_fetch_for_check_get_events_duration: timings.events_for_game_timings.get_events_duration,
        db_fetch_for_check_group_events_duration: timings
            .events_for_game_timings
            .group_events_duration,
        db_fetch_for_check_get_runners_duration: timings
            .events_for_game_timings
            .get_runners_duration,
        db_fetch_for_check_group_runners_duration: timings
            .events_for_game_timings
            .group_runners_duration,
        db_fetch_for_check_get_fielders_duration: timings
            .events_for_game_timings
            .get_fielders_duration,
        db_fetch_for_check_group_fielders_duration: timings
            .events_for_game_timings
            .group_fielders_duration,
        db_fetch_for_check_post_process_duration: timings
            .events_for_game_timings
            .post_process_duration,
        db_insert_duration: timings.db_insert_duration,
        db_insert_delete_old_games_duration: timings.db_insert_timings.delete_old_games_duration,
        db_insert_update_weather_table_duration: timings
            .db_insert_timings
            .update_weather_table_duration,
        db_insert_insert_games_duration: timings.db_insert_timings.insert_games_duration,
        db_insert_insert_raw_events_duration: timings.db_insert_timings.insert_raw_events_duration,
        db_insert_insert_logs_duration: timings.db_insert_timings.insert_logs_duration,
        db_insert_insert_events_duration: timings.db_insert_timings.insert_events_duration,
        db_insert_get_event_ids_duration: timings.db_insert_timings.get_event_ids_duration,
        db_insert_insert_baserunners_duration: timings
            .db_insert_timings
            .insert_baserunners_duration,
        db_insert_insert_fielders_duration: timings.db_insert_timings.insert_fielders_duration,
        db_fetch_for_check_duration: timings.db_fetch_for_check_duration,
        check_round_trip_duration: timings.check_round_trip_duration,
        insert_extra_logs_duration: timings.insert_extra_logs_duration,
        save_duration: timings.save_duration,
    }
    .insert_into(crate::info_schema::info::ingest_timings::dsl::ingest_timings)
    .execute(conn)
    .map(|_| ())
}

#[derive(Debug, Error)]
pub enum DbMetaQueryError {
    #[error(transparent)]
    Db(#[from] diesel::result::Error),

    #[error("Table is missing required field {0}")]
    TableMissingField(&'static str),

    #[error("Column is missing required field {0}")]
    ColumnMissingField(&'static str),

    #[error(
        "Unexpected value {actual_value} in field {field}. Expected one of {expected_values:?}"
    )]
    UnexpectedValueInField {
        field: &'static str,
        actual_value: String,
        expected_values: &'static [&'static str],
    },
}

#[derive(Debug, Serialize)]
pub struct DbColumn {
    pub name: String,
    pub r#type: String,
    pub is_nullable: bool,
}

#[derive(Debug, Serialize)]
pub struct DbTable {
    pub name: String,
    pub columns: Vec<DbColumn>,
}

pub fn tables_for_schema(
    conn: &mut PgConnection,
    catalog_name: &str,
    schema_name: &str,
) -> Result<Vec<DbTable>, DbMetaQueryError> {
    use crate::meta_schema::meta::columns::dsl as columns_dsl;
    use crate::meta_schema::meta::tables::dsl as tables_dsl;

    let raw_tables = tables_dsl::tables
        .filter(
            tables_dsl::table_catalog
                .eq(catalog_name)
                .and(tables_dsl::table_schema.eq(schema_name)),
        )
        .order_by((
            tables_dsl::table_catalog.asc(),
            tables_dsl::table_schema.asc(),
            tables_dsl::table_name.asc(),
        ))
        .select(RawDbTable::as_select())
        .get_results(conn)?;

    let raw_columns = columns_dsl::columns
        .filter(
            columns_dsl::table_catalog
                .eq(catalog_name)
                .and(columns_dsl::table_schema.eq(schema_name)),
        )
        .order_by((
            columns_dsl::table_catalog.asc(),
            columns_dsl::table_schema.asc(),
            columns_dsl::table_name.asc(),
            columns_dsl::ordinal_position.asc(),
        ))
        .select(RawDbColumn::as_select())
        .get_results(conn)?;

    let raw_columns_grouped = raw_columns.into_iter().chunk_by(|col| {
        (
            col.table_catalog.clone(),
            col.table_schema.clone(),
            col.table_name.clone(),
        )
    });

    iter::zip(raw_tables, raw_columns_grouped.into_iter())
        .map(|(table, (table_key, columns))| {
            // Gotta unwrap the option to convert to tuple of references
            let (table_key_catalog, table_key_schema, table_key_name) = table_key;
            assert_eq!(
                (&table_key_catalog, &table_key_schema, &table_key_name),
                (&table.table_catalog, &table.table_schema, &table.table_name),
            );

            Ok(DbTable {
                name: table
                    .table_name
                    .ok_or(DbMetaQueryError::TableMissingField("table_name"))?,
                columns: columns
                    .map(|column| {
                        Ok(DbColumn {
                            name: column
                                .column_name
                                .ok_or(DbMetaQueryError::ColumnMissingField("column_name"))?,
                            r#type: column
                                .data_type
                                .ok_or(DbMetaQueryError::ColumnMissingField("data_type"))?,
                            is_nullable: match column.column_is_nullable.as_deref() {
                                // Note that I renamed it to column_is_nullable for diesel to avoid a
                                // name conflict. The sql name for it is just is_nullable.
                                None => {
                                    return Err(DbMetaQueryError::ColumnMissingField("is_nullable"));
                                }
                                Some("YES") => true,
                                Some("NO") => false,
                                Some(other) => {
                                    return Err(DbMetaQueryError::UnexpectedValueInField {
                                        field: "is_nullable",
                                        actual_value: other.to_owned(),
                                        expected_values: &["YES", "NO"],
                                    });
                                }
                            },
                        })
                    })
                    .collect::<Result<Vec<_>, DbMetaQueryError>>()?,
            })
        })
        .collect()
}
