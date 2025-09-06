mod entities;
mod to_db_format;
mod versions;
mod weather;

// Reexports
pub use crate::db::weather::NameEmojiTooltip;
pub use entities::*;
pub use to_db_format::RowToEventError;
pub use versions::*;

// Third-party imports
use chrono::{DateTime, NaiveDateTime, Utc};
use diesel::dsl::{count, count_distinct, count_star, max, min};
use diesel::query_builder::SqlQuery;
use diesel::{PgConnection, prelude::*, sql_query, sql_types::*};
use hashbrown::HashMap;
use itertools::{Either, Itertools};
use log::{info, warn};
use mmolb_parsing::ParsedEventMessage;
use mmolb_parsing::enums::Day;
use serde::Serialize;
use std::iter;
use thiserror::Error;
// First-party imports
use crate::event_detail::{EventDetail, IngestLog};
use crate::models::{DbAuroraPhoto, DbDoorPrize, DbDoorPrizeItem, DbEjection, DbEvent, DbEventIngestLog, DbFielder, DbGame, DbIngest, DbModification, DbPlayerEquipmentEffectVersion, DbPlayerEquipmentVersion, DbPlayerModificationVersion, DbPlayerVersion, DbRawEvent, DbRunner, NewEventIngestLog, NewGame, NewTeamGamePlayed, NewGameIngestTimings, NewIngest, NewIngestCount, NewModification, NewPlayerAttributeAugment, NewPlayerEquipmentEffectVersion, NewPlayerEquipmentVersion, NewPlayerFeedVersion, NewPlayerModificationVersion, NewPlayerParadigmShift, NewPlayerRecomposition, NewPlayerReportAttributeVersion, NewPlayerReportVersion, NewPlayerVersion, NewRawEvent, NewTeamFeedVersion, NewTeamPlayerVersion, NewTeamVersion, NewVersionIngestLog, RawDbColumn, RawDbTable};
use crate::taxa::Taxa;
use crate::{PartyEvent, PitcherChange, QueryError};

pub fn set_current_user_statement_timeout(
    conn: &mut PgConnection,
    timeout_seconds: i64,
) -> QueryResult<usize> {
    // Note that `alter role` cannot use a prepared query. The only way to
    // parameterize it is to build it from a string. `timeout_seconds` is
    // an i64 and thus its format cannot have a `'` character, so this should
    // be safe.
    sql_query(format!(
        "alter role CURRENT_USER set statement_timeout='{}s'",
        timeout_seconds
    ))
    .execute(conn)
}

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

pub fn get_game_ingest_start_cursor(
    conn: &mut PgConnection,
) -> QueryResult<Option<(NaiveDateTime, String)>> {
    use crate::schema::data_schema::data::games::dsl as games_dsl;

    games_dsl::games
        .select((games_dsl::from_version, games_dsl::mmolb_game_id))
        .order_by((
            games_dsl::from_version.desc(),
            games_dsl::mmolb_game_id.desc(),
        ))
        .limit(1)
        .get_result(conn)
        .optional()
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

pub fn group_child_table_results<'a, ChildT>(
    games_events: impl IntoIterator<Item = &'a Vec<DbEvent>>,
    child_results: Vec<ChildT>,
    event_id_for_child: impl Fn(&ChildT) -> i64,
) -> Vec<Vec<Vec<ChildT>>> {
    let mut child_results_iter = child_results.into_iter().peekable();

    let results = games_events
        .into_iter()
        .map(|game_events| {
            game_events
                .iter()
                .map(|game_event| {
                    let mut children = Vec::new();
                    while let Some(child) =
                        child_results_iter.next_if(|f| event_id_for_child(f) == game_event.id)
                    {
                        children.push(child);
                    }
                    children
                })
                .collect_vec()
        })
        .collect_vec();

    assert_eq!(child_results_iter.count(), 0);

    results
}

pub fn events_for_games(
    conn: &mut PgConnection,
    taxa: &Taxa,
    for_game_ids: &[&str],
) -> QueryResult<(
    Vec<(i64, Vec<Result<EventDetail<String>, RowToEventError>>)>,
    EventsForGameTimings,
)> {
    use crate::data_schema::data::aurora_photos::dsl as aurora_photo_dsl;
    use crate::data_schema::data::door_prize_items::dsl as door_prize_item_dsl;
    use crate::data_schema::data::door_prizes::dsl as door_prize_dsl;
    use crate::data_schema::data::ejections::dsl as ejection_dsl;
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
    let db_runners = runner_dsl::event_baserunners
        .filter(runner_dsl::event_id.eq_any(&all_event_ids))
        .order_by((
            runner_dsl::event_id.asc(),
            runner_dsl::base_before.desc().nulls_last(),
        ))
        .select(DbRunner::as_select())
        .load(conn)?;
    let get_runners_duration = (Utc::now() - get_runners_start).as_seconds_f64();

    let group_runners_start = Utc::now();
    let db_runners = group_child_table_results(&db_games_events, db_runners, |r| r.event_id);
    let group_runners_duration = (Utc::now() - group_runners_start).as_seconds_f64();

    let get_fielders_start = Utc::now();
    let db_fielders = fielder_dsl::event_fielders
        .filter(fielder_dsl::event_id.eq_any(&all_event_ids))
        .order_by((fielder_dsl::event_id, fielder_dsl::play_order))
        .select(DbFielder::as_select())
        .load(conn)?;
    let get_fielders_duration = (Utc::now() - get_fielders_start).as_seconds_f64();

    let group_fielders_start = Utc::now();
    let db_fielders = group_child_table_results(&db_games_events, db_fielders, |r| r.event_id);
    let group_fielders_duration = (Utc::now() - group_fielders_start).as_seconds_f64();

    let get_aurora_photos_start = Utc::now();
    let db_aurora_photos = aurora_photo_dsl::aurora_photos
        .filter(aurora_photo_dsl::event_id.eq_any(&all_event_ids))
        .order_by((
            aurora_photo_dsl::event_id,
            aurora_photo_dsl::is_listed_first.desc(),
        ))
        .select(DbAuroraPhoto::as_select())
        .load(conn)?;
    let _get_aurora_photos_duration = (Utc::now() - get_aurora_photos_start).as_seconds_f64();

    let group_aurora_photos_start = Utc::now();
    let db_aurora_photos =
        group_child_table_results(&db_games_events, db_aurora_photos, |r| r.event_id);
    let _group_aurora_photos_duration = (Utc::now() - group_aurora_photos_start).as_seconds_f64();

    let get_ejections_start = Utc::now();
    let db_ejections = ejection_dsl::ejections
        .filter(ejection_dsl::event_id.eq_any(&all_event_ids))
        .order_by(ejection_dsl::event_id)
        .select(DbEjection::as_select())
        .load(conn)?;
    let _get_ejections_duration = (Utc::now() - get_ejections_start).as_seconds_f64();

    let group_ejections_start = Utc::now();
    let db_ejections = group_child_table_results(&db_games_events, db_ejections, |r| r.event_id);
    let _group_ejections_duration = (Utc::now() - group_ejections_start).as_seconds_f64();

    let get_door_prizes_start = Utc::now();
    let db_door_prizes = door_prize_dsl::door_prizes
        .filter(door_prize_dsl::event_id.eq_any(&all_event_ids))
        .order_by((door_prize_dsl::event_id, door_prize_dsl::door_prize_index))
        .select(DbDoorPrize::as_select())
        .load(conn)?;
    let _get_door_prizes_duration = (Utc::now() - get_door_prizes_start).as_seconds_f64();

    let group_door_prizes_start = Utc::now();
    let db_door_prizes =
        group_child_table_results(&db_games_events, db_door_prizes, |r| r.event_id);
    let _group_door_prizes_duration = (Utc::now() - group_door_prizes_start).as_seconds_f64();

    let get_door_prize_items_start = Utc::now();
    let db_door_prize_items = door_prize_item_dsl::door_prize_items
        .filter(door_prize_item_dsl::event_id.eq_any(&all_event_ids))
        .order_by((
            door_prize_item_dsl::event_id,
            door_prize_item_dsl::door_prize_index,
            door_prize_item_dsl::item_index,
        ))
        .select(DbDoorPrizeItem::as_select())
        .load(conn)?;
    let _get_door_prize_items_duration = (Utc::now() - get_door_prize_items_start).as_seconds_f64();

    let group_door_prize_items_start = Utc::now();
    let db_door_prize_items =
        group_child_table_results(&db_games_events, db_door_prize_items, |r| r.event_id);
    let _group_door_prize_items_duration =
        (Utc::now() - group_door_prize_items_start).as_seconds_f64();

    let post_process_start = Utc::now();
    let result = itertools::izip!(
        game_ids,
        db_games_events,
        db_runners,
        db_fielders,
        db_aurora_photos,
        db_ejections,
        db_door_prizes,
        db_door_prize_items
    )
    .map(
        |(
            game_id,
            events,
            runners,
            fielders,
            aurora_photos,
            ejections,
            door_prizes,
            door_prize_items,
        )| {
            // Note: This should stay a vec of results. The individual results for each
            // entry are semantically meaningful.
            let detail_events = itertools::izip!(
                events,
                runners,
                fielders,
                aurora_photos,
                ejections,
                door_prizes,
                door_prize_items
            )
            .map(
                |(
                    event,
                    runners,
                    fielders,
                    aurora_photo,
                    ejection,
                    door_prizes,
                    door_prize_items,
                )| {
                    to_db_format::row_to_event(
                        taxa,
                        event,
                        runners,
                        fielders,
                        aurora_photo,
                        ejection,
                        door_prizes,
                        door_prize_items,
                    )
                },
            )
            .collect_vec();
            (game_id, detail_events)
        },
    )
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
    pub pitcher_changes: Vec<PitcherChange<&'g str>>,
    pub parties: Vec<PartyEvent<&'g str>>,
    pub logs: Vec<Vec<IngestLog>>,
    // This is used for verifying the round trip
    pub parsed_game: Vec<ParsedEventMessage<&'g str>>,
    pub stadium_name: Option<&'g str>,
    pub away_team_final_score: Option<i32>,
    pub home_team_final_score: Option<i32>,
    pub home_team_earned_coins: Option<i32>,
    pub away_team_earned_coins: Option<i32>,
    pub home_team_photo_contest_top_scorer: Option<&'g str>,
    pub home_team_photo_contest_score: Option<i32>,
    pub away_team_photo_contest_top_scorer: Option<&'g str>,
    pub away_team_photo_contest_score: Option<i32>,
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
    // e.g. the home run challenge
    NotSupported {
        game_id: &'g str,
        from_version: DateTime<Utc>,
        raw_game: &'g mmolb_parsing::Game,
        reason: String,
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
            GameForDb::Ongoing {
                game_id,
                from_version,
                raw_game,
            } => (*game_id, *from_version, raw_game),
            GameForDb::ForeverIncomplete {
                game_id,
                from_version,
                raw_game,
            } => (*game_id, *from_version, raw_game),
            GameForDb::Completed { game, from_version } => {
                (&game.id, *from_version, &game.raw_game)
            }
            GameForDb::NotSupported {
                game_id,
                from_version,
                raw_game,
                ..
            } => (*game_id, *from_version, raw_game),
            GameForDb::FatalError {
                game_id,
                from_version,
                raw_game,
                ..
            } => (*game_id, *from_version, raw_game),
        }
    }

    pub fn is_ongoing(&self) -> bool {
        match self {
            GameForDb::Ongoing { .. } => true,
            _ => false,
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
    use crate::data_schema::data::aurora_photos::dsl as aurora_photos_dsl;
    use crate::data_schema::data::door_prize_items::dsl as door_prize_items_dsl;
    use crate::data_schema::data::door_prizes::dsl as door_prizes_dsl;
    use crate::data_schema::data::ejections::dsl as ejections_dsl;
    use crate::data_schema::data::event_baserunners::dsl as baserunners_dsl;
    use crate::data_schema::data::event_fielders::dsl as fielders_dsl;
    use crate::data_schema::data::events::dsl as events_dsl;
    use crate::data_schema::data::games::dsl as games_dsl;
    use crate::data_schema::data::parties::dsl as parties_dsl;
    use crate::data_schema::data::pitcher_changes::dsl as pitcher_changes_dsl;
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
                Ok(Day::Day(day)) => (Some(*day), None),
                Ok(Day::SuperstarDay(day)) => (None, Some(*day)),
                Ok(other) => {
                    // TODO Convert this to a gamewide ingest log warning
                    warn!("A game happened on an unexpected type of day: {other}.");
                    (None, None)
                }
                Err(error) => {
                    // TODO Convert this to a gamewide ingest log error
                    warn!("Day was not recognized: {error}");
                    (None, None)
                }
            };

            match game {
                GameForDb::Completed {
                    game: completed_game,
                    ..
                } => NewGame {
                    ingest: ingest_id,
                    mmolb_game_id: game_id,
                    season: raw_game.season as i32,
                    day: day.map(Into::into),
                    superstar_day: superstar_day.map(Into::into),
                    weather: *weather_id,
                    away_team_emoji: &raw_game.away_team_emoji,
                    away_team_name: &raw_game.away_team_name,
                    away_team_mmolb_id: &raw_game.away_team_id,
                    away_team_final_score: completed_game.away_team_final_score,
                    home_team_emoji: &raw_game.home_team_emoji,
                    home_team_name: &raw_game.home_team_name,
                    home_team_mmolb_id: &raw_game.home_team_id,
                    home_team_final_score: completed_game.home_team_final_score,
                    is_ongoing: game.is_ongoing(),
                    stadium_name: completed_game.stadium_name,
                    from_version: from_version.naive_utc(),
                    home_team_earned_coins: completed_game.home_team_earned_coins,
                    away_team_earned_coins: completed_game.away_team_earned_coins,
                    home_team_photo_contest_top_scorer: completed_game
                        .home_team_photo_contest_top_scorer,
                    home_team_photo_contest_score: completed_game.home_team_photo_contest_score,
                    away_team_photo_contest_top_scorer: completed_game
                        .away_team_photo_contest_top_scorer,
                    away_team_photo_contest_score: completed_game.away_team_photo_contest_score,
                },
                _ => NewGame {
                    ingest: ingest_id,
                    mmolb_game_id: game_id,
                    season: raw_game.season as i32,
                    day: day.map(Into::into),
                    superstar_day: superstar_day.map(Into::into),
                    weather: *weather_id,
                    away_team_emoji: &raw_game.away_team_emoji,
                    away_team_name: &raw_game.away_team_name,
                    away_team_mmolb_id: &raw_game.away_team_id,
                    away_team_final_score: None,
                    home_team_emoji: &raw_game.home_team_emoji,
                    home_team_name: &raw_game.home_team_name,
                    home_team_mmolb_id: &raw_game.home_team_id,
                    home_team_final_score: None,
                    is_ongoing: game.is_ongoing(),
                    stadium_name: None,
                    from_version: from_version.naive_utc(),
                    home_team_earned_coins: None,
                    away_team_earned_coins: None,
                    home_team_photo_contest_top_scorer: None,
                    home_team_photo_contest_score: None,
                    away_team_photo_contest_top_scorer: None,
                    away_team_photo_contest_score: None,
                },
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
            GameForDb::NotSupported { reason, .. } => {
                Some(Either::Right(NewEventIngestLog {
                    game_id: *game_id,
                    game_event_index: None, // None => applies to the entire game
                    log_index: 0,           // there's only ever one
                    log_level: 3,           // info
                    log_text: reason,
                }))
            },
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

    let insert_aurora_photos_start = Utc::now();
    let new_aurora_photos = iter::zip(&event_ids_by_game, &completed_games)
        .flat_map(|((game_id_a, event_ids), (game_id_b, game))| {
            assert_eq!(game_id_a, game_id_b);
            // Within this closure we're acting on all events in a single game
            iter::zip(event_ids, &game.events).flat_map(|(event_id, event)| {
                // Within this closure we're acting on a single event
                to_db_format::event_to_aurora_photos(taxa, *event_id, event)
            })
        })
        .collect_vec();

    let n_aurora_photos_to_insert = new_aurora_photos.len();
    let n_aurora_photos_inserted = diesel::copy_from(aurora_photos_dsl::aurora_photos)
        .from_insertable(&new_aurora_photos)
        .execute(conn)?;

    log_only_assert!(
        n_aurora_photos_to_insert == n_aurora_photos_inserted,
        "Event aurora photos insert should have inserted {} rows, but it inserted {}",
        n_aurora_photos_to_insert,
        n_aurora_photos_inserted,
    );
    let _insert_aurora_photos_duration = (Utc::now() - insert_aurora_photos_start).as_seconds_f64();

    let insert_ejections_start = Utc::now();
    let new_ejections = iter::zip(&event_ids_by_game, &completed_games)
        .flat_map(|((game_id_a, event_ids), (game_id_b, game))| {
            assert_eq!(game_id_a, game_id_b);
            // Within this closure we're acting on all events in a single game
            iter::zip(event_ids, &game.events).flat_map(|(event_id, event)| {
                // Within this closure we're acting on a single event
                to_db_format::event_to_ejection(taxa, *event_id, event)
            })
        })
        .collect_vec();

    let n_ejections_to_insert = new_ejections.len();
    let n_ejections_inserted = diesel::copy_from(ejections_dsl::ejections)
        .from_insertable(&new_ejections)
        .execute(conn)?;

    log_only_assert!(
        n_ejections_to_insert == n_ejections_inserted,
        "Event ejections insert should have inserted {} rows, but it inserted {}",
        n_ejections_to_insert,
        n_ejections_inserted,
    );
    let _insert_ejections_duration = (Utc::now() - insert_ejections_start).as_seconds_f64();

    let insert_door_prizes_start = Utc::now();
    let new_door_prizes = iter::zip(&event_ids_by_game, &completed_games)
        .flat_map(|((game_id_a, event_ids), (game_id_b, game))| {
            assert_eq!(game_id_a, game_id_b);
            // Within this closure we're acting on all events in a single game
            iter::zip(event_ids, &game.events).flat_map(|(event_id, event)| {
                // Within this closure we're acting on a single event
                to_db_format::event_to_door_prize(*event_id, event)
            })
        })
        .collect_vec();

    let n_door_prizes_to_insert = new_door_prizes.len();
    let n_door_prizes_inserted = diesel::copy_from(door_prizes_dsl::door_prizes)
        .from_insertable(&new_door_prizes)
        .execute(conn)?;

    log_only_assert!(
        n_door_prizes_to_insert == n_door_prizes_inserted,
        "Event door prizes insert should have inserted {} rows, but it inserted {}",
        n_door_prizes_to_insert,
        n_door_prizes_inserted,
    );
    let _insert_door_prizes_duration = (Utc::now() - insert_door_prizes_start).as_seconds_f64();

    let insert_door_prize_items_start = Utc::now();
    let new_door_prize_items = iter::zip(&event_ids_by_game, &completed_games)
        .flat_map(|((game_id_a, event_ids), (game_id_b, game))| {
            assert_eq!(game_id_a, game_id_b);
            // Within this closure we're acting on all events in a single game
            iter::zip(event_ids, &game.events).flat_map(|(event_id, event)| {
                // Within this closure we're acting on a single event
                to_db_format::event_to_door_prize_items(*event_id, event)
            })
        })
        .collect_vec();

    let n_door_prize_items_to_insert = new_door_prize_items.len();
    let n_door_prize_items_inserted = diesel::copy_from(door_prize_items_dsl::door_prize_items)
        .from_insertable(&new_door_prize_items)
        .execute(conn)?;

    log_only_assert!(
        n_door_prize_items_to_insert == n_door_prize_items_inserted,
        "Event door prize items insert should have inserted {} rows, but it inserted {}",
        n_door_prize_items_to_insert,
        n_door_prize_items_inserted,
    );
    let _insert_door_prize_items_duration =
        (Utc::now() - insert_door_prize_items_start).as_seconds_f64();

    let insert_pitcher_changes_start = Utc::now();
    let new_pitcher_changes: Vec<_> = completed_games
        .iter()
        .flat_map(|(game_id, game)| {
            game.pitcher_changes.iter().map(|pitcher_change| {
                to_db_format::pitcher_change_to_row(taxa, *game_id, pitcher_change)
            })
        })
        .collect();

    let n_pitcher_changes_to_insert = new_pitcher_changes.len();
    let n_pitcher_changes_inserted = diesel::copy_from(pitcher_changes_dsl::pitcher_changes)
        .from_insertable(&new_pitcher_changes)
        .execute(conn)?;

    log_only_assert!(
        n_pitcher_changes_to_insert == n_pitcher_changes_inserted,
        "pitcher_changes insert should have inserted {} rows, but it inserted {}",
        n_pitcher_changes_to_insert,
        n_pitcher_changes_inserted,
    );
    let _insert_pitcher_changes_duration =
        (Utc::now() - insert_pitcher_changes_start).as_seconds_f64();

    let insert_parties_start = Utc::now();
    let new_parties: Vec<_> = completed_games
        .iter()
        .flat_map(|(game_id, game)| {
            game.parties
                .iter()
                .flat_map(|party| to_db_format::party_to_rows(taxa, *game_id, party))
        })
        .collect();

    let n_parties_to_insert = new_parties.len();
    let n_parties_inserted = diesel::copy_from(parties_dsl::parties)
        .from_insertable(&new_parties)
        .execute(conn)?;

    log_only_assert!(
        n_parties_to_insert == n_parties_inserted,
        "parties insert should have inserted {} rows, but it inserted {}",
        n_parties_to_insert,
        n_parties_inserted,
    );
    let _insert_parties_duration = (Utc::now() - insert_parties_start).as_seconds_f64();

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
                                    return Err(DbMetaQueryError::ColumnMissingField(
                                        "is_nullable",
                                    ));
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

macro_rules! player_cursor_from_table {
    ($conn:expr, $($namespace:ident)::*, $table_name:ident) => {
        $($namespace)::*::$table_name::dsl::$table_name
            .select(($($namespace)::*::$table_name::dsl::valid_from, $($namespace)::*::$table_name::dsl::mmolb_player_id))
            .order_by((
                $($namespace)::*::$table_name::dsl::valid_from.desc(),
                $($namespace)::*::$table_name::dsl::mmolb_player_id.desc(),
            ))
            .limit(1)
            .get_result::<(NaiveDateTime, String)>($conn)
            .optional()
    };
}

fn max_of_options<T: Ord>(a: Option<T>, b: Option<T>) -> Option<T> {
    match (a, b) {
        (None, None) => None,
        (Some(accum), None) => Some(accum),
        (None, Some(value)) => Some(value),
        (Some(accum), Some(value)) => Some(std::cmp::max(accum, value)),
    }
}

pub fn get_player_ingest_start_cursor(
    conn: &mut PgConnection,
) -> QueryResult<Option<(NaiveDateTime, String)>> {
    use crate::schema::data_schema::data as schema;

    let player_version_with_embedded_feed_cutoff =
        DateTime::parse_from_rfc3339("2025-07-28 12:00:00.000000Z")
            .unwrap()
            .naive_utc();

    // This must list all tables that have a valid_from derived from the `player` kind.
    let cursor: Option<(NaiveDateTime, String)> = [
        player_cursor_from_table!(conn, schema, player_versions)?,
        player_cursor_from_table!(conn, schema, player_feed_versions)?.map(|(dt, id)| {
            (
                std::cmp::min(player_version_with_embedded_feed_cutoff, dt),
                id,
            )
        }),
        player_cursor_from_table!(conn, schema, player_modification_versions)?,
        player_cursor_from_table!(conn, schema, player_equipment_versions)?,
        player_cursor_from_table!(conn, schema, player_equipment_effect_versions)?,
        player_cursor_from_table!(conn, schema, player_report_versions)?,
        player_cursor_from_table!(conn, schema, player_report_attribute_versions)?,
    ]
    .into_iter()
    // Compute the latest of all cursors
    .fold(None, max_of_options);

    Ok(cursor)
}

macro_rules! team_cursor_from_table {
    ($conn:expr, $($namespace:ident)::*, $table_name:ident) => {
        $($namespace)::*::$table_name::dsl::$table_name
            .select(($($namespace)::*::$table_name::dsl::valid_from, $($namespace)::*::$table_name::dsl::mmolb_team_id))
            .order_by((
                $($namespace)::*::$table_name::dsl::valid_from.desc(),
                $($namespace)::*::$table_name::dsl::mmolb_team_id.desc(),
            ))
            .limit(1)
            .get_result::<(NaiveDateTime, String)>($conn)
            .optional()
    };
}

pub fn get_team_ingest_start_cursor(
    conn: &mut PgConnection,
) -> QueryResult<Option<(NaiveDateTime, String)>> {
    use crate::schema::data_schema::data as schema;

    // This must list all tables that have a valid_from derived from the `player` kind.
    let cursor: Option<(NaiveDateTime, String)> = [
        team_cursor_from_table!(conn, schema, team_versions)?,
        team_cursor_from_table!(conn, schema, team_player_versions)?,
    ]
    .into_iter()
    // Compute the latest of all cursors
    .fold(None, max_of_options);

    Ok(cursor)
}

pub fn get_player_feed_ingest_start_cursor(
    conn: &mut PgConnection,
) -> QueryResult<Option<(NaiveDateTime, String)>> {
    use crate::schema::data_schema::data::player_feed_versions::dsl as player_feed_dsl;

    player_feed_dsl::player_feed_versions
        .select((
            player_feed_dsl::valid_from,
            player_feed_dsl::mmolb_player_id,
        ))
        .order_by((
            player_feed_dsl::valid_from.desc(),
            player_feed_dsl::mmolb_player_id.desc(),
        ))
        .limit(1)
        .get_result(conn)
        .optional()
}

pub fn get_team_feed_ingest_start_cursor(
    conn: &mut PgConnection,
) -> QueryResult<Option<(NaiveDateTime, String)>> {
    use crate::schema::data_schema::data::team_feed_versions::dsl as team_feed_dsl;

    team_feed_dsl::team_feed_versions
        .select((
            team_feed_dsl::valid_from,
            team_feed_dsl::mmolb_team_id,
        ))
        .order_by((
            team_feed_dsl::valid_from.desc(),
            team_feed_dsl::mmolb_team_id.desc(),
        ))
        .limit(1)
        .get_result(conn)
        .optional()
}

pub fn get_modifications_table(
    conn: &mut PgConnection,
) -> QueryResult<HashMap<NameEmojiTooltip, i64>> {
    use crate::data_schema::data::modifications::dsl as mod_dsl;

    let table = mod_dsl::modifications
        .select((
            mod_dsl::id,
            mod_dsl::name,
            mod_dsl::emoji,
            mod_dsl::description,
        ))
        .get_results::<(i64, String, String, String)>(conn)?
        .into_iter()
        .map(|(id, name, emoji, tooltip)| {
            (
                NameEmojiTooltip {
                    name,
                    emoji,
                    tooltip,
                },
                id,
            )
        })
        .collect();

    Ok(table)
}

pub fn insert_modifications(
    conn: &mut PgConnection,
    new_modifications: &[&(
        &str, /* name */
        &str, /* emoji */
        &str, /* description */
    )],
) -> QueryResult<Option<Vec<(NameEmojiTooltip, i64)>>> {
    use crate::data_schema::data::modifications::dsl as mod_dsl;

    let to_insert = new_modifications
        .iter()
        .map(|(name, emoji, description)| NewModification {
            name,
            emoji,
            description,
        })
        .collect_vec();

    let to_insert_len = to_insert.len();
    let results = diesel::insert_into(mod_dsl::modifications)
        .values(to_insert)
        .returning((
            mod_dsl::id,
            mod_dsl::name,
            mod_dsl::emoji,
            mod_dsl::description,
        ))
        .on_conflict((mod_dsl::name, mod_dsl::emoji, mod_dsl::description))
        .do_nothing()
        .get_results::<(i64, String, String, String)>(conn)?;

    // TODO investigate on using on_conflict().update().set(some no-op) --
    //   I think that should allow a get-or-insert
    if results.len() < to_insert_len {
        // Returning a None signals that we weren't able to insert all
        // the modifications due to a conflict and that the caller
        // should call us again with the same arguments
        return Ok(None);
    }

    let results = results
        .into_iter()
        .map(|(id, name, emoji, tooltip)| {
            (
                NameEmojiTooltip {
                    name,
                    emoji,
                    tooltip,
                },
                id,
            )
        })
        .collect();

    Ok(Some(results))
}

pub fn get_latest_player_valid_from(conn: &mut PgConnection) -> QueryResult<Option<NaiveDateTime>> {
    use crate::data_schema::data::player_versions::dsl as pv_dsl;

    pv_dsl::player_versions
        .select(pv_dsl::valid_from)
        .order(pv_dsl::valid_from.desc())
        .limit(1)
        .get_result(conn)
        .optional()
}

pub fn latest_player_versions(
    conn: &mut PgConnection,
    player_ids: &[String],
) -> QueryResult<HashMap<String, DbPlayerVersion>> {
    use crate::data_schema::data::player_versions::dsl as pv_dsl;

    let map = pv_dsl::player_versions
        .filter(pv_dsl::mmolb_player_id.eq_any(player_ids))
        .filter(pv_dsl::valid_until.is_null())
        .select(DbPlayerVersion::as_select())
        .order_by(pv_dsl::mmolb_player_id)
        .get_results::<DbPlayerVersion>(conn)?
        .into_iter()
        .map(|v| (v.mmolb_player_id.clone(), v))
        .collect();

    Ok(map)
}

type NewPlayerFeedVersionExt<'a> = (
    NewPlayerFeedVersion<'a>,
    Vec<NewPlayerAttributeAugment<'a>>,
    Vec<NewPlayerParadigmShift<'a>>,
    Vec<NewPlayerRecomposition<'a>>,
    Vec<NewVersionIngestLog<'a>>,
);

type NewPlayerVersionExt<'a> = (
    NewPlayerVersion<'a>,
    Vec<NewPlayerModificationVersion<'a>>,
    Option<NewPlayerFeedVersionExt<'a>>,
    Vec<(
        NewPlayerReportVersion<'a>,
        Vec<NewPlayerReportAttributeVersion<'a>>,
    )>,
    Vec<(
        NewPlayerEquipmentVersion<'a>,
        Vec<NewPlayerEquipmentEffectVersion<'a>>,
    )>,
    Vec<NewVersionIngestLog<'a>>,
);

fn insert_player_report_attribute_versions(
    conn: &mut PgConnection,
    new_player_report_attribute_versions: Vec<Vec<NewPlayerReportAttributeVersion>>,
) -> QueryResult<usize> {
    use crate::data_schema::data::player_report_attribute_versions::dsl as prav_dsl;
    let new_player_report_attribute_versions = new_player_report_attribute_versions
        .into_iter()
        .flatten()
        .collect_vec();

    // Insert new records
    diesel::copy_from(prav_dsl::player_report_attribute_versions)
        .from_insertable(new_player_report_attribute_versions)
        .execute(conn)
}

fn insert_player_report_versions(
    conn: &mut PgConnection,
    new_player_report_versions: Vec<
        Vec<(NewPlayerReportVersion, Vec<NewPlayerReportAttributeVersion>)>,
    >,
) -> QueryResult<usize> {
    use crate::data_schema::data::player_report_versions::dsl as prv_dsl;

    let (new_player_report_versions, new_player_report_attribute_versions): (
        Vec<NewPlayerReportVersion>,
        Vec<Vec<NewPlayerReportAttributeVersion>>,
    ) = itertools::multiunzip(new_player_report_versions.into_iter().flatten());

    // Insert new records
    let num_inserted = diesel::copy_from(prv_dsl::player_report_versions)
        .from_insertable(new_player_report_versions)
        .execute(conn)?;

    insert_player_report_attribute_versions(conn, new_player_report_attribute_versions)?;

    Ok(num_inserted)
}

fn insert_player_equipment_effects(
    conn: &mut PgConnection,
    new_player_equipment_effects: Vec<Vec<NewPlayerEquipmentEffectVersion>>,
) -> QueryResult<usize> {
    use crate::data_schema::data::player_equipment_effect_versions::dsl as peev_dsl;

    let new_player_equipment_effects = new_player_equipment_effects
        .into_iter()
        .flatten()
        .collect_vec();

    // Insert new records
    diesel::copy_from(peev_dsl::player_equipment_effect_versions)
        .from_insertable(new_player_equipment_effects)
        .execute(conn)
}

fn insert_player_equipment(
    conn: &mut PgConnection,
    new_player_equipment: Vec<
        Vec<(
            NewPlayerEquipmentVersion,
            Vec<NewPlayerEquipmentEffectVersion>,
        )>,
    >,
) -> QueryResult<usize> {
    use crate::data_schema::data::player_equipment_versions::dsl as pev_dsl;

    let (new_player_equipment_versions, new_player_equipment_effect_versions): (
        Vec<NewPlayerEquipmentVersion>,
        Vec<Vec<NewPlayerEquipmentEffectVersion>>,
    ) = itertools::multiunzip(new_player_equipment.into_iter().flatten());

    // Insert new records
    let insert_versions_start = Utc::now();
    let num_inserted = diesel::copy_from(pev_dsl::player_equipment_versions)
        .from_insertable(new_player_equipment_versions)
        .execute(conn)?;
    let insert_versions_duration = (Utc::now() - insert_versions_start).as_seconds_f64();

    let insert_effect_versions_start = Utc::now();
    insert_player_equipment_effects(conn, new_player_equipment_effect_versions)?;
    let insert_effect_versions_duration =
        (Utc::now() - insert_effect_versions_start).as_seconds_f64();
    info!(
        "insert_equipment_versions_duration: {insert_versions_duration:.2}, \
        insert_equipment_effect_versions_duration: {insert_effect_versions_duration:.2}"
    );

    Ok(num_inserted)
}

pub fn insert_player_feed_versions<'a>(
    conn: &mut PgConnection,
    new_player_feed_versions: impl IntoIterator<Item = NewPlayerFeedVersionExt<'a>>,
) -> QueryResult<usize> {
    use crate::data_schema::data::player_feed_versions::dsl as pfv_dsl;

    let (
        new_player_feed_versions,
        new_player_attribute_augments,
        new_player_paradigm_shifts,
        new_player_recompositions,
        ingest_logs,
    ): (
        Vec<NewPlayerFeedVersion>,
        Vec<Vec<NewPlayerAttributeAugment>>,
        Vec<Vec<NewPlayerParadigmShift>>,
        Vec<Vec<NewPlayerRecomposition>>,
        Vec<Vec<NewVersionIngestLog>>,
    ) = itertools::multiunzip(new_player_feed_versions.into_iter());

    // Insert new records
    let num_inserted = diesel::copy_from(pfv_dsl::player_feed_versions)
        .from_insertable(new_player_feed_versions)
        .execute(conn)?;

    insert_player_attribute_augments(conn, new_player_attribute_augments)?;
    insert_player_paradigm_shifts(conn, new_player_paradigm_shifts)?;
    insert_player_recompositions(conn, new_player_recompositions)?;
    insert_ingest_logs(conn, ingest_logs)?;

    Ok(num_inserted)
}

type NewTeamFeedVersionExt<'a> = (
    NewTeamFeedVersion<'a>,
    Vec<NewTeamGamePlayed<'a>>,
    Vec<NewVersionIngestLog<'a>>,
);

fn insert_new_team_games_played(
    conn: &mut PgConnection,
    new_team_games_played: Vec<Vec<NewTeamGamePlayed>>,
) -> QueryResult<usize> {
    use crate::data_schema::data::team_games_played::dsl as get_dsl;
    let new_team_games_played = new_team_games_played.into_iter().flatten().collect_vec();

    // Insert new records
    diesel::copy_from(get_dsl::team_games_played)
        .from_insertable(new_team_games_played)
        .execute(conn)
}
pub fn insert_team_feed_versions<'a>(
    conn: &mut PgConnection,
    new_team_feed_versions: impl IntoIterator<Item = NewTeamFeedVersionExt<'a>>,
) -> QueryResult<usize> {
    use crate::data_schema::data::team_feed_versions::dsl as tfv_dsl;

    let (
        new_team_feed_versions,
        new_team_games_played,
        ingest_logs,
    ): (
        Vec<NewTeamFeedVersion>,
        Vec<Vec<NewTeamGamePlayed>>,
        Vec<Vec<NewVersionIngestLog>>,
    ) = itertools::multiunzip(new_team_feed_versions.into_iter());

    // Insert new records
    let num_inserted = diesel::copy_from(tfv_dsl::team_feed_versions)
        .from_insertable(new_team_feed_versions)
        .execute(conn)?;

    insert_new_team_games_played(conn, new_team_games_played)?;
    insert_ingest_logs(conn, ingest_logs)?;

    Ok(num_inserted)
}

fn insert_player_recompositions(
    conn: &mut PgConnection,
    new_player_recompositions: Vec<Vec<NewPlayerRecomposition>>,
) -> QueryResult<usize> {
    use crate::data_schema::data::player_recompositions::dsl as pr_dsl;
    let player_recompositions = new_player_recompositions
        .into_iter()
        .flatten()
        .collect_vec();

    // Insert new records
    diesel::copy_from(pr_dsl::player_recompositions)
        .from_insertable(player_recompositions)
        .execute(conn)
}

fn insert_ingest_logs(
    conn: &mut PgConnection,
    new_logs: Vec<Vec<NewVersionIngestLog>>,
) -> QueryResult<usize> {
    use crate::info_schema::info::version_ingest_log::dsl as vil_dsl;
    let new_logs = new_logs.into_iter().flatten().collect_vec();

    // Insert new records
    diesel::copy_from(vil_dsl::version_ingest_log)
        .from_insertable(new_logs)
        .execute(conn)
}

fn insert_player_paradigm_shifts(
    conn: &mut PgConnection,
    new_player_paradigm_shifts: Vec<Vec<NewPlayerParadigmShift>>,
) -> QueryResult<usize> {
    use crate::data_schema::data::player_paradigm_shifts::dsl as pps_dsl;
    let player_augments = new_player_paradigm_shifts
        .into_iter()
        .flatten()
        .collect_vec();

    // Insert new records
    diesel::copy_from(pps_dsl::player_paradigm_shifts)
        .from_insertable(player_augments)
        .execute(conn)
}

fn insert_player_attribute_augments(
    conn: &mut PgConnection,
    new_player_augments: Vec<Vec<NewPlayerAttributeAugment>>,
) -> QueryResult<usize> {
    use crate::data_schema::data::player_attribute_augments::dsl as paa_dsl;
    let player_attribute_augments = new_player_augments.into_iter().flatten().collect_vec();

    // Insert new records
    diesel::copy_from(paa_dsl::player_attribute_augments)
        .from_insertable(player_attribute_augments)
        .execute(conn)
}

fn insert_player_modifications(
    conn: &mut PgConnection,
    new_player_modification_versions: Vec<Vec<NewPlayerModificationVersion>>,
) -> QueryResult<usize> {
    use crate::data_schema::data::player_modification_versions::dsl as pmv_dsl;

    let new_player_modification_versions = new_player_modification_versions
        .into_iter()
        .flatten()
        .collect_vec();

    // Insert new records
    diesel::copy_from(pmv_dsl::player_modification_versions)
        .from_insertable(new_player_modification_versions)
        .execute(conn)
}

pub fn insert_player_versions_with_retry<'v, 'g>(
    conn: &mut PgConnection,
    new_player_versions: &'v [NewPlayerVersionExt<'g>],
) -> (usize, Vec<(&'v NewPlayerVersionExt<'g>, QueryError)>) {
    let num_versions = new_player_versions.len();
    if num_versions == 0 {
        return (0, Vec::new());
    }

    let res = conn.transaction(|conn| insert_player_versions(conn, new_player_versions));

    match res {
        Ok(inserted) => (inserted, Vec::new()),
        Err(e) => {
            if num_versions == 1 {
                (0, vec![(&new_player_versions[0], e)])
            } else {
                let pivot = num_versions / 2;
                let (left, right) = new_player_versions.split_at(pivot);
                let (inserted_a, mut errs_a) = insert_player_versions_with_retry(conn, left);
                let (inserted_b, errs_b) = insert_player_versions_with_retry(conn, right);
                errs_a.extend(errs_b);
                (inserted_a + inserted_b, errs_a)
            }
        }
    }
}

pub fn insert_player_versions(
    conn: &mut PgConnection,
    new_player_versions: &[NewPlayerVersionExt],
) -> QueryResult<usize> {
    use crate::data_schema::data::player_versions::dsl as pv_dsl;

    let preprocess_start = Utc::now();
    let mut new = Vec::new();
    new.extend_from_slice(new_player_versions);
    let new_player_versions = new;

    let (
        new_player_versions,
        new_player_modification_versions,
        new_player_feed_versions,
        new_player_report_attributes,
        new_player_equipment,
        new_ingest_logs,
    ): (
        Vec<NewPlayerVersion>,
        Vec<Vec<NewPlayerModificationVersion>>,
        Vec<Option<NewPlayerFeedVersionExt>>,
        Vec<Vec<(NewPlayerReportVersion, Vec<NewPlayerReportAttributeVersion>)>>,
        Vec<
            Vec<(
                NewPlayerEquipmentVersion,
                Vec<NewPlayerEquipmentEffectVersion>,
            )>,
        >,
        Vec<Vec<NewVersionIngestLog>>,
    ) = itertools::multiunzip(new_player_versions);
    let preprocess_duration = (Utc::now() - preprocess_start).as_seconds_f64();

    // Insert new records
    let insert_player_version_start = Utc::now();
    let num_player_insertions = diesel::copy_from(pv_dsl::player_versions)
        .from_insertable(new_player_versions)
        .execute(conn)?;
    let insert_player_version_duration =
        (Utc::now() - insert_player_version_start).as_seconds_f64();

    let insert_player_modifications_start = Utc::now();
    insert_player_modifications(conn, new_player_modification_versions)?;
    let insert_player_modifications_duration =
        (Utc::now() - insert_player_modifications_start).as_seconds_f64();

    let insert_player_feed_versions_start = Utc::now();
    insert_player_feed_versions(conn, new_player_feed_versions.into_iter().flatten())?;
    let insert_player_feed_versions_duration =
        (Utc::now() - insert_player_feed_versions_start).as_seconds_f64();

    let insert_player_reports_start = Utc::now();
    insert_player_report_versions(conn, new_player_report_attributes)?;
    let insert_player_reports_duration =
        (Utc::now() - insert_player_reports_start).as_seconds_f64();

    let insert_player_equipment_start = Utc::now();
    insert_player_equipment(conn, new_player_equipment)?;
    let insert_player_equipment_duration =
        (Utc::now() - insert_player_equipment_start).as_seconds_f64();

    let insert_ingest_logs_start = Utc::now();
    insert_ingest_logs(conn, new_ingest_logs)?;
    let insert_ingest_logs_duration = (Utc::now() - insert_ingest_logs_start).as_seconds_f64();

    info!(
        "preprocess_duration: {preprocess_duration:.2}, \
        insert_player_equipment_duration: {insert_player_equipment_duration:.2}, \
        insert_player_reports_duration: {insert_player_reports_duration:.2}, \
        insert_player_feed_versions_duration: {insert_player_feed_versions_duration:.2}, \
        insert_player_modifications_duration: {insert_player_modifications_duration:.2}, \
        insert_player_version_duration: {insert_player_version_duration:.2}, \
        insert_ingest_logs_duration: {insert_ingest_logs_duration:.2}"
    );

    Ok(num_player_insertions)
}

pub fn get_player_versions(
    conn: &mut PgConnection,
    player_id: &str,
) -> QueryResult<Vec<DbPlayerVersion>> {
    use crate::data_schema::data::player_versions::dsl as pv_dsl;

    pv_dsl::player_versions
        .filter(pv_dsl::mmolb_player_id.eq(player_id))
        .order(pv_dsl::valid_from.asc())
        .select(DbPlayerVersion::as_select())
        .get_results(conn)
}

pub fn get_player_modification_versions(
    conn: &mut PgConnection,
    player_id: &str,
) -> QueryResult<Vec<DbPlayerModificationVersion>> {
    use crate::data_schema::data::player_modification_versions::dsl as pmv_dsl;

    pmv_dsl::player_modification_versions
        .filter(pmv_dsl::mmolb_player_id.eq(player_id))
        .order(pmv_dsl::valid_from.asc())
        .select(DbPlayerModificationVersion::as_select())
        .get_results(conn)
}

pub fn get_player_equipment_versions(
    conn: &mut PgConnection,
    player_id: &str,
) -> QueryResult<Vec<DbPlayerEquipmentVersion>> {
    use crate::data_schema::data::player_equipment_versions::dsl as pev_dsl;

    pev_dsl::player_equipment_versions
        .filter(pev_dsl::mmolb_player_id.eq(player_id))
        .order(pev_dsl::valid_from.asc())
        .select(DbPlayerEquipmentVersion::as_select())
        .get_results(conn)
}

pub fn get_player_equipment_effect_versions(
    conn: &mut PgConnection,
    player_id: &str,
) -> QueryResult<Vec<DbPlayerEquipmentEffectVersion>> {
    use crate::data_schema::data::player_equipment_effect_versions::dsl as peev_dsl;

    peev_dsl::player_equipment_effect_versions
        .filter(peev_dsl::mmolb_player_id.eq(player_id))
        .order(peev_dsl::valid_from.asc())
        .select(DbPlayerEquipmentEffectVersion::as_select())
        .get_results(conn)
}

pub fn get_modifications(conn: &mut PgConnection, ids: &[i64]) -> QueryResult<Vec<DbModification>> {
    use crate::data_schema::data::modifications::dsl as m_dsl;

    m_dsl::modifications
        .filter(m_dsl::id.eq_any(ids))
        .select(DbModification::as_select())
        .get_results(conn)
}

macro_rules! rollback_table {
    ($conn:expr, $($namespace:ident)::*, $table_name:ident, $dt:ident) => {{
        // This struct is a workaround for the apparent otherwise inability
        // to get Diesel to set a column to null
        #[derive(AsChangeset)]
        #[diesel(table_name = $($namespace)::*::$table_name)]
        #[diesel(treat_none_as_null = true)]
        struct Update {
            valid_until: Option<NaiveDateTime>,
        }

        // Delete all versions that began after the target date
        diesel::delete($($namespace)::*::$table_name::dsl::$table_name)
            .filter($($namespace)::*::$table_name::dsl::valid_from.le($dt))
            .execute($conn)?;

        // Un-close-out all versions that were closed out after the target date
        diesel::update($($namespace)::*::$table_name::dsl::$table_name)
            .filter($($namespace)::*::$table_name::dsl::valid_until.le($dt))
            .set(&Update { valid_until: None })
            .execute($conn)?;
    }};
}

pub fn roll_back_ingest_to_date(conn: &mut PgConnection, dt: NaiveDateTime) -> QueryResult<()> {
    use crate::schema::data_schema::data as schema;

    rollback_table!(conn, schema, player_versions, dt);
    rollback_table!(conn, schema, player_feed_versions, dt);
    rollback_table!(conn, schema, player_modification_versions, dt);
    rollback_table!(conn, schema, player_equipment_versions, dt);
    rollback_table!(conn, schema, player_equipment_effect_versions, dt);
    rollback_table!(conn, schema, player_report_versions, dt);
    rollback_table!(conn, schema, player_report_attribute_versions, dt);
    rollback_table!(conn, schema, team_versions, dt);

    Ok(())
}

#[derive(Queryable)]
pub struct PitchTypeInfo {
    pub pitch_type: Option<i64>,
    pub event_type: i64,
    pub min_speed: Option<f64>,
    pub max_speed: Option<f64>,
    pub count: i64,
}

#[derive(Queryable)]
pub struct Outcome {
    pub event_type: i64,
    pub count: i64,
}

pub struct PlayerAll {
    pub player: DbPlayerVersion,
    pub pitch_types: Option<Vec<PitchTypeInfo>>,
    pub batting_outcomes: Option<Vec<Outcome>>,
    pub fielding_outcomes: Option<Vec<Outcome>>,
    pub pitching_outcomes: Option<Vec<Outcome>>,
}

use crate::schema::data_schema::data::events::dsl as event_dsl;
use crate::schema::data_schema::data::games::dsl as game_dsl;
use diesel::helper_types as d;
use crate::schema::data_schema::data::parties::dsl::parties;

type PlayerFilter<'q, Field> = d::And<
    d::Eq<Field, &'q str>,
    d::Or<
        d::And<
            d::Eq<event_dsl::top_of_inning, bool>,
            d::Eq<game_dsl::home_team_mmolb_id, &'q str>,
        >,
        d::And<
            d::Eq<event_dsl::top_of_inning, bool>,
            d::Eq<game_dsl::away_team_mmolb_id, &'q str>,
        >
    >
>;

fn filter_pitcher<'q>(pitcher_name: &'q str, team_id: &'q str) -> PlayerFilter<'q, event_dsl::pitcher_name> {
    event_dsl::pitcher_name.eq(pitcher_name)
        .and(
            event_dsl::top_of_inning.eq(true)
                .and(game_dsl::home_team_mmolb_id.eq(team_id))
                .or(event_dsl::top_of_inning.eq(false)
                    .and(game_dsl::away_team_mmolb_id.eq(team_id)))
        )
}

fn filter_fielder<'q>(pitcher_name: &'q str, team_id: &'q str) -> PlayerFilter<'q, event_dsl::fair_ball_fielder_name> {
    event_dsl::fair_ball_fielder_name.eq(pitcher_name)
        .and(
            event_dsl::top_of_inning.eq(true)
                .and(game_dsl::home_team_mmolb_id.eq(team_id))
                .or(event_dsl::top_of_inning.eq(false)
                    .and(game_dsl::away_team_mmolb_id.eq(team_id)))
        )
}

fn filter_batter<'q>(batter_name: &'q str, team_id: &'q str) -> PlayerFilter<'q, event_dsl::batter_name> {
    event_dsl::batter_name.eq(batter_name)
        .and(
            event_dsl::top_of_inning.eq(false)
                .and(game_dsl::home_team_mmolb_id.eq(team_id))
                .or(event_dsl::top_of_inning.eq(true)
                    .and(game_dsl::away_team_mmolb_id.eq(team_id)))
        )
}

// I want this to be a function, but I cannot figure out the Diesel
// types involved
macro_rules! outcomes {
    ($player_filter:expr, $season:expr, $conn:expr) => {
        event_dsl::events
            .inner_join(game_dsl::games)
            .filter($player_filter)
            // Filter by season if it's provided
            .filter(game_dsl::season.eq($season.unwrap_or(-1)).or::<bool, Bool>($season.is_none()))
            .group_by(event_dsl::event_type)
            .order_by(count(event_dsl::id).desc())
            .select((
                event_dsl::event_type,
                count(event_dsl::id),
            ))
            .get_results::<Outcome>($conn)

    };
}

pub fn player_all(
    conn: &mut PgConnection,
    player_id: &str,
    taxa: &Taxa,
    season: Option<i32>,
) -> QueryResult<PlayerAll> {
    use crate::schema::data_schema::data::events::dsl as event_dsl;
    use crate::schema::data_schema::data::games::dsl as game_dsl;
    use crate::schema::data_schema::data::player_versions::dsl as pv_dsl;

    let player = pv_dsl::player_versions
        .filter(pv_dsl::mmolb_player_id.eq(player_id))
        .filter(pv_dsl::valid_until.is_null())
        .select(DbPlayerVersion::as_select())
        .get_result(conn)?;

    let full_name = format!("{} {}", player.first_name, player.last_name);

    // TODO Extract a function
    let pitch_types = if let Some(team_id) = &player.mmolb_team_id {
        let result = event_dsl::events
            .inner_join(game_dsl::games)
            .filter(filter_pitcher(&full_name, team_id))
            // This is a workaround to conditionally apply the season filter iff
            // season is set. Diesel makes it really difficult to conditionally add
            // a filter to a query that uses group_by.
            .filter(game_dsl::season.eq(season.unwrap_or(-1)).or::<bool, Bool>(season.is_none()))
            .group_by((event_dsl::pitch_type, event_dsl::event_type))
            .order_by((event_dsl::pitch_type.asc(), event_dsl::event_type.asc()))
            .select((
                event_dsl::pitch_type,
                event_dsl::event_type,
                min(event_dsl::pitch_speed),
                max(event_dsl::pitch_speed),
                count(event_dsl::id),
            ))
            .get_results::<PitchTypeInfo>(conn)?;

        Some(result)
    } else {
        None
    };

    let batting_outcomes = player.mmolb_team_id.as_ref()
        .map(|team_id| outcomes!(filter_batter(&full_name, team_id), season, conn))
        .transpose()?;

    let fielding_outcomes = player.mmolb_team_id.as_ref()
        .map(|team_id| outcomes!(filter_fielder(&full_name, team_id), season, conn))
        .transpose()?;

    let pitching_outcomes = player.mmolb_team_id.as_ref()
        .map(|team_id| outcomes!(filter_pitcher(&full_name, team_id), season, conn))
        .transpose()?;

    Ok(PlayerAll {
        player,
        pitch_types,
        batting_outcomes,
        fielding_outcomes,
        pitching_outcomes,
    })
}

type NewTeamVersionExt<'a> = (
    NewTeamVersion<'a>,
    Vec<NewTeamPlayerVersion<'a>>,
    Vec<NewVersionIngestLog<'a>>,
);

fn insert_team_player_versions(
    conn: &mut PgConnection,
    new_team_player_versions: Vec<Vec<NewTeamPlayerVersion>>,
) -> QueryResult<usize> {
    use crate::data_schema::data::team_player_versions::dsl as tpv_dsl;

    let new_team_player_versions = new_team_player_versions.into_iter().flatten().collect_vec();

    // Insert new records
    diesel::copy_from(tpv_dsl::team_player_versions)
        .from_insertable(new_team_player_versions)
        .execute(conn)
}

pub fn insert_team_versions(
    conn: &mut PgConnection,
    new_team_versions: Vec<NewTeamVersionExt>,
) -> QueryResult<usize> {
    use crate::data_schema::data::team_versions::dsl as tv_dsl;

    let (new_team_versions, new_team_player_versions, new_ingest_logs): (
        Vec<NewTeamVersion>,
        Vec<Vec<NewTeamPlayerVersion>>,
        Vec<Vec<NewVersionIngestLog>>,
    ) = itertools::multiunzip(new_team_versions);

    // Insert new records
    let insert_team_version_start = Utc::now();
    let num_team_insertions = diesel::copy_from(tv_dsl::team_versions)
        .from_insertable(new_team_versions)
        .execute(conn)?;
    let insert_team_version_duration = (Utc::now() - insert_team_version_start).as_seconds_f64();

    let insert_team_player_versions_start = Utc::now();
    insert_team_player_versions(conn, new_team_player_versions)?;
    let insert_team_player_versions_duration =
        (Utc::now() - insert_team_player_versions_start).as_seconds_f64();

    let insert_ingest_logs_start = Utc::now();
    insert_ingest_logs(conn, new_ingest_logs)?;
    let insert_ingest_logs_duration = (Utc::now() - insert_ingest_logs_start).as_seconds_f64();

    info!(
        "insert_team_version_duration: {insert_team_version_duration:.2}, \
        insert_team_player_versions_duration: {insert_team_player_versions_duration:.2}, \
        insert_ingest_logs_duration: {insert_ingest_logs_duration:.2}"
    );

    Ok(num_team_insertions)
}

pub fn update_num_ingested(
    conn: &mut PgConnection,
    ingest_id: i64,
    name: &str,
    count: i32,
) -> QueryResult<()> {
    use crate::info_schema::info::ingest_counts::dsl as ic_dsl;

    let new = NewIngestCount {
        ingest_id,
        name,
        count,
    };

    diesel::insert_into(ic_dsl::ingest_counts)
        .values(&new)
        .on_conflict((ic_dsl::ingest_id, ic_dsl::name))
        .do_update()
        .set(&new)
        .execute(conn)?;

    Ok(())
}

pub fn refresh_matviews(conn: &mut PgConnection) -> QueryResult<()> {
    sql_query("refresh materialized view data.offense_outcomes").execute(conn)?;
    sql_query("refresh materialized view data.defense_outcomes").execute(conn)?;

    Ok(())
}

pub struct GamesStats {
    pub num_games: i64,
    pub num_events: i64,
    pub num_baserunners: i64,
    pub num_fielders: i64,
    pub num_pitcher_changes: i64,
    pub num_aurora_photos: i64,
    pub num_ejections: i64,
    pub num_parties: i64,
    pub num_games_with_issues: i64,
}
pub fn games_stats(conn: &mut PgConnection) -> QueryResult<GamesStats> {
    use crate::data_schema::data::*;
    use crate::info_schema::info::*;

    let num_games = games::dsl::games.select(count_star())
        .get_result(conn)?;
    let num_events = events::dsl::events.select(count_star())
        .get_result(conn)?;
    let num_baserunners = event_baserunners::dsl::event_baserunners.select(count_star())
        .get_result(conn)?;
    let num_fielders = event_fielders::dsl::event_fielders.select(count_star())
        .get_result(conn)?;
    let num_pitcher_changes = pitcher_changes::dsl::pitcher_changes.select(count_star())
        .get_result(conn)?;
    let num_aurora_photos = aurora_photos::dsl::aurora_photos.select(count_star())
        .get_result(conn)?;
    let num_ejections = ejections::dsl::ejections.select(count_star())
        .get_result(conn)?;
    let num_parties = parties::dsl::parties.select(count_star())
        .get_result(conn)?;

    let num_games_with_issues = event_ingest_log::dsl::event_ingest_log
        .select(count_distinct(event_ingest_log::dsl::game_id))
        .filter(event_ingest_log::dsl::log_level.le(2))
        .get_result(conn)?;

    Ok(GamesStats {
        num_games,
        num_events,
        num_baserunners,
        num_fielders,
        num_pitcher_changes,
        num_aurora_photos,
        num_ejections,
        num_parties,
        num_games_with_issues,
    })
}

pub struct PlayersStats {
    pub num_player_versions: i64,
    pub num_player_modification_versions: i64,
    pub num_player_equipment_versions: i64,
    pub num_player_equipment_effect_versions: i64,
    pub num_player_report_versions: i64,
    pub num_player_report_attribute_versions: i64,
    pub num_player_attribute_augments: i64,
    pub num_player_paradigm_shifts: i64,
    pub num_player_recompositions: i64,
    pub num_player_feed_versions: i64,
    pub num_players_with_issues: i64,
}
pub fn players_stats(conn: &mut PgConnection) -> QueryResult<PlayersStats> {
    use crate::data_schema::data::*;
    use crate::info_schema::info::*;

    let num_player_versions = player_versions::dsl::player_versions.select(count_star())
        .get_result(conn)?;
    let num_player_modification_versions = player_modification_versions::dsl::player_modification_versions.select(count_star())
        .get_result(conn)?;
    let num_player_equipment_versions = player_equipment_versions::dsl::player_equipment_versions.select(count_star())
        .get_result(conn)?;
    let num_player_equipment_effect_versions = player_equipment_effect_versions::dsl::player_equipment_effect_versions.select(count_star())
        .get_result(conn)?;
    let num_player_report_versions = player_report_versions::dsl::player_report_versions.select(count_star())
        .get_result(conn)?;
    let num_player_report_attribute_versions = player_report_attribute_versions::dsl::player_report_attribute_versions.select(count_star())
        .get_result(conn)?;
    let num_player_attribute_augments = player_attribute_augments::dsl::player_attribute_augments.select(count_star())
        .get_result(conn)?;
    let num_player_paradigm_shifts = player_paradigm_shifts::dsl::player_paradigm_shifts.select(count_star())
        .get_result(conn)?;
    let num_player_recompositions = player_recompositions::dsl::player_recompositions.select(count_star())
        .get_result(conn)?;
    let num_player_feed_versions = player_feed_versions::dsl::player_feed_versions.select(count_star())
        .get_result(conn)?;

    let num_players_with_issues = version_ingest_log::dsl::version_ingest_log
        .select(count_distinct(version_ingest_log::dsl::entity_id))
        .filter(version_ingest_log::dsl::kind.eq("player"))
        .filter(version_ingest_log::dsl::log_level.le(2))
        .get_result(conn)?;

    Ok(PlayersStats {
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
    })
}

pub struct TeamsStats {
    pub num_team_versions: i64,
    pub num_team_player_versions: i64,
    pub num_team_games_played: i64,
    pub num_team_feed_versions: i64,
    pub num_teams_with_issues: i64,
}
pub fn teams_stats(conn: &mut PgConnection) -> QueryResult<TeamsStats> {
    use crate::data_schema::data::*;
    use crate::info_schema::info::*;

    let num_team_versions = team_versions::dsl::team_versions.select(count_star())
        .get_result(conn)?;
    let num_team_player_versions = team_player_versions::dsl::team_player_versions.select(count_star())
        .get_result(conn)?;
    let num_team_games_played = team_games_played::dsl::team_games_played.select(count_star())
        .get_result(conn)?;
    let num_team_feed_versions = team_feed_versions::dsl::team_feed_versions.select(count_star())
        .get_result(conn)?;

    let num_teams_with_issues = version_ingest_log::dsl::version_ingest_log
        .select(count_distinct(version_ingest_log::dsl::entity_id))
        .filter(version_ingest_log::dsl::kind.eq("team"))
        .filter(version_ingest_log::dsl::log_level.le(2))
        .get_result(conn)?;

    Ok(TeamsStats {
        num_team_versions,
        num_team_player_versions,
        num_team_games_played,
        num_team_feed_versions,
        num_teams_with_issues,
    })
}