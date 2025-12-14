mod entities;
mod to_db_format;
mod versions;
mod weather;

use std::collections::HashSet;
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
use log::{debug, error, info, warn};
use mmolb_parsing::ParsedEventMessage;
use mmolb_parsing::enums::Day;
use serde::Serialize;
use std::iter;
use diesel::connection::DefaultLoadingMode;
use thiserror::Error;
// First-party imports
use crate::event_detail::{EventDetail, IngestLog};
use crate::models::{DbAuroraPhoto, DbDoorPrize, DbDoorPrizeItem, DbEjection, DbEvent, DbEventIngestLog, DbFielder, DbGame, DbIngest, DbModification, DbPlayerEquipmentEffectVersion, DbPlayerEquipmentVersion, DbPlayerModificationVersion, DbPlayerVersion, DbRunner, NewEventIngestLog, NewGame, NewTeamGamePlayed, NewGameIngestTimings, NewIngest, NewIngestCount, NewModification, NewPlayerAttributeAugment, NewPlayerEquipmentEffectVersion, NewPlayerEquipmentVersion, NewPlayerModificationVersion, NewPlayerParadigmShift, NewPlayerRecomposition, NewPlayerReportAttributeVersion, NewPlayerReportVersion, NewPlayerVersion, NewTeamPlayerVersion, NewTeamVersion, NewVersionIngestLog, RawDbColumn, RawDbTable, DbPlayerRecomposition, DbPlayerReportVersion, DbPlayerReportAttributeVersion, DbPlayerAttributeAugment, DbWither, NewFeedEventProcessed, DbEfflorescence, DbEfflorescenceGrowth, DbFailedEjection};
use crate::taxa::Taxa;
use crate::{PartyEvent, PitcherChange, QueryError, WitherOutcome};

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

pub fn player_versions_count(conn: &mut PgConnection) -> QueryResult<i64> {
    use crate::data_schema::data::player_versions::dsl::*;

    player_versions.count().get_result(conn)
}

pub fn player_feed_versions_count(conn: &mut PgConnection) -> QueryResult<i64> {
    use crate::data_schema::data::player_feed_versions::dsl::*;

    player_feed_versions.count().get_result(conn)
}

pub fn team_versions_count(conn: &mut PgConnection) -> QueryResult<i64> {
    use crate::data_schema::data::team_versions::dsl::*;

    team_versions.count().get_result(conn)
}

pub fn feed_event_versions_count(conn: &mut PgConnection, of_kind: &str) -> QueryResult<i64> {
    use crate::data_schema::data::feed_events_processed::dsl::*;

    feed_events_processed
        .filter(kind.eq(of_kind))
        .count()
        .get_result(conn)
}

pub fn entity_counts(conn: &mut PgConnection) -> QueryResult<HashMap<String, (i64, i64)>> {
    #[derive(QueryableByName)]
    pub struct IssueCounts {
        #[diesel(sql_type = Text)]
        pub kind: String,
        #[diesel(sql_type = Nullable<BigInt>)]
        pub entities_with_issues: Option<i64>,
        #[diesel(sql_type = Nullable<BigInt>)]
        pub count: Option<i64>,
    }

    let results = sql_query("
        select coalesce(ewi.kind, e.kind) as kind, ewi.entities_with_issues, e.count
        from info.entities_with_issues_count ewi
        full outer join info.entities_count e on e.kind=ewi.kind
    ")
        .get_results::<IssueCounts>(conn)?;

    Ok(
        results.into_iter()
            .map(|r| (r.kind, (r.count.unwrap_or(0), r.entities_with_issues.unwrap_or(0))))
            .collect()
    )
}

pub fn latest_ingest_start_time(conn: &mut PgConnection) -> QueryResult<Option<NaiveDateTime>> {
    use crate::info_schema::info::ingests::dsl::*;

    ingests
        .select(started_at)
        .order_by(started_at.desc())
        .first(conn)
        .optional()
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
    #[diesel(sql_type = Nullable<Text>)]
    pub message: Option<String>,
    #[diesel(sql_type = Int8)]
    pub num_games: i64,
}

pub fn latest_ingests(conn: &mut PgConnection) -> QueryResult<Vec<IngestWithGameCount>> {
    sql_query(
        "
        select i.id, i.started_at, i.finished_at, i.aborted_at, i.message, count(g.mmolb_game_id) as num_games
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
    set_message: Option<&str>
) -> QueryResult<()> {
    use crate::info_schema::info::ingests::dsl;

    diesel::update(dsl::ingests.filter(dsl::id.eq(ingest_id)))
        .set((
            dsl::finished_at.eq(at.naive_utc()),
            dsl::message.eq(set_message)
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
    set_message: Option<&str>,
) -> QueryResult<()> {
    use crate::info_schema::info::ingests::dsl::*;

    diesel::update(ingests.filter(id.eq(ingest_id)))
        .set((
            aborted_at.eq(at.naive_utc()),
            message.eq(set_message),
        ))
        .execute(conn)
        .map(|_| ())
}

pub fn get_all_game_entity_ids_set(conn: &mut PgConnection) -> QueryResult<HashSet<String>> {
    use crate::data_schema::data::entities::dsl as entities_dsl;

    entities_dsl::entities
        .filter(entities_dsl::kind.eq("game"))
        .select(entities_dsl::entity_id)
        .order_by(entities_dsl::entity_id.desc())
        .load_iter::<_, DefaultLoadingMode>(conn)?
        .collect()
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

pub fn group_wither_table_results<'a>(
    games_events: impl IntoIterator<Item = &'a Vec<DbEvent>>,
    withers: Vec<DbWither>,
) -> Vec<Vec<Vec<DbWither>>> {
    let mut wither_iter = withers.into_iter().peekable();

    let results = games_events
        .into_iter()
        .map(|game_events| {
            game_events
                .iter()
                .map(|game_event| {
                    let mut children = Vec::new();
                    while let Some(child) =
                        wither_iter.next_if(|f|
                            f.game_id == game_event.game_id &&
                                f.attempt_game_event_index == game_event.game_event_index
                        ) {
                        children.push(child);
                    }
                    children
                })
                .collect_vec()
        })
        .collect_vec();

    assert_eq!(wither_iter.count(), 0);

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
    use crate::data_schema::data::failed_ejections::dsl as failed_ejection_dsl;
    use crate::data_schema::data::event_baserunners::dsl as runner_dsl;
    use crate::data_schema::data::event_fielders::dsl as fielder_dsl;
    use crate::data_schema::data::events::dsl as events_dsl;
    use crate::data_schema::data::games::dsl as games_dsl;
    use crate::data_schema::data::wither::dsl as wither_dsl;
    use crate::data_schema::data::efflorescence::dsl as efflorescence_dsl;
    use crate::data_schema::data::efflorescence_growth::dsl as efflorescence_growth_dsl;

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

    let get_failed_ejections_start = Utc::now();
    let db_failed_ejections = failed_ejection_dsl::failed_ejections
        .filter(failed_ejection_dsl::event_id.eq_any(&all_event_ids))
        .order_by(failed_ejection_dsl::event_id)
        .select(DbFailedEjection::as_select())
        .load(conn)?;
    let _get_failed_ejections_duration = (Utc::now() - get_failed_ejections_start).as_seconds_f64();

    let group_failed_ejections_start = Utc::now();
    let db_failed_ejections = group_child_table_results(&db_games_events, db_failed_ejections, |r| r.event_id);
    let _group_failed_ejections_duration = (Utc::now() - group_failed_ejections_start).as_seconds_f64();

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

    let get_efflorescences_start = Utc::now();
    let db_efflorescences = efflorescence_dsl::efflorescence
        .filter(efflorescence_dsl::event_id.eq_any(&all_event_ids))
        .order_by((efflorescence_dsl::event_id, efflorescence_dsl::efflorescence_index))
        .select(DbEfflorescence::as_select())
        .load(conn)?;
    let _get_efflorescences_duration = (Utc::now() - get_efflorescences_start).as_seconds_f64();

    let group_efflorescences_start = Utc::now();
    let db_efflorescences =
        group_child_table_results(&db_games_events, db_efflorescences, |r| r.event_id);
    let _group_efflorescences_duration = (Utc::now() - group_efflorescences_start).as_seconds_f64();

    let get_efflorescence_growths_start = Utc::now();
    let db_efflorescence_growths = efflorescence_growth_dsl::efflorescence_growth
        .filter(efflorescence_growth_dsl::event_id.eq_any(&all_event_ids))
        .order_by((
            efflorescence_growth_dsl::event_id,
            efflorescence_growth_dsl::efflorescence_index,
            efflorescence_growth_dsl::growth_index,
        ))
        .select(DbEfflorescenceGrowth::as_select())
        .load(conn)?;
    let _get_efflorescence_growths_duration = (Utc::now() - get_efflorescence_growths_start).as_seconds_f64();

    let group_efflorescence_growths_start = Utc::now();
    let db_efflorescence_growths =
        group_child_table_results(&db_games_events, db_efflorescence_growths, |r| r.event_id);
    let _group_efflorescence_growths_duration =
        (Utc::now() - group_efflorescence_growths_start).as_seconds_f64();

    let get_wither_start = Utc::now();
    let db_wither = wither_dsl::wither
        .filter(wither_dsl::game_id.eq_any(&game_ids))
        .order_by((
            wither_dsl::game_id,
            wither_dsl::attempt_game_event_index,
        ))
        .select(DbWither::as_select())
        .load(conn)?;
    let _get_wither_duration = (Utc::now() - get_wither_start).as_seconds_f64();

    let group_wither_start = Utc::now();
    let db_wither: Vec<Vec<Vec<DbWither>>> =
        group_wither_table_results(&db_games_events, db_wither);
    let _group_wither_duration =
        (Utc::now() - group_wither_start).as_seconds_f64();

    let post_process_start = Utc::now();
    let result = itertools::izip!(
        game_ids,
        db_games_events,
        db_runners,
        db_fielders,
        db_aurora_photos,
        db_ejections,
        db_failed_ejections,
        db_door_prizes,
        db_door_prize_items,
        db_efflorescences,
        db_efflorescence_growths,
        db_wither,
    )
    .map(
        |(
            game_id,
            events,
            runners,
            fielders,
            aurora_photos,
            ejections,
            failed_ejections,
            door_prizes,
            door_prize_items,
            efflorescence,
            efflorescence_growths,
            wither,
        )| {
            // Note: This should stay a vec of results. The individual results for each
            // entry are semantically meaningful.
            let detail_events = itertools::izip!(
                events,
                runners,
                fielders,
                aurora_photos,
                ejections,
                failed_ejections,
                door_prizes,
                door_prize_items,
                efflorescence,
                efflorescence_growths,
                wither,
            )
            .map(
                |(
                    event,
                    runners,
                    fielders,
                    aurora_photo,
                    ejection,
                    failed_ejection,
                    door_prizes,
                    door_prize_items,
                    efflorescence,
                    efflorescence_growths,
                    wither,
                )| {
                    to_db_format::row_to_event(
                        taxa,
                        event,
                        runners,
                        fielders,
                        aurora_photo,
                        ejection,
                        failed_ejection,
                        door_prizes,
                        door_prize_items,
                        efflorescence,
                        efflorescence_growths,
                        wither,
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
    pub withers: Vec<WitherOutcome<&'g str>>,
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
    // Like FatalError but we don't have a raw game
    DeserializeError {
        game_id: &'g str,
        from_version: DateTime<Utc>,
        error_message: String,
    },
}

impl<'g> GameForDb<'g> {
    pub fn metadata(&self) -> (&'g str, DateTime<Utc>) {
        match self {
            GameForDb::Ongoing {
                game_id,
                from_version,
                ..
            } => (*game_id, *from_version),
            GameForDb::ForeverIncomplete {
                game_id,
                from_version,
                ..
            } => (*game_id, *from_version),
            GameForDb::Completed { game, from_version } => {
                (&game.id, *from_version)
            }
            GameForDb::NotSupported {
                game_id,
                from_version,
                ..
            } => (*game_id, *from_version),
            GameForDb::FatalError {
                game_id,
                from_version,
                ..
            } => (*game_id, *from_version),
            GameForDb::DeserializeError {
                game_id,
                from_version,
                ..
            } => (*game_id, *from_version),
        }
    }

    pub fn raw_game(&self) -> Option<&'g mmolb_parsing::Game> {
        match self {
            GameForDb::Ongoing { raw_game, .. } => Some(raw_game),
            GameForDb::ForeverIncomplete { raw_game, .. } => Some(raw_game),
            GameForDb::Completed { game, .. } => Some(&game.raw_game),
            GameForDb::NotSupported { raw_game, .. } => Some(raw_game),
            GameForDb::FatalError { raw_game, .. } => Some(raw_game),
            GameForDb::DeserializeError { .. } => None,
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

fn insert_aurora_photos<'e>(
    conn: &mut PgConnection,
    taxa: &Taxa,
    event_ids_by_game: &[(i64, Vec<i64>)],
    completed_games: &[(i64, &CompletedGameForDb)],
) -> QueryResult<()> {
    let new_aurora_photos = iter::zip(event_ids_by_game, completed_games)
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
    let n_aurora_photos_inserted = diesel::copy_from(crate::schema::data_schema::data::aurora_photos::dsl::aurora_photos)
        .from_insertable(&new_aurora_photos)
        .execute(conn)?;

    log_only_assert!(
        n_aurora_photos_to_insert == n_aurora_photos_inserted,
        "Event aurora photos insert should have inserted {} rows, but it inserted {}",
        n_aurora_photos_to_insert,
        n_aurora_photos_inserted,
    );

    Ok(())
}

fn insert_ejections<'e>(
    conn: &mut PgConnection,
    taxa: &Taxa,
    event_ids_by_game: &[(i64, Vec<i64>)],
    completed_games: &[(i64, &CompletedGameForDb)],
) -> QueryResult<()> {
    let new_ejections = iter::zip(event_ids_by_game, completed_games)
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
    let n_ejections_inserted = diesel::copy_from(crate::schema::data_schema::data::ejections::dsl::ejections)
        .from_insertable(&new_ejections)
        .execute(conn)?;

    log_only_assert!(
        n_ejections_to_insert == n_ejections_inserted,
        "Event ejections insert should have inserted {} rows, but it inserted {}",
        n_ejections_to_insert,
        n_ejections_inserted,
    );

    Ok(())
}

fn insert_failed_ejections<'e>(
    conn: &mut PgConnection,
    event_ids_by_game: &[(i64, Vec<i64>)],
    completed_games: &[(i64, &CompletedGameForDb)],
) -> QueryResult<()> {
    let new_failed_ejections = iter::zip(event_ids_by_game, completed_games)
        .flat_map(|((game_id_a, event_ids), (game_id_b, game))| {
            assert_eq!(game_id_a, game_id_b);
            // Within this closure we're acting on all events in a single game
            iter::zip(event_ids, &game.events).flat_map(|(event_id, event)| {
                // Within this closure we're acting on a single event
                to_db_format::event_to_failed_ejection(*event_id, event)
            })
        })
        .collect_vec();

    let n_failed_ejections_to_insert = new_failed_ejections.len();
    let n_failed_ejections_inserted = diesel::copy_from(crate::schema::data_schema::data::failed_ejections::dsl::failed_ejections)
        .from_insertable(&new_failed_ejections)
        .execute(conn)?;

    log_only_assert!(
        n_failed_ejections_to_insert == n_failed_ejections_inserted,
        "Event failed ejections insert should have inserted {} rows, but it inserted {}",
        n_failed_ejections_to_insert,
        n_failed_ejections_inserted,
    );

    Ok(())
}

fn insert_door_prizes<'e>(
    conn: &mut PgConnection,
    event_ids_by_game: &[(i64, Vec<i64>)],
    completed_games: &[(i64, &CompletedGameForDb)],
) -> QueryResult<()> {
    let new_door_prizes = iter::zip(event_ids_by_game, completed_games)
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
    let n_door_prizes_inserted = diesel::copy_from(crate::schema::data_schema::data::door_prizes::dsl::door_prizes)
        .from_insertable(&new_door_prizes)
        .execute(conn)?;

    log_only_assert!(
        n_door_prizes_to_insert == n_door_prizes_inserted,
        "Event door prizes insert should have inserted {} rows, but it inserted {}",
        n_door_prizes_to_insert,
        n_door_prizes_inserted,
    );

    let new_door_prize_items = iter::zip(event_ids_by_game, completed_games)
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
    let n_door_prize_items_inserted = diesel::copy_from(crate::schema::data_schema::data::door_prize_items::dsl::door_prize_items)
        .from_insertable(&new_door_prize_items)
        .execute(conn)?;

    log_only_assert!(
        n_door_prize_items_to_insert == n_door_prize_items_inserted,
        "Event door prize items insert should have inserted {} rows, but it inserted {}",
        n_door_prize_items_to_insert,
        n_door_prize_items_inserted,
    );
    
    Ok(())
}

fn insert_efflorescences<'e>(
    conn: &mut PgConnection,
    taxa: &Taxa,
    event_ids_by_game: &[(i64, Vec<i64>)],
    completed_games: &[(i64, &CompletedGameForDb)],
) -> QueryResult<()> {
    let new_efflorescences = iter::zip(event_ids_by_game, completed_games)
        .flat_map(|((game_id_a, event_ids), (game_id_b, game))| {
            assert_eq!(game_id_a, game_id_b);
            // Within this closure we're acting on all events in a single game
            iter::zip(event_ids, &game.events).flat_map(|(event_id, event)| {
                // Within this closure we're acting on a single event
                to_db_format::event_to_efflorescence(*event_id, event)
            })
        })
        .collect_vec();

    let n_efflorescences_to_insert = new_efflorescences.len();
    let n_efflorescences_inserted = diesel::copy_from(crate::schema::data_schema::data::efflorescence::dsl::efflorescence)
        .from_insertable(&new_efflorescences)
        .execute(conn)?;

    log_only_assert!(
        n_efflorescences_to_insert == n_efflorescences_inserted,
        "Event efflorescences insert should have inserted {} rows, but it inserted {}",
        n_efflorescences_to_insert,
        n_efflorescences_inserted,
    );

    let new_efflorescence_growths = iter::zip(event_ids_by_game, completed_games)
        .flat_map(|((game_id_a, event_ids), (game_id_b, game))| {
            assert_eq!(game_id_a, game_id_b);
            // Within this closure we're acting on all events in a single game
            iter::zip(event_ids, &game.events).flat_map(|(event_id, event)| {
                // Within this closure we're acting on a single event
                to_db_format::event_to_efflorescence_growths(taxa, *event_id, event)
            })
        })
        .collect_vec();
    
    let n_efflorescence_items_to_insert = new_efflorescence_growths.len();
    let n_efflorescence_items_inserted = diesel::copy_from(crate::schema::data_schema::data::efflorescence_growth::dsl::efflorescence_growth)
        .from_insertable(&new_efflorescence_growths)
        .execute(conn)?;
    
    log_only_assert!(
        n_efflorescence_items_to_insert == n_efflorescence_items_inserted,
        "Event efflorescence growth insert should have inserted {} rows, but it inserted {}",
        n_efflorescence_items_to_insert,
        n_efflorescence_items_inserted,
    );
    
    Ok(())
}

fn insert_pitcher_changes<'e>(
    conn: &mut PgConnection,
    taxa: &Taxa,
    completed_games: &[(i64, &CompletedGameForDb)],
) -> QueryResult<()> {
    let new_pitcher_changes: Vec<_> = completed_games
        .iter()
        .flat_map(|(game_id, game)| {
            game.pitcher_changes.iter().map(|pitcher_change| {
                to_db_format::pitcher_change_to_row(taxa, *game_id, pitcher_change)
            })
        })
        .collect();

    let n_pitcher_changes_to_insert = new_pitcher_changes.len();
    let n_pitcher_changes_inserted = diesel::copy_from(crate::schema::data_schema::data::pitcher_changes::dsl::pitcher_changes)
        .from_insertable(&new_pitcher_changes)
        .execute(conn)?;

    log_only_assert!(
        n_pitcher_changes_to_insert == n_pitcher_changes_inserted,
        "pitcher_changes insert should have inserted {} rows, but it inserted {}",
        n_pitcher_changes_to_insert,
        n_pitcher_changes_inserted,
    );

    Ok(())
}

fn insert_parties<'e>(
    conn: &mut PgConnection,
    taxa: &Taxa,
    completed_games: &[(i64, &CompletedGameForDb)],
) -> QueryResult<()> {
    let new_parties: Vec<_> = completed_games
        .iter()
        .flat_map(|(game_id, game)| {
            game.parties
                .iter()
                .flat_map(|party| to_db_format::party_to_rows(taxa, *game_id, party))
        })
        .collect();

    let n_parties_to_insert = new_parties.len();
    let n_parties_inserted = diesel::copy_from(crate::schema::data_schema::data::parties::dsl::parties)
        .from_insertable(&new_parties)
        .execute(conn)?;

    log_only_assert!(
        n_parties_to_insert == n_parties_inserted,
        "parties insert should have inserted {} rows, but it inserted {}",
        n_parties_to_insert,
        n_parties_inserted,
    );

    Ok(())
}

fn insert_withers<'e>(
    conn: &mut PgConnection,
    taxa: &Taxa,
    completed_games: &[(i64, &CompletedGameForDb)],
) -> QueryResult<()> {
    let new_withers: Vec<_> = completed_games
        .iter()
        .flat_map(|(game_id, game)| {
            game.withers
                .iter()
                .map(|wither| to_db_format::wither_to_rows(taxa, *game_id, wither))
        })
        .collect();

    let n_withers_to_insert = new_withers.len();
    let n_withers_inserted = diesel::copy_from(crate::schema::data_schema::data::wither::dsl::wither)
        .from_insertable(&new_withers)
        .execute(conn)?;

    log_only_assert!(
        n_withers_to_insert == n_withers_inserted,
        "withers insert should have inserted {} rows, but it inserted {}",
        n_withers_to_insert,
        n_withers_inserted,
    );

    Ok(())
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

    // First delete all games. If particular debug settings are turned on this may happen for every
    // game, but even in release mode we may need to delete partial games and replace them with
    // full games.
    let delete_old_games_start = Utc::now();
    let game_mmolb_ids = games
        .iter()
        .map(GameForDb::metadata)
        .map(|(id, _)| id)
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
            let (game_id, from_version) = game.metadata();
            let Some(raw_game) = game.raw_game() else {
                // TODO Is there a more elegant solution than a defaulted game?
                // Get an arbitrary weather. This is a bad solution.
                let weather = weather_table.values()
                    .next()
                    .cloned()
                    // This unwrap is pretty much guaranteed to lead to a db constraint error.
                    // It's here to make the code compile while I hope it's never hit.
                    .unwrap_or(0);
                return NewGame {
                    ingest: ingest_id,
                    mmolb_game_id: game_id,
                    weather,
                    season: 0,
                    day: Some(0), // db constraints enforce one of (day, superstar_day) is not None
                    superstar_day: None,
                    away_team_emoji: "",
                    away_team_name: "",
                    away_team_mmolb_id: "",
                    away_team_final_score: None,
                    home_team_emoji: "",
                    home_team_name: "",
                    home_team_mmolb_id: "",
                    home_team_final_score: None,
                    is_ongoing: false,
                    stadium_name: None,
                    from_version: from_version.naive_utc(),
                    home_team_earned_coins: None,
                    away_team_earned_coins: None,
                    home_team_photo_contest_top_scorer: None,
                    home_team_photo_contest_score: None,
                    away_team_photo_contest_top_scorer: None,
                    away_team_photo_contest_score: None,
                }
            };

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
            GameForDb::DeserializeError { error_message, .. } => {
                Some(Either::Right(NewEventIngestLog {
                    game_id: *game_id,
                    game_event_index: None, // None => applies to the entire game
                    log_index: 0,           // there's only ever one (this overrides FatalError)
                    log_level: 0,           // critical
                    log_text: error_message,
                }))
            }
        })
        .partition_map(|x| x);

    let insert_games_duration = (Utc::now() - insert_games_start).as_seconds_f64();

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
    insert_aurora_photos(conn, taxa, &event_ids_by_game, &completed_games)?;
    let _insert_aurora_photos_duration = (Utc::now() - insert_aurora_photos_start).as_seconds_f64();

    let insert_ejections_start = Utc::now();
    insert_ejections(conn, taxa, &event_ids_by_game, &completed_games)?;
    insert_failed_ejections(conn, &event_ids_by_game, &completed_games)?;
    let _insert_ejections_duration = (Utc::now() - insert_ejections_start).as_seconds_f64();

    let insert_door_prizes_start = Utc::now();
    insert_door_prizes(conn, &event_ids_by_game, &completed_games)?;
    let _insert_door_prizes_duration =
        (Utc::now() - insert_door_prizes_start).as_seconds_f64();

    let insert_efflorescences_start = Utc::now();
    insert_efflorescences(conn, taxa, &event_ids_by_game, &completed_games)?;
    let _insert_efflorescences_duration =
        (Utc::now() - insert_efflorescences_start).as_seconds_f64();

    // These ones are not attached to a DbEvent
    let insert_pitcher_changes_start = Utc::now();
    insert_pitcher_changes(conn, taxa, &completed_games)?;
    let _insert_pitcher_changes_duration =
        (Utc::now() - insert_pitcher_changes_start).as_seconds_f64();

    let insert_parties_start = Utc::now();
    insert_parties(conn, taxa, &completed_games)?;
    let _insert_parties_duration = (Utc::now() - insert_parties_start).as_seconds_f64();

    let insert_withers_start = Utc::now();
    insert_withers(conn, taxa, &completed_games)?;
    let _insert_withers_duration = (Utc::now() - insert_withers_start).as_seconds_f64();

    Ok(InsertGamesTimings {
        delete_old_games_duration,
        update_weather_table_duration,
        insert_games_duration,
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
    pub raw_events_with_logs: Vec<(mmolb_parsing::game::Event, Vec<DbEventIngestLog>)>,
}

#[derive(Debug, Error)]
pub enum QueryDeserializeError {
    #[error(transparent)]
    Query(#[from] diesel::result::Error),

    #[error(transparent)]
    Deserialize(#[from] serde_json::error::Error),
}

pub fn game_and_raw_events(
    conn: &mut PgConnection,
    mmolb_game_id: &str,
) -> Result<DbFullGameWithLogs, QueryDeserializeError> {
    use crate::data_schema::data::games::dsl as games_dsl;
    use crate::info_schema::info::event_ingest_log::dsl as event_ingest_log_dsl;
    use crate::data_schema::data::entities::dsl as entities_dsl;

    let game = games_dsl::games
        .filter(games_dsl::mmolb_game_id.eq(mmolb_game_id))
        .select(DbGame::as_select())
        .get_result::<DbGame>(conn)?;

    let raw_game = entities_dsl::entities
        .filter(entities_dsl::entity_id.eq(mmolb_game_id))
        .select(entities_dsl::data)
        .get_result::<serde_json::Value>(conn)?;

    let raw_game: mmolb_parsing::Game = serde_json::from_value(raw_game)?;

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

    let logs_by_event = raw_game
        .event_log
        .iter()
        .enumerate()
        .map(|(game_event_index, _)| {
            let mut events = Vec::new();
            while let Some(event) =
                raw_logs.next_if(|log| log.game_event_index.expect("All logs with a None game_event_index should have been extracted before this loop began") == game_event_index as i32)
            {
                events.push(event);
            }
            events
        })
        .collect_vec();

    assert!(raw_logs.next().is_none(), "Failed to map all raw logs");

    let raw_events_with_logs = raw_game.event_log
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

    // This must list all tables that have a valid_from derived from the `team` kind.
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
    NewFeedEventProcessed<'a>,
    Option<NewPlayerAttributeAugment<'a>>,
    Option<NewPlayerParadigmShift<'a>>,
    Vec<NewPlayerRecomposition<'a>>,
    Vec<NewVersionIngestLog<'a>>,
);

type NewPlayerVersionExt<'a> = (
    NewPlayerVersion<'a>,
    Vec<NewPlayerModificationVersion<'a>>,
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
    new_player_report_attribute_versions: Vec<&Vec<NewPlayerReportAttributeVersion>>,
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
        &Vec<(NewPlayerReportVersion, Vec<NewPlayerReportAttributeVersion>)>,
    >,
) -> QueryResult<usize> {
    use crate::data_schema::data::player_report_versions::dsl as prv_dsl;

    // Flatten and convert reference to tuple to tuple of references
    let new_player_report_versions = new_player_report_versions.into_iter()
        .flatten()
        .map(|(a, b)| (a, b));

    let (new_player_report_versions, new_player_report_attribute_versions): (
        Vec<&NewPlayerReportVersion>,
        Vec<&Vec<NewPlayerReportAttributeVersion>>,
    ) = itertools::multiunzip(new_player_report_versions);

    // Insert new records
    let num_inserted = diesel::copy_from(prv_dsl::player_report_versions)
        .from_insertable(new_player_report_versions)
        .execute(conn)?;

    insert_player_report_attribute_versions(conn, new_player_report_attribute_versions)?;

    Ok(num_inserted)
}

fn insert_player_equipment_effects(
    conn: &mut PgConnection,
    new_player_equipment_effects: Vec<&Vec<NewPlayerEquipmentEffectVersion>>,
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
        &Vec<(
            NewPlayerEquipmentVersion,
            Vec<NewPlayerEquipmentEffectVersion>,
        )>,
    >,
) -> QueryResult<usize> {
    use crate::data_schema::data::player_equipment_versions::dsl as pev_dsl;

    // Flatten and convert reference to tuple to tuple of references
    let new_player_equipment = new_player_equipment.into_iter()
        .flatten()
        .map(|(a, b)| (a, b));

    let (new_player_equipment_versions, new_player_equipment_effect_versions): (
        Vec<&NewPlayerEquipmentVersion>,
        Vec<&Vec<NewPlayerEquipmentEffectVersion>>,
    ) = itertools::multiunzip(new_player_equipment);

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

pub fn insert_to_error<'container, InsertableT: 'container>(
    f: impl Fn(&mut PgConnection, &'container [InsertableT]) -> QueryResult<usize> + Copy,
    conn: &mut PgConnection,
    items: &'container [InsertableT],
) -> Result<usize, (usize, QueryError)> {
    insert_to_error_internal(f, conn, items, 0)
}

fn insert_to_error_internal<'container, InsertableT: 'container>(
    f: impl Fn(&mut PgConnection, &'container [InsertableT]) -> QueryResult<usize> + Copy,
    conn: &mut PgConnection,
    items: &'container [InsertableT],
    previously_inserted: usize
) -> Result<usize, (usize, QueryError)> {
    if items.len() <= 1 {
        return match f(conn, items) {
            Ok(newly_inserted) => Ok(previously_inserted + newly_inserted),
            Err(err) => Err((previously_inserted, err)),
        };
    }

    match f(conn, items) {
        Ok(count) => Ok(count),
        Err(QueryError::DatabaseError(diesel::result::DatabaseErrorKind::UniqueViolation, _)) |
        Err(QueryError::DatabaseError(diesel::result::DatabaseErrorKind::ForeignKeyViolation, _)) |
        Err(QueryError::DatabaseError(diesel::result::DatabaseErrorKind::NotNullViolation, _)) |
        Err(QueryError::DatabaseError(diesel::result::DatabaseErrorKind::CheckViolation, _)) => {
            let halfway = items.len() / 2;
            debug!(
                "Constraint violation error inserting {} items. Trying again with the first {}.",
                items.len(), halfway,
            );
            let inserted_first_half = insert_to_error_internal(f, conn, &items[..halfway], previously_inserted)
                .map_err(|(inserted, err)| (previously_inserted + inserted, err))?;

            debug!(
                "No error in the first {} items. Trying the next {}.",
                halfway, items.len() - halfway
            );
            insert_to_error_internal(f, conn, &items[halfway..], previously_inserted)
                .map_err(|(inserted, err)| (previously_inserted + inserted_first_half + inserted, err))
        }
        Err(err) => Err((previously_inserted, err)),
    }
}

pub fn insert_player_feed_versions<'container, 'game: 'container>(
    conn: &mut PgConnection,
    new_player_feed_versions: impl IntoIterator<Item = &'container NewPlayerFeedVersionExt<'game>>,
) -> QueryResult<usize> {
    // Convert reference to tuple into tuple of references
    let new_player_feed_versions = new_player_feed_versions.into_iter()
        .map(|(a, b, c, d, e)| (a, b, c, d, e));

    let (
        new_player_feed_events_processed,
        new_player_attribute_augments,
        new_player_paradigm_shifts,
        new_player_recompositions,
        ingest_logs,
    ): (
        Vec<&NewFeedEventProcessed>,
        Vec<&Option<NewPlayerAttributeAugment>>,
        Vec<&Option<NewPlayerParadigmShift>>,
        Vec<&Vec<NewPlayerRecomposition>>,
        Vec<&Vec<NewVersionIngestLog>>,
    ) = itertools::multiunzip(new_player_feed_versions);

    // Insert new records
    insert_player_attribute_augments(conn, new_player_attribute_augments)?;
    insert_player_paradigm_shifts(conn, new_player_paradigm_shifts)?;
    insert_player_recompositions(conn, new_player_recompositions)?;
    insert_nested_ingest_logs(conn, ingest_logs)?;

    // This is last so that we don't mark them as processed if there were db errors
    let num_inserted = insert_feed_events_processed(conn, new_player_feed_events_processed)?;

    Ok(num_inserted)
}

type NewTeamFeedVersionExt<'a> = (
    NewFeedEventProcessed<'a>,
    Option<NewTeamGamePlayed<'a>>,
    Vec<NewVersionIngestLog<'a>>,
);

fn insert_new_team_games_played(
    conn: &mut PgConnection,
    new_team_games_played: Vec<&Option<NewTeamGamePlayed>>,
) -> QueryResult<usize> {
    use crate::data_schema::data::team_games_played::dsl as get_dsl;
    let new_team_games_played = new_team_games_played.into_iter().flatten().collect_vec();

    // Insert new records
    diesel::copy_from(get_dsl::team_games_played)
        .from_insertable(new_team_games_played)
        .execute(conn)
}

fn insert_feed_events_processed(
    conn: &mut PgConnection,
    new_feed_events_processed: Vec<&NewFeedEventProcessed>,
) -> QueryResult<usize> {
    use crate::data_schema::data::feed_events_processed::dsl as fep_dsl;

    // Insert new records
    diesel::insert_into(fep_dsl::feed_events_processed)
        .values(new_feed_events_processed)
        // Because of the buffers between the coordinator and the workers, we
        // may process the same record multiple times. Every table handles
        // that itself, so we just need to not error on inserting this one.
        .on_conflict_do_nothing()
        .execute(conn)
}

pub fn insert_team_feed_versions<'container, 'game: 'container>(
    conn: &mut PgConnection,
    new_team_feed_versions: impl IntoIterator<Item = &'container NewTeamFeedVersionExt<'game>>,
) -> QueryResult<usize> {
    let new_team_feed_versions = new_team_feed_versions.into_iter()
        .map(|(a, b, c)| (a, b, c));

    let (
        new_team_feed_events_processed,
        new_team_games_played,
        ingest_logs,
    ): (
        Vec<&NewFeedEventProcessed>,
        Vec<&Option<NewTeamGamePlayed>>,
        Vec<&Vec<NewVersionIngestLog>>,
    ) = itertools::multiunzip(new_team_feed_versions);

    // Insert new records
    insert_new_team_games_played(conn, new_team_games_played)?;
    insert_nested_ingest_logs(conn, ingest_logs)?;

    // This is last so that we don't mark them as processed if there were db errors
    let num_inserted = insert_feed_events_processed(conn, new_team_feed_events_processed)?;

    Ok(num_inserted)
}

fn insert_player_recompositions(
    conn: &mut PgConnection,
    new_player_recompositions: Vec<&Vec<NewPlayerRecomposition>>,
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

pub fn insert_ingest_logs(
    conn: &mut PgConnection,
    new_logs: Vec<NewVersionIngestLog>,
) -> QueryResult<usize> {
    use crate::info_schema::info::version_ingest_log::dsl as vil_dsl;

    // Insert new records
    diesel::copy_from(vil_dsl::version_ingest_log)
        .from_insertable(new_logs)
        .execute(conn)
}

fn insert_nested_ingest_logs(
    conn: &mut PgConnection,
    new_logs: Vec<&Vec<NewVersionIngestLog>>,
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
    new_player_paradigm_shifts: Vec<&Option<NewPlayerParadigmShift>>,
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
    new_player_augments: Vec<&Option<NewPlayerAttributeAugment>>,
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
    new_player_modification_versions: Vec<&Vec<NewPlayerModificationVersion>>,
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

pub fn insert_player_versions<'container, 'game: 'container>(
    conn: &mut PgConnection,
    new_player_versions: impl IntoIterator<Item = &'container NewPlayerVersionExt<'game>>,
) -> QueryResult<usize> {
    use crate::data_schema::data::player_versions::dsl as pv_dsl;

    // Reference to tuple into tuple of references
    let new_player_versions = new_player_versions.into_iter()
        .map(|(a, b, c, d, e)| (a, b, c, d, e));

    let preprocess_start = Utc::now();
    let (
        new_player_versions,
        new_player_modification_versions,
        new_player_report_attributes,
        new_player_equipment,
        new_ingest_logs,
    ): (
        Vec<&NewPlayerVersion>,
        Vec<&Vec<NewPlayerModificationVersion>>,
        Vec<&Vec<(NewPlayerReportVersion, Vec<NewPlayerReportAttributeVersion>)>>,
        Vec<
            &Vec<(
                NewPlayerEquipmentVersion,
                Vec<NewPlayerEquipmentEffectVersion>,
            )>,
        >,
        Vec<&Vec<NewVersionIngestLog>>,
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

    let insert_player_reports_start = Utc::now();
    insert_player_report_versions(conn, new_player_report_attributes)?;
    let insert_player_reports_duration =
        (Utc::now() - insert_player_reports_start).as_seconds_f64();

    let insert_player_equipment_start = Utc::now();
    insert_player_equipment(conn, new_player_equipment)?;
    let insert_player_equipment_duration =
        (Utc::now() - insert_player_equipment_start).as_seconds_f64();

    let insert_ingest_logs_start = Utc::now();
    insert_nested_ingest_logs(conn, new_ingest_logs)?;
    let insert_ingest_logs_duration = (Utc::now() - insert_ingest_logs_start).as_seconds_f64();

    info!(
        "preprocess_duration: {preprocess_duration:.2}, \
        insert_player_equipment_duration: {insert_player_equipment_duration:.2}, \
        insert_player_reports_duration: {insert_player_reports_duration:.2}, \
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

pub fn get_player_report_versions(
    conn: &mut PgConnection,
    player_id: &str,
) -> QueryResult<Vec<DbPlayerReportVersion>> {
    use crate::data_schema::data::player_report_versions::dsl as prv_dsl;

    prv_dsl::player_report_versions
        .filter(prv_dsl::mmolb_player_id.eq(player_id))
        .order(prv_dsl::valid_from.asc())
        .select(DbPlayerReportVersion::as_select())
        .get_results(conn)
}

pub fn get_player_report_attribute_versions(
    conn: &mut PgConnection,
    player_id: &str,
) -> QueryResult<Vec<DbPlayerReportAttributeVersion>> {
    use crate::data_schema::data::player_report_attribute_versions::dsl as prav_dsl;

    prav_dsl::player_report_attribute_versions
        .filter(prav_dsl::mmolb_player_id.eq(player_id))
        .order(prav_dsl::valid_from.asc())
        .select(DbPlayerReportAttributeVersion::as_select())
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

pub fn get_player_recompositions(
    conn: &mut PgConnection,
    player_id: &str,
) -> QueryResult<Vec<DbPlayerRecomposition>> {
    use crate::data_schema::data::player_recompositions::dsl as pr_dsl;

    pr_dsl::player_recompositions
        .filter(pr_dsl::mmolb_player_id.eq(player_id))
        .order((pr_dsl::feed_event_index.asc(), pr_dsl::inferred_event_index.asc().nulls_last()))
        .select(DbPlayerRecomposition::as_select())
        .get_results(conn)
}

pub fn get_player_attribute_augments(
    conn: &mut PgConnection,
    player_id: &str,
) -> QueryResult<Vec<DbPlayerAttributeAugment>> {
    use crate::data_schema::data::player_attribute_augments::dsl as paa_dsl;

    paa_dsl::player_attribute_augments
        .filter(paa_dsl::mmolb_player_id.eq(player_id))
        .order(paa_dsl::feed_event_index.asc())
        .select(DbPlayerAttributeAugment::as_select())
        .get_results(conn)
}

#[derive(QueryableByName)]
pub struct DbPlayerParty {
    #[diesel(sql_type = Text)]
    pub mmolb_game_id: String,
    #[diesel(sql_type = Timestamp)]
    pub game_start_time: NaiveDateTime,
    #[diesel(sql_type = Timestamp)]
    pub game_end_time: NaiveDateTime,
    #[diesel(sql_type = Int8)]
    pub attribute: i64,
    #[diesel(sql_type = Integer)]
    pub value: i32,
}

pub fn get_player_parties(
    conn: &mut PgConnection,
    player_id: &str,
) -> QueryResult<Vec<DbPlayerParty>> {
    let q = sql_query("
        with game_end_times as (
            select min(tgp.time) as time, mmolb_game_id
            from data.team_games_played tgp
            group by mmolb_game_id
        ), parties_extended as (select
            g.mmolb_game_id,
            case when p.is_pitcher=p.top_of_inning then g.home_team_mmolb_id else g.away_team_mmolb_id end as mmolb_team_id,
            p.player_name,
            p.attribute,
            p.value
        from data.parties p
        left join data.games g on g.id=p.game_id)
        select
            pe.mmolb_game_id,
            to_timestamp(('0x'||substr(pe.mmolb_game_id, 1, 8))::numeric) as game_start_time,
            gt.time as game_end_time,
            pe.attribute,
            pe.value
        from parties_extended pe
        left join game_end_times gt on gt.mmolb_game_id=pe.mmolb_game_id
        left join data.team_player_versions tpv on pe.mmolb_team_id=tpv.mmolb_team_id
            and pe.player_name=(tpv.first_name || ' ' || tpv.last_name)
            and (gt.time >= tpv.valid_from and gt.time < coalesce(tpv.valid_until, 'infinity'))
        where tpv.mmolb_player_id=$1
        order by game_start_time asc
    ");

    q
        .bind::<Text, _>(player_id)
        .get_results(conn)
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

fn filter_fielder<'q>(fielder_name: &'q str, team_id: &'q str) -> PlayerFilter<'q, event_dsl::fair_ball_fielder_name> {
    event_dsl::fair_ball_fielder_name.eq(fielder_name)
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
    new_team_player_versions: Vec<&Vec<NewTeamPlayerVersion>>,
) -> QueryResult<usize> {
    use crate::data_schema::data::team_player_versions::dsl as tpv_dsl;

    let new_team_player_versions = new_team_player_versions.into_iter().flatten().collect_vec();

    // Insert new records
    diesel::copy_from(tpv_dsl::team_player_versions)
        .from_insertable(new_team_player_versions)
        .execute(conn)
}

pub fn insert_team_versions<'container, 'game: 'container>(
    conn: &mut PgConnection,
    new_team_versions: impl IntoIterator<Item = &'container NewTeamVersionExt<'game>>,
) -> QueryResult<usize> {
    use crate::data_schema::data::team_versions::dsl as tv_dsl;

    // Convert reference to tuple to tuple of references
    let new_team_versions = new_team_versions.into_iter()
        .map(|(a, b, c)| (a, b, c));

    let (new_team_versions, new_team_player_versions, new_ingest_logs): (
        Vec<&NewTeamVersion>,
        Vec<&Vec<NewTeamPlayerVersion>>,
        Vec<&Vec<NewVersionIngestLog>>,
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
    insert_nested_ingest_logs(conn, new_ingest_logs)?;
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

pub fn refresh_entity_counting_matviews(conn: &mut PgConnection) -> QueryResult<()> {
    debug!("Updating info.entities_count");
    sql_query("refresh materialized view info.entities_count").execute(conn)?;
    debug!("Updating info.entities_with_issues_count");
    sql_query("refresh materialized view info.entities_with_issues_count").execute(conn)?;

    Ok(())
}

pub fn refresh_matviews(conn: &mut PgConnection) -> QueryResult<()> {
    refresh_entity_counting_matviews(conn)?;
    debug!("Updating data.offense_outcomes");
    sql_query("refresh materialized view data.offense_outcomes").execute(conn)?;
    debug!("Updating data.defense_outcomes");
    sql_query("refresh materialized view data.defense_outcomes").execute(conn)?;
    debug!("Updating data.player_versions_extended");
    sql_query("refresh materialized view data.player_versions_extended").execute(conn)?;

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

#[derive(QueryableByName, PartialEq, Debug)]
pub struct SummaryStat {
    #[diesel(sql_type = BigInt)]
    pub count: i64,
    #[diesel(sql_type = BigInt)]
    pub event_type: i64,
    #[diesel(sql_type = Nullable<BigInt>)]
    pub fair_ball_direction: Option<i64>,
}

pub fn season_averages(
    conn: &mut PgConnection,
    season: Option<i32>,
) -> QueryResult<Vec<SummaryStat>> {
    if let Some(season) = season {
        let q = sql_query("\
            select coalesce(sum(count)::int8, 0) as count, event_type, fair_ball_direction
            from data.offense_outcomes
            where season=$1
            group by event_type, fair_ball_direction
            order by event_type, fair_ball_direction
        ")
            .bind::<Integer, _>(season);
        println!("{:?}", diesel::debug_query(&q));
        q
            .get_results::<SummaryStat>(conn)
    } else {
        sql_query("\
            select coalesce(sum(count)::int8, 0) as count, event_type, fair_ball_direction
            from data.offense_outcomes
            group by event_type, fair_ball_direction
            order by event_type, fair_ball_direction
        ")
            .get_results::<SummaryStat>(conn)
    }
}

pub fn latest_game(conn: &mut PgConnection) -> QueryResult<Option<(NaiveDateTime, i32, Option<i32>, Option<i32>)>> {
    use crate::data_schema::data::team_games_played::dsl as tgp_dsl;

    game_dsl::games
        .inner_join(tgp_dsl::team_games_played.on(tgp_dsl::mmolb_game_id.eq(game_dsl::mmolb_game_id)))
        .filter(game_dsl::is_ongoing.eq(false))
        .order_by(tgp_dsl::time.desc())
        .select((tgp_dsl::time, game_dsl::season, game_dsl::day, game_dsl::superstar_day))
        .get_result(conn)
        .optional()
}

#[derive(QueryableByName)]
pub struct PitchSpeedRecord {
    #[diesel(sql_type = Text)]
    pub mmolb_team_id: String,
    #[diesel(sql_type = Text)]
    pub team_emoji: String,
    #[diesel(sql_type = Text)]
    pub team_location: String,
    #[diesel(sql_type = Text)]
    pub team_name: String,
    #[diesel(sql_type = Text)]
    pub mmolb_player_id: String,
    #[diesel(sql_type = Text)]
    pub player_name: String,
    #[diesel(sql_type = Text)]
    pub mmolb_game_id: String,
    #[diesel(sql_type = Integer)]
    pub game_event_index: i32,
    #[diesel(sql_type = Float8)]
    pub pitch_speed: f64,
}

pub fn fastest_pitch(conn: &mut PgConnection) -> QueryResult<Option<PitchSpeedRecord>> {
    sql_query("
        select
            tv.mmolb_team_id,
            tv.emoji as team_emoji,
            tv.location as team_location,
            tv.name as team_name,
            tpv.mmolb_player_id,
            tpv.first_name || ' ' || tpv.last_name as player_name,
            ee.mmolb_game_id,
            ee.game_event_index,
            ee.pitch_speed
        from data.events_extended ee
        inner join data.team_player_versions tpv on tpv.mmolb_team_id=ee.defending_team_mmolb_id
            and tpv.first_name || ' ' || tpv.last_name=ee.pitcher_name
            and tpv.valid_from <= ee.game_end_time and ee.game_end_time < coalesce(tpv.valid_until, 'infinity')
        -- I'm intentionally selecting the latest team version, rather than the one from when the record
        -- was set, because I want to get the latest team name and emoji
        inner join data.team_versions tv on tv.mmolb_team_id=ee.defending_team_mmolb_id
            and tv.valid_until is null
        where ee.pitch_speed is not null
        -- Select highest pitch speed, and in case of ties, select earliest game id
        -- This will get the earliest record setter unless the record was broken multiple times in the same day
        order by ee.pitch_speed desc, ee.mmolb_game_id asc, ee.game_event_index asc
        limit 1
    ").get_result(conn).optional()
}

#[derive(QueryableByName)]
pub struct MostPitchesInGameRecord {
    #[diesel(sql_type = Text)]
    pub mmolb_team_id: String,
    #[diesel(sql_type = Text)]
    pub team_emoji: String,
    #[diesel(sql_type = Text)]
    pub team_location: String,
    #[diesel(sql_type = Text)]
    pub team_name: String,
    #[diesel(sql_type = Text)]
    pub mmolb_player_id: String,
    #[diesel(sql_type = Text)]
    pub player_name: String,
    #[diesel(sql_type = Text)]
    pub mmolb_game_id: String,
    #[diesel(sql_type = BigInt)]
    pub num_pitch_like_events: i64,
}

pub fn most_pitches_by_player_in_one_game(conn: &mut PgConnection) -> QueryResult<Option<MostPitchesInGameRecord>> {
    sql_query("
        with counts as (
            select
                count(1) as num_pitch_like_events,
                ee.mmolb_game_id,
                ee.defending_team_mmolb_id,
                ee.pitcher_name,
                ee.game_end_time
            from data.events_extended ee
            -- Also group on pitcher_count in the unlikely event a pitcher is replaced with a
            -- pitcher of the same name. This doesn't catch the possibility that a player is ejected
            -- and replaced with a same-name pitcher, but the DB doesn't make that easy at the moment.
            group by ee.mmolb_game_id, ee.defending_team_mmolb_id, ee.pitcher_name, ee.pitcher_count, ee.game_end_time
            order by count(1) desc, ee.mmolb_game_id asc
            limit 1
        )
        select
            tv.mmolb_team_id,
            tv.emoji as team_emoji,
            tv.location as team_location,
            tv.name as team_name,
            tpv.mmolb_player_id,
            tpv.first_name || ' ' || tpv.last_name as player_name,
            c.mmolb_game_id,
            c.num_pitch_like_events
        from counts c
        inner join data.team_player_versions tpv on tpv.mmolb_team_id=c.defending_team_mmolb_id
            and tpv.first_name || ' ' || tpv.last_name=c.pitcher_name
            and tpv.valid_from <= c.game_end_time and c.game_end_time < coalesce(tpv.valid_until, 'infinity')
        -- I'm intentionally selecting the latest team version, rather than the one from when the record
        -- was set, because I want to get the latest team name and emoji
        inner join data.team_versions tv on tv.mmolb_team_id=c.defending_team_mmolb_id
            and tv.valid_until is null
    ").get_result(conn).optional()
}

pub fn highest_scoring_game(conn: &mut PgConnection) -> QueryResult<Option<DbGame>> {
    sql_query("
        select *
        from data.games g
        where g.away_team_final_score is not null
            and g.home_team_final_score is not null
        order by g.away_team_final_score + g.home_team_final_score desc,
            g.mmolb_game_id asc
        limit 1
    ").get_result(conn).optional()
}

pub fn highest_score_in_a_game(conn: &mut PgConnection) -> QueryResult<Option<DbGame>> {
    sql_query("
        select *
        from data.games g
        where g.away_team_final_score is not null
            and g.home_team_final_score is not null
        order by greatest(g.away_team_final_score, g.home_team_final_score) desc,
            g.mmolb_game_id asc
        limit 1
    ").get_result(conn).optional()
}



#[derive(QueryableByName)]
pub struct GameWithCount {
    #[diesel(sql_type = Text)]
    pub away_team_mmolb_id: String,
    #[diesel(sql_type = Text)]
    pub away_team_emoji: String,
    #[diesel(sql_type = Text)]
    pub away_team_name: String,
    #[diesel(sql_type = Text)]
    pub home_team_mmolb_id: String,
    #[diesel(sql_type = Text)]
    pub home_team_emoji: String,
    #[diesel(sql_type = Text)]
    pub home_team_name: String,
    #[diesel(sql_type = Text)]
    pub mmolb_game_id: String,
    #[diesel(sql_type = BigInt)]
    pub count: i64,
}

pub fn longest_game_by_events(conn: &mut PgConnection) -> QueryResult<Option<GameWithCount>> {
    sql_query("
        select
            (select count(1) from data.events e where e.game_id=g.id) as count,
            g.*
        from data.games g
        order by count desc, g.mmolb_game_id asc
        limit 1
    ").get_result(conn).optional()
}

pub fn longest_game_by_innings(conn: &mut PgConnection) -> QueryResult<Option<GameWithCount>> {
    sql_query("
        select
            e.inning::bigint as count,
            g.*
        from data.events e
        left join data.games g on g.id=e.game_id
        order by count desc, g.mmolb_game_id asc
        limit 1
    ").get_result(conn).optional()
}

#[derive(QueryableByName)]
pub struct DbPlayerIdentityWithValue {
    #[diesel(sql_type = Text)]
    pub mmolb_team_id: String,
    #[diesel(sql_type = Text)]
    pub team_emoji: String,
    #[diesel(sql_type = Text)]
    pub team_location: String,
    #[diesel(sql_type = Text)]
    #[diesel(sql_type = Text)]
    pub team_name: String,
    #[diesel(sql_type = Text)]
    pub mmolb_player_id: String,
    #[diesel(sql_type = Text)]
    pub player_name: String,
    #[diesel(sql_type = Float8)]
    pub value: f64,
}

// TODO Consider excluding the period during season 6 where values were fluctuating a lot
pub fn highest_reported_attribute(conn: &mut PgConnection, attr_name: &str) -> QueryResult<Option<DbPlayerIdentityWithValue>> {
    sql_query("
        select
            tv.mmolb_team_id,
            tv.emoji as team_emoji,
            tv.location as team_location,
            tv.name as team_name,
            pv.mmolb_player_id,
            pv.first_name || ' ' || pv.last_name as player_name,
            prav.modified_total as value
        from data.player_report_attribute_versions prav
        inner join data.player_versions pv on pv.mmolb_player_id=prav.mmolb_player_id
            and prav.valid_from >= pv.valid_from and prav.valid_from < coalesce(pv.valid_until, 'infinity')
        -- intentionally getting the latest team version
        inner join data.team_versions tv on tv.mmolb_team_id=pv.mmolb_team_id
            and tv.valid_until is null
        inner join taxa.attribute a on a.id=prav.attribute
        where a.name=$1 and prav.modified_total is not null
        order by prav.modified_total desc, prav.valid_from asc
        limit 1
    ").bind::<Text, _>(attr_name).get_result(conn).optional()
}