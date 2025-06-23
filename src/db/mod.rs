// Philosophically, I would like this module to be decoupled from Rocket. But
// Rocket does some magic to kinda-sorta merge diesel and diesel-async, so I'm
// not sure that will be possible.

mod taxa;
mod to_db_format;
mod taxa_macro;

pub use crate::db::taxa::{
    Taxa, TaxaBase, TaxaBaseDescriptionFormat, TaxaBaseWithDescriptionFormat, TaxaEventType,
    TaxaFairBallType, TaxaFieldingErrorType, TaxaHitType, TaxaPitchType, TaxaPosition,
};
use std::collections::HashMap;

use crate::ingest::{EventDetail, MinimalGameState, IngestLog};
use crate::models::{
    DbEvent, DbEventIngestLog, DbFielder, DbGame, DbRawEvent, DbRunner, Ingest,
    NewEventIngestLogOwning, NewGame, NewGameIngestTimings, NewIngest, NewRawEvent,
};
use chrono::{DateTime, NaiveDateTime, Utc};
use itertools::PeekingNext;
use rocket_db_pools::diesel::scoped_futures::ScopedFutureExt;
use rocket_db_pools::{diesel::AsyncPgConnection, diesel::prelude::*};
pub use to_db_format::RowToEventError;

pub async fn ingest_count(conn: &mut AsyncPgConnection) -> QueryResult<i64> {
    use crate::data_schema::data::ingests::dsl::*;

    ingests.count().get_result(conn).await
}

pub async fn game_count(conn: &mut AsyncPgConnection) -> QueryResult<i64> {
    use crate::data_schema::data::games::dsl::*;

    games.count().get_result(conn).await
}

pub async fn game_with_issues_count(conn: &mut AsyncPgConnection) -> QueryResult<i64> {
    use diesel::sql_types::*;
    #[derive(QueryableByName)]
    struct GamesWithIssuesCount {
        #[diesel(sql_type = Int8)]
        pub games_with_issues: i64,
    }

    diesel::sql_query(
        "
        select count(distinct data.games.id) games_with_issues
        from data.games
        join info.raw_events on data.games.id = info.raw_events.game_id
        join info.event_ingest_log on info.raw_events.id = info.event_ingest_log.raw_event_id
        where info.event_ingest_log.log_level < 3
        ",
    )
    .get_result::<GamesWithIssuesCount>(conn)
    .await
    .map(|games_with_issues| games_with_issues.games_with_issues)
}

pub async fn latest_ingest_start_time(
    conn: &mut AsyncPgConnection,
) -> QueryResult<Option<NaiveDateTime>> {
    use crate::data_schema::data::ingests::dsl::*;

    ingests
        .select(started_at)
        .order(started_at.desc())
        .first(conn)
        .await
        .optional()
}

pub async fn latest_completed_season(
    conn: &mut AsyncPgConnection,
) -> QueryResult<Option<i32>> {
    use crate::data_schema::data::ingests::dsl::*;

    ingests
        .filter(latest_completed_season.is_not_null())
        .select(latest_completed_season)
        .order(started_at.desc())
        .first(conn)
        .await
        .optional()
        .map(Option::flatten)
}

pub async fn latest_ingests(conn: &mut AsyncPgConnection) -> QueryResult<Vec<(Ingest, i64)>> {
    use diesel::sql_types::*;
    #[derive(QueryableByName)]
    struct IngestWithGameCount {
        #[diesel(sql_type = Int8)]
        pub id: i64,
        #[diesel(sql_type = Timestamp)]
        pub started_at: NaiveDateTime,
        #[diesel(sql_type = Nullable<Timestamp>)]
        pub finished_at: Option<NaiveDateTime>,
        #[diesel(sql_type = Nullable<Timestamp>)]
        pub aborted_at: Option<NaiveDateTime>,
        #[diesel(sql_type = Nullable<Int4>)]
        pub latest_completed_season: Option<i32>,
        #[diesel(sql_type = Int8)]
        pub num_games: i64,
    }

    diesel::sql_query(
        "
        select i.*, count(g.mmolb_game_id) as num_games
        from data.ingests i
             left join data.games g on g.ingest = i.id
        group by i.id, i.started_at
        order by i.started_at desc
        limit 25
    ",
    )
    .load::<IngestWithGameCount>(conn)
    .await
    .map(|ok| {
        ok.into_iter()
            .map(
                |IngestWithGameCount {
                     id,
                     started_at,
                     finished_at,
                     aborted_at,
                     latest_completed_season,
                     num_games,
                 }| {
                    (
                        Ingest {
                            id,
                            started_at,
                            finished_at,
                            aborted_at,
                            latest_completed_season,
                        },
                        num_games,
                    )
                },
            )
            .collect()
    })
}

pub async fn start_ingest(conn: &mut AsyncPgConnection, at: DateTime<Utc>) -> QueryResult<i64> {
    use crate::data_schema::data::ingests::dsl::*;

    NewIngest {
        started_at: at.naive_utc(),
    }
    .insert_into(ingests)
    .returning(id)
    .get_result(conn)
    .await
}

pub async fn mark_ingest_finished(
    conn: &mut AsyncPgConnection,
    ingest_id: i64,
    at: DateTime<Utc>,
    latest_completed: Option<i64>,
) -> QueryResult<()> {
    use crate::data_schema::data::ingests::dsl::*;

    diesel::update(ingests.filter(id.eq(ingest_id)))
        .set((
            finished_at.eq(at.naive_utc()),
            latest_completed_season.eq(latest_completed.map(|x| x as i32)),
         ))
        .execute(conn)
        .await
        .map(|_| ())
}

// It is assumed that an aborted ingest won't have a later 
// latest_completed_season than the previous ingest. That is not 
// necessarily true, but it's inconvenient to refactor the code to
// support it.
pub async fn mark_ingest_aborted(
    conn: &mut AsyncPgConnection,
    ingest_id: i64,
    at: DateTime<Utc>,
) -> QueryResult<()> {
    use crate::data_schema::data::ingests::dsl::*;

    diesel::update(ingests.filter(id.eq(ingest_id)))
        .set(aborted_at.eq(at.naive_utc()))
        .execute(conn)
        .await
        .map(|_| ())
}

macro_rules! log_only_assert {
    ($e: expr, $($msg:tt)*) => {
        if !$e {
            log::error!($($msg)*)
        }
    };
}

async fn add_log_levels_to_games(
    conn: &mut AsyncPgConnection,
    games: Vec<DbGame>,
) -> QueryResult<Vec<(DbGame, i64, i64, i64)>> {
    use crate::info_schema::info::event_ingest_log::dsl as event_ingest_log_dsl;
    use crate::info_schema::info::raw_events::dsl as raw_event_dsl;

    let game_ids = games.iter().map(|g| g.id).collect::<Vec<i64>>();

    async fn count_log_level(
        conn: &mut AsyncPgConnection,
        game_ids: &Vec<i64>,
        level: i32,
    ) -> QueryResult<Vec<(i64, i64)>> {
        raw_event_dsl::raw_events
            .filter(raw_event_dsl::game_id.eq_any(game_ids))
            .left_join(event_ingest_log_dsl::event_ingest_log)
            .filter(event_ingest_log_dsl::log_level.eq(level))
            .group_by(raw_event_dsl::game_id)
            .select((raw_event_dsl::game_id, diesel::dsl::count_star()))
            .order_by(raw_event_dsl::game_id)
            .get_results::<(i64, i64)>(conn)
            .await
    }

    let mut num_warnings = count_log_level(conn, &game_ids, 2)
        .await?
        .into_iter()
        .peekable();
    let mut num_errors = count_log_level(conn, &game_ids, 1)
        .await?
        .into_iter()
        .peekable();
    let mut num_critical = count_log_level(conn, &game_ids, 0)
        .await?
        .into_iter()
        .peekable();

    let games = games
        .into_iter()
        .map(|game| {
            let warnings = num_warnings
                .peeking_next(|(id, _)| *id == game.id)
                .map_or(0, |(_, n)| n);
            let errors = num_errors
                .peeking_next(|(id, _)| *id == game.id)
                .map_or(0, |(_, n)| n);
            let critical = num_critical
                .peeking_next(|(id, _)| *id == game.id)
                .map_or(0, |(_, n)| n);
            (game, warnings, errors, critical)
        })
        .collect();

    log_only_assert!(
        num_warnings.next().is_none(),
        "db::ingest_with_games failed to match at least one warnings count with its event",
    );
    log_only_assert!(
        num_errors.next().is_none(),
        "db::ingest_with_games failed to match at least one errors count with its event",
    );
    log_only_assert!(
        num_critical.next().is_none(),
        "db::ingest_with_games failed to match at least one critical count with its event",
    );

    Ok(games)
}

pub async fn ingest_with_games(
    conn: &mut AsyncPgConnection,
    for_ingest_id: i64,
) -> QueryResult<(Ingest, Vec<(DbGame, i64, i64, i64)>)> {
    use crate::data_schema::data::games::dsl as game_dsl;
    use crate::data_schema::data::ingests::dsl as ingest_dsl;

    let ingest = ingest_dsl::ingests
        .filter(ingest_dsl::id.eq(for_ingest_id))
        .get_result::<Ingest>(conn)
        .await?;

    let games = DbGame::belonging_to(&ingest)
        .order_by(game_dsl::id)
        .get_results::<DbGame>(conn)
        .await?;

    let games = add_log_levels_to_games(conn, games).await?;

    Ok((ingest, games))
}

pub async fn has_game(conn: &mut AsyncPgConnection, with_id: &str) -> QueryResult<bool> {
    use crate::data_schema::data::games::dsl::*;
    use diesel::dsl::*;

    select(exists(games.filter(mmolb_game_id.eq(with_id))))
        .get_result(conn)
        .await
}

pub async fn delete_game(conn: &mut AsyncPgConnection, with_id: &str) -> QueryResult<usize> {
    use crate::data_schema::data::games::dsl::*;
    use diesel::dsl::*;

    delete(games.filter(mmolb_game_id.eq(with_id)))
        .execute(conn)
        .await
}

pub async fn all_games(conn: &mut AsyncPgConnection) -> QueryResult<Vec<(DbGame, i64, i64, i64)>> {
    use crate::data_schema::data::games::dsl as games_dsl;

    let games = games_dsl::games
        .order_by(games_dsl::id)
        .get_results::<DbGame>(conn)
        .await?;

    add_log_levels_to_games(conn, games).await
}

pub struct EventsForGameTimings {
    pub get_game_id_duration: f64,
    pub get_events_duration: f64,
    pub get_runners_duration: f64,
    pub group_runners_duration: f64,
    pub get_fielders_duration: f64,
    pub group_fielders_duration: f64,
    pub post_process_duration: f64,
}

pub async fn events_for_game<'e>(
    conn: &mut AsyncPgConnection,
    taxa: &Taxa,
    for_game_id: &str,
) -> QueryResult<(
    Vec<Result<EventDetail<String>, RowToEventError>>,
    EventsForGameTimings,
)> {
    use crate::data_schema::data::event_baserunners::dsl as runner_dsl;
    use crate::data_schema::data::event_fielders::dsl as fielder_dsl;
    use crate::data_schema::data::events::dsl as events_dsl;
    use crate::data_schema::data::games::dsl as games_dsl;

    let get_game_id_start = Utc::now();
    let game_id = games_dsl::games
        .filter(games_dsl::mmolb_game_id.eq(for_game_id))
        .select(games_dsl::id)
        .get_result::<i64>(conn)
        .await?;
    let get_game_id_duration = (Utc::now() - get_game_id_start).as_seconds_f64();

    let get_events_start = Utc::now();
    let db_events = events_dsl::events
        .filter(events_dsl::game_id.eq(game_id))
        .order(events_dsl::game_event_index.asc())
        .select(DbEvent::as_select())
        .load(conn)
        .await?;
    let get_events_duration = (Utc::now() - get_events_start).as_seconds_f64();

    let get_runners_start = Utc::now();
    let db_runners = DbRunner::belonging_to(&db_events)
        .order((
            runner_dsl::event_id,
            runner_dsl::base_before.desc().nulls_last(),
        ))
        .select(DbRunner::as_select())
        .load(conn)
        .await?;
    let get_runners_duration = (Utc::now() - get_runners_start).as_seconds_f64();

    let group_runners_start = Utc::now();
    let db_runners = db_runners.grouped_by(&db_events);
    let group_runners_duration = (Utc::now() - group_runners_start).as_seconds_f64();

    let get_fielders_start = Utc::now();
    let db_fielders = DbFielder::belonging_to(&db_events)
        .order((fielder_dsl::event_id, fielder_dsl::play_order))
        .select(DbFielder::as_select())
        .load(conn)
        .await?;
    let get_fielders_duration = (Utc::now() - get_fielders_start).as_seconds_f64();

    let group_fielders_start = Utc::now();
    let db_fielders = db_fielders.grouped_by(&db_events);
    let group_fielders_duration = (Utc::now() - group_fielders_start).as_seconds_f64();

    // This complicated-looking statement just zips all the iterators
    // together and passes the corresponding elements to row_to_event
    let post_process_start = Utc::now();
    let result = db_events
        .into_iter()
        .zip(db_runners)
        .zip(db_fielders)
        .map(|((db_event, db_runners), db_fielders)| {
            to_db_format::row_to_event(taxa, db_event, db_runners, db_fielders)
        })
        .collect();
    let post_process_duration = (Utc::now() - post_process_start).as_seconds_f64();

    Ok((
        result,
        EventsForGameTimings {
            get_game_id_duration,
            get_events_duration,
            get_runners_duration,
            group_runners_duration,
            get_fielders_duration,
            group_fielders_duration,
            post_process_duration,
        },
    ))
}

pub async fn insert_game<'e>(
    conn: &mut AsyncPgConnection,
    taxa: &Taxa,
    ingest_id: i64,
    mmolb_game_id: &str,
    game_data: &mmolb_parsing::game::Game,
    logs: impl IntoIterator<Item = impl IntoIterator<Item = IngestLog>> + Send,
    event_details: &'e [EventDetail<&'e str>],
    final_game_state: MinimalGameState,
) -> QueryResult<i64> {
    conn.transaction::<_, _, _>(|conn| {
        async move {
            insert_game_internal(
                conn,
                taxa,
                ingest_id,
                mmolb_game_id,
                game_data,
                logs,
                event_details,
                final_game_state,
            )
            .await
        }
        .scope_boxed()
    })
    .await
}

async fn insert_game_internal<'e>(
    conn: &mut AsyncPgConnection,
    taxa: &Taxa,
    ingest_id: i64,
    mmolb_game_id: &str,
    game_data: &mmolb_parsing::game::Game,
    logs: impl IntoIterator<Item = impl IntoIterator<Item = IngestLog>>,
    event_details: &'e [EventDetail<&'e str>],
    final_game_state: MinimalGameState,
) -> QueryResult<i64> {
    use crate::data_schema::data::event_baserunners::dsl as baserunners_dsl;
    use crate::data_schema::data::event_fielders::dsl as fielders_dsl;
    use crate::data_schema::data::events::dsl as events_dsl;
    use crate::data_schema::data::games::dsl as games_dsl;
    use crate::info_schema::info::event_ingest_log::dsl as event_ingest_log_dsl;
    use crate::info_schema::info::raw_events::dsl as raw_events_dsl;

    let game_id = diesel::insert_into(games_dsl::games)
        .values(&NewGame {
            ingest: ingest_id,
            mmolb_game_id,
            season: game_data.season as i32,
            day: game_data.day as i32,
            away_team_emoji: &game_data.away_team_emoji,
            away_team_name: &game_data.away_team_name,
            away_team_id: &game_data.away_team_id,
            final_away_score: final_game_state.away_score as i32,
            home_team_emoji: &game_data.home_team_emoji,
            home_team_name: &game_data.home_team_name,
            home_team_id: &game_data.home_team_id,
            final_home_score: final_game_state.home_score as i32,
        })
        .returning(games_dsl::id)
        .get_result::<i64>(conn)
        .await?;

    let new_raw_events = game_data
        .event_log
        .iter()
        .enumerate()
        .map(|(index, raw_event)| NewRawEvent {
            game_id,
            game_event_index: index as i32,
            event_text: &raw_event.message,
        })
        .collect::<Vec<_>>();
    let raw_event_ids = diesel::insert_into(raw_events_dsl::raw_events)
        .values(new_raw_events)
        .returning(raw_events_dsl::id)
        .get_results::<i64>(conn)
        .await?;

    let new_logs: Vec<_> = logs
        .into_iter()
        .zip(raw_event_ids)
        .flat_map(|(logs_for_event, raw_event_id)| {
            logs_for_event
                .into_iter()
                .enumerate()
                .map(move |(log_idx, ingest_log)| NewEventIngestLogOwning {
                    raw_event_id,
                    log_order: log_idx as i32,
                    log_level: ingest_log.log_level,
                    log_text: ingest_log.log_text,
                })
        })
        .collect();

    diesel::insert_into(event_ingest_log_dsl::event_ingest_log)
        .values(new_logs)
        .execute(conn)
        .await?;

    let new_events: Vec<_> = event_details
        .iter()
        .map(|event| to_db_format::event_to_row(taxa, game_id, event))
        .collect();

    let event_ids = diesel::insert_into(events_dsl::events)
        .values(new_events)
        .returning(events_dsl::id)
        .get_results::<i64>(conn)
        .await?;

    log_only_assert!(
        event_ids.len() == event_details.len(),
        "Events insert should insert {} rows",
        event_details.len(),
    );

    let new_advances: Vec<_> = event_details
        .iter()
        .zip(&event_ids)
        .flat_map(|(event, &event_id)| to_db_format::event_to_baserunners(taxa, event_id, event))
        .collect();
    let n_advances_to_insert = new_advances.len();

    let n_advances_inserted = diesel::insert_into(baserunners_dsl::event_baserunners)
        .values(new_advances)
        .execute(conn)
        .await?;

    log_only_assert!(
        n_advances_inserted == n_advances_to_insert,
        "Advances insert should insert {n_advances_to_insert} rows",
    );

    let new_fielders: Vec<_> = event_details
        .iter()
        .zip(&event_ids)
        .flat_map(|(event, &event_id)| to_db_format::event_to_fielders(taxa, event_id, event))
        .collect();
    let n_fielders_to_insert = new_fielders.len();

    let n_fielders_inserted = diesel::insert_into(fielders_dsl::event_fielders)
        .values(new_fielders)
        .execute(conn)
        .await?;

    log_only_assert!(
        n_fielders_inserted == n_fielders_to_insert,
        "Fielders insert should insert {n_fielders_to_insert} rows",
    );

    Ok(game_id)
}

pub async fn insert_additional_ingest_logs(
    conn: &mut AsyncPgConnection,
    game_id: i64,
    extra_ingest_logs: impl IntoIterator<Item = (usize, IngestLog)>,
) -> QueryResult<()> {
    use crate::info_schema::info::event_ingest_log::dsl as event_ingest_log_dsl;
    use crate::info_schema::info::raw_events::dsl as raw_events_dsl;

    // index is game_event_index, value is raw_event id
    let raw_event_ids: Vec<i64> = raw_events_dsl::raw_events
        .select(raw_events_dsl::id)
        .filter(raw_events_dsl::game_id.eq(game_id))
        .order_by(raw_events_dsl::game_event_index.asc())
        .get_results(conn)
        .await?;

    // Maps raw_event_id to its highest log_order
    let mut highest_log_order_for_event: HashMap<_, _> = event_ingest_log_dsl::event_ingest_log
        .group_by(event_ingest_log_dsl::raw_event_id)
        .select((
            event_ingest_log_dsl::raw_event_id,
            diesel::dsl::max(event_ingest_log_dsl::log_order),
        ))
        .filter(event_ingest_log_dsl::raw_event_id.eq_any(&raw_event_ids))
        .order_by(event_ingest_log_dsl::raw_event_id.asc())
        .get_results::<(i64, Option<i32>)>(conn)
        .await?
        .into_iter()
        .filter_map(|(id, num)| num.map(|n| (id, n)))
        .collect();

    let new_logs: Vec<_> = extra_ingest_logs
        .into_iter()
        .map(|(game_event_index, ingest_log)| {
            let raw_event_id = raw_event_ids[game_event_index];
            let n = highest_log_order_for_event.entry(raw_event_id).or_default();
            *n += 1;

            NewEventIngestLogOwning {
                raw_event_id,
                log_order: *n,
                log_level: ingest_log.log_level,
                log_text: ingest_log.log_text,
            }
        })
        .collect();

    diesel::insert_into(event_ingest_log_dsl::event_ingest_log)
        .values(new_logs)
        .execute(conn)
        .await?;

    Ok(())
}

pub async fn game_and_raw_events(
    conn: &mut AsyncPgConnection,
    for_game_id: i64,
) -> QueryResult<(DbGame, Vec<(DbRawEvent, Vec<DbEventIngestLog>)>)> {
    use crate::data_schema::data::games::dsl as games_dsl;
    use crate::info_schema::info::event_ingest_log::dsl as event_ingest_log_dsl;
    use crate::info_schema::info::raw_events::dsl as raw_events_dsl;

    let game = games_dsl::games
        .filter(games_dsl::id.eq(for_game_id))
        .select(DbGame::as_select())
        .get_result::<DbGame>(conn)
        .await?;

    let raw_events = DbRawEvent::belonging_to(&game)
        .order_by(raw_events_dsl::game_event_index.asc())
        .load::<DbRawEvent>(conn)
        .await?;

    let raw_logs = DbEventIngestLog::belonging_to(&raw_events)
        .order_by(event_ingest_log_dsl::log_order.asc())
        .load::<DbEventIngestLog>(conn)
        .await?;

    let logs_by_event = raw_logs.grouped_by(&raw_events);

    let events_with_logs = raw_events
        .into_iter()
        .zip(logs_by_event)
        .collect::<Vec<_>>();

    Ok((game, events_with_logs))
}

pub struct Timings {
    pub check_already_ingested_duration: f64,
    pub network_duration: f64,
    pub parse_duration: f64,
    pub sim_duration: f64,
    pub db_insert_duration: f64,
    pub db_fetch_for_check_duration: f64,
    pub events_for_game_timings: EventsForGameTimings,
    pub db_duration: f64,
    pub check_round_trip_duration: f64,
    pub insert_extra_logs_duration: f64,
    pub total_duration: f64,
}

pub async fn insert_timings(
    conn: &mut AsyncPgConnection,
    for_game_id: i64,
    timings: Timings,
) -> QueryResult<()> {
    NewGameIngestTimings {
        game_id: for_game_id,
        check_already_ingested_duration: timings.check_already_ingested_duration,
        network_duration: timings.network_duration,
        parse_duration: timings.parse_duration,
        sim_duration: timings.sim_duration,
        db_fetch_for_check_get_game_id_duration: timings
            .events_for_game_timings
            .get_game_id_duration,
        db_fetch_for_check_get_events_duration: timings.events_for_game_timings.get_events_duration,
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
        db_fetch_for_check_duration: timings.db_fetch_for_check_duration,
        db_duration: timings.db_duration,
        check_round_trip_duration: timings.check_round_trip_duration,
        insert_extra_logs_duration: timings.insert_extra_logs_duration,
        total_duration: timings.total_duration,
    }
    .insert_into(crate::info_schema::info::game_ingest_timing::dsl::game_ingest_timing)
    .execute(conn)
    .await
    .map(|_| ())
}
