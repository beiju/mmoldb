use chrono::NaiveDateTime;
use serde::Serialize;
use diesel::prelude::*;

#[derive(Insertable)]
#[diesel(table_name = crate::info_schema::info::ingests)]
pub struct NewIngest {
    pub started_at: NaiveDateTime,
}

#[derive(Identifiable, Queryable, Selectable)]
#[diesel(table_name = crate::info_schema::info::ingests)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct DbIngest {
    pub id: i64,
    pub started_at: NaiveDateTime,
    pub finished_at: Option<NaiveDateTime>,
    pub aborted_at: Option<NaiveDateTime>,
    pub start_next_ingest_at_page: Option<String>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = crate::data_schema::data::weather)]
pub struct NewWeather<'a> {
    pub name: &'a str,
    pub emoji: &'a str,
    pub tooltip: &'a str,
}

#[derive(Debug, Identifiable, Queryable, Selectable, QueryableByName)]
#[diesel(table_name = crate::data_schema::data::weather)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct DbWeather {
    pub id: i64,
    pub name: String,
    pub emoji: String,
    pub tooltip: String,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = crate::data_schema::data::games)]
pub struct NewGame<'a> {
    pub ingest: i64,
    pub mmolb_game_id: &'a str,
    pub weather: i64,
    pub season: i32,
    pub day: Option<i32>,
    pub superstar_day: Option<i32>,
    pub away_team_emoji: &'a str,
    pub away_team_name: &'a str,
    pub away_team_mmolb_id: &'a str,
    pub away_team_final_score: Option<i32>,
    pub home_team_emoji: &'a str,
    pub home_team_name: &'a str,
    pub home_team_mmolb_id: &'a str,
    pub home_team_final_score: Option<i32>,
    pub is_finished: bool,
}

#[derive(Identifiable, Queryable, Selectable, Associations, QueryableByName)]
#[diesel(belongs_to(DbIngest, foreign_key = ingest))]
#[diesel(table_name = crate::data_schema::data::games)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct DbGame {
    pub id: i64,
    pub ingest: i64,
    pub mmolb_game_id: String,
    pub season: i32,
    pub day: Option<i32>,
    pub superstar_day: Option<i32>,
    pub away_team_emoji: String,
    pub away_team_name: String,
    pub away_team_mmolb_id: String,
    pub home_team_emoji: String,
    pub home_team_name: String,
    pub home_team_mmolb_id: String,
    pub is_finished: bool,
}

#[derive(Insertable)]
#[diesel(table_name = crate::data_schema::data::events)]
#[diesel(treat_none_as_default_value = false)]
pub struct NewEvent<'a> {
    pub game_id: i64,
    pub game_event_index: i32,
    pub fair_ball_event_index: Option<i32>,
    pub inning: i32,
    pub top_of_inning: bool,
    pub event_type: i64,
    pub hit_base: Option<i64>,
    pub fair_ball_type: Option<i64>,
    pub fair_ball_direction: Option<i64>,
    pub fielding_error_type: Option<i64>,
    pub pitch_type: Option<i64>,
    pub pitch_speed: Option<f64>,
    pub pitch_zone: Option<i32>,
    pub described_as_sacrifice: Option<bool>,
    pub is_toasty: Option<bool>,
    pub balls_before: i32,
    pub strikes_before: i32,
    pub outs_before: i32,
    pub outs_after: i32,
    pub away_team_score_before: i32,
    pub away_team_score_after: i32,
    pub home_team_score_before: i32,
    pub home_team_score_after: i32,
    pub pitcher_name: &'a str,
    pub pitcher_count: i32,
    pub batter_name: &'a str,
    pub batter_count: i32,
    pub batter_subcount: i32,
}
#[derive(Queryable, Selectable, Identifiable)]
#[diesel(table_name = crate::data_schema::data::events)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct DbEvent {
    pub id: i64,
    pub game_id: i64,
    pub game_event_index: i32,
    pub fair_ball_event_index: Option<i32>,
    pub inning: i32,
    pub top_of_inning: bool,
    pub event_type: i64,
    pub hit_base: Option<i64>,
    pub fair_ball_type: Option<i64>,
    pub fair_ball_direction: Option<i64>,
    pub fielding_error_type: Option<i64>,
    pub pitch_type: Option<i64>,
    pub pitch_speed: Option<f64>,
    pub pitch_zone: Option<i32>,
    pub described_as_sacrifice: Option<bool>,
    pub is_toasty: Option<bool>,
    pub balls_before: i32,
    pub strikes_before: i32,
    pub away_team_score_before: i32,
    pub away_team_score_after: i32,
    pub home_team_score_before: i32,
    pub home_team_score_after: i32,
    pub outs_before: i32,
    pub outs_after: i32,
    pub pitcher_name: String,
    pub pitcher_count: i32,
    pub batter_name: String,
    pub batter_count: i32,
    pub batter_subcount: i32,
}

#[derive(Insertable)]
#[diesel(table_name = crate::info_schema::info::raw_events)]
#[diesel(treat_none_as_default_value = false)]
pub struct NewRawEvent<'a> {
    pub game_id: i64,
    pub game_event_index: i32,
    pub event_text: &'a str,
}

#[derive(Identifiable, Queryable, Selectable, Associations)]
#[diesel(belongs_to(DbGame, foreign_key = game_id))]
#[diesel(table_name = crate::info_schema::info::raw_events)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct DbRawEvent {
    pub id: i64,
    pub game_id: i64,
    pub game_event_index: i32,
    pub event_text: String,
}

#[derive(Insertable)]
#[diesel(table_name = crate::info_schema::info::event_ingest_log)]
#[diesel(treat_none_as_default_value = false)]
pub struct NewEventIngestLog<'a> {
    // Compound key
    pub game_id: i64,
    pub game_event_index: Option<i32>,
    pub log_index: i32,

    // Data
    pub log_level: i32,
    pub log_text: &'a str,
}

#[derive(Identifiable, Queryable, Selectable)]
#[diesel(table_name = crate::info_schema::info::event_ingest_log)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct DbEventIngestLog {
    pub id: i64,
    pub game_id: i64,
    pub game_event_index: Option<i32>,
    pub log_index: i32,
    pub log_level: i32,
    pub log_text: String,
}

#[derive(Insertable)]
#[diesel(table_name = crate::info_schema::info::ingest_timings)]
pub struct NewGameIngestTimings {
    pub ingest_id: i64,
    pub index: i32,

    pub fetch_duration: f64,

    pub filter_finished_games_duration: f64,
    pub parse_and_sim_duration: f64,
    pub db_insert_duration: f64,
    pub db_insert_delete_old_games_duration: f64,
    pub db_insert_update_weather_table_duration: f64,
    pub db_insert_insert_games_duration: f64,
    pub db_insert_insert_raw_events_duration: f64,
    pub db_insert_insert_logs_duration: f64,
    pub db_insert_insert_events_duration: f64,
    pub db_insert_get_event_ids_duration: f64,
    pub db_insert_insert_baserunners_duration: f64,
    pub db_insert_insert_fielders_duration: f64,
    pub db_fetch_for_check_duration: f64,
    pub db_fetch_for_check_get_game_id_duration: f64,
    pub db_fetch_for_check_get_events_duration: f64,
    pub db_fetch_for_check_group_events_duration: f64,
    pub db_fetch_for_check_get_runners_duration: f64,
    pub db_fetch_for_check_group_runners_duration: f64,
    pub db_fetch_for_check_get_fielders_duration: f64,
    pub db_fetch_for_check_group_fielders_duration: f64,
    pub db_fetch_for_check_post_process_duration: f64,
    pub check_round_trip_duration: f64,
    pub insert_extra_logs_duration: f64,
    pub save_duration: f64,
}

#[derive(Identifiable, Queryable, Selectable, Associations)]
#[diesel(belongs_to(DbIngest, foreign_key = ingest_id))]
#[diesel(table_name = crate::info_schema::info::ingest_timings)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct DbGameIngestTimings {
    pub id: i64,
    pub ingest_id: i64,
    pub index: i32,
    pub fetch_duration: f64,
    pub filter_finished_games_duration: f64,
    pub parse_and_sim_duration: f64,
    pub db_insert_duration: f64,
    pub db_fetch_for_check_get_game_id_duration: f64,
    pub db_fetch_for_check_get_events_duration: f64,
    pub db_fetch_for_check_group_events_duration: f64,
    pub db_fetch_for_check_get_runners_duration: f64,
    pub db_fetch_for_check_group_runners_duration: f64,
    pub db_fetch_for_check_get_fielders_duration: f64,
    pub db_fetch_for_check_group_fielders_duration: f64,
    pub db_fetch_for_check_post_process_duration: f64,
    pub db_fetch_for_check_duration: f64,
    pub check_round_trip_duration: f64,
    pub insert_extra_logs_duration: f64,
    pub save_duration: f64,
}

#[derive(Insertable)]
#[diesel(table_name = crate::data_schema::data::event_baserunners)]
#[diesel(treat_none_as_default_value = false)]
pub struct NewBaserunner<'a> {
    pub event_id: i64,
    pub baserunner_name: &'a str,
    pub base_before: Option<i64>,
    pub base_after: i64,
    pub is_out: bool,
    pub base_description_format: Option<i64>,
    pub steal: bool,
}

#[derive(Identifiable, Queryable, Selectable, Associations)]
#[diesel(belongs_to(DbEvent, foreign_key = event_id))]
#[diesel(table_name = crate::data_schema::data::event_baserunners)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct DbRunner {
    pub id: i64,
    pub event_id: i64,
    pub baserunner_name: String,
    pub base_before: Option<i64>,
    pub base_after: i64,
    pub is_out: bool,
    pub base_description_format: Option<i64>,
    pub steal: bool,
}

#[derive(Insertable)]
#[diesel(table_name = crate::data_schema::data::event_fielders)]
#[diesel(treat_none_as_default_value = false)]
pub struct NewFielder<'a> {
    pub event_id: i64,
    pub fielder_name: &'a str,
    pub fielder_slot: i64,
    pub play_order: i32,
}

#[derive(Identifiable, Queryable, Selectable, Associations)]
#[diesel(belongs_to(DbEvent, foreign_key = event_id))]
#[diesel(table_name = crate::data_schema::data::event_fielders)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct DbFielder {
    pub id: i64,
    pub event_id: i64,
    pub fielder_name: String,
    pub fielder_slot: i64,
    pub play_order: i32,
}

#[derive(Queryable, Selectable, Serialize)]
#[diesel(table_name = crate::meta_schema::meta::schemata)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct DbSchema {
    pub catalog_name: Option<String>,
    pub schema_name: Option<String>,
}


#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::meta_schema::meta::tables)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct RawDbTable {
    pub table_catalog: Option<String>,
    pub table_schema: Option<String>,
    pub table_name: Option<String>,
    pub table_type: Option<String>,
}


#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::meta_schema::meta::columns)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct RawDbColumn {
    pub table_catalog: Option<String>,
    pub table_schema: Option<String>,
    pub table_name: Option<String>,
    pub column_name: Option<String>,
    pub ordinal_position: Option<i32>,
    pub column_default: Option<String>,
    pub column_is_nullable: Option<String>,
    pub data_type: Option<String>,
    pub character_maximum_length: Option<i32>,
    pub character_octet_length: Option<i32>,
    pub numeric_precision: Option<i32>,
    pub numeric_precision_radix: Option<i32>,
    pub numeric_scale: Option<i32>,
    pub datetime_precision: Option<i32>,
    pub interval_type: Option<String>,
    pub interval_precision: Option<i32>,
}
