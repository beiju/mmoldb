use chrono::NaiveDateTime;
use rocket_db_pools::diesel::prelude::*;

#[derive(Insertable)]
#[diesel(table_name = crate::taxa_schema::taxa::event_type)]
pub struct NewEventType<'a> {
    pub name: &'a str,
    pub display_name: &'a str,
}

#[derive(Insertable)]
#[diesel(table_name = crate::taxa_schema::taxa::hit_type)]
pub struct NewHitType<'a> {
    pub name: &'a str,
    pub display_name: &'a str,
}

#[derive(Insertable)]
#[diesel(table_name = crate::taxa_schema::taxa::position)]
pub struct NewPosition<'a> {
    pub name: &'a str,
    pub display_name: &'a str,
}

#[derive(Insertable)]
#[diesel(table_name = crate::taxa_schema::taxa::fair_ball_type)]
pub struct NewFairBallType<'a> {
    pub name: &'a str,
    pub display_name: &'a str,
}

#[derive(Insertable)]
#[diesel(table_name = crate::taxa_schema::taxa::base)]
pub struct NewBase<'a> {
    pub name: &'a str,
    pub display_name: &'a str,
}

#[derive(Insertable)]
#[diesel(table_name = crate::taxa_schema::taxa::base_description_format)]
pub struct NewBaseDescriptionFormat<'a> {
    pub name: &'a str,
    pub display_name: &'a str,
}

#[derive(Insertable)]
#[diesel(table_name = crate::taxa_schema::taxa::fielding_error_type)]
pub struct NewFieldingErrorType<'a> {
    pub name: &'a str,
    pub display_name: &'a str,
}

#[derive(Insertable)]
#[diesel(table_name = crate::taxa_schema::taxa::pitch_type)]
pub struct NewPitchType<'a> {
    pub name: &'a str,
    pub display_name: &'a str,
}

#[derive(Insertable)]
#[diesel(table_name = crate::data_schema::data::ingests)]
pub struct NewIngest {
    pub started_at: NaiveDateTime,
}

#[derive(Identifiable, Queryable, Selectable)]
#[diesel(table_name = crate::data_schema::data::ingests)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Ingest {
    pub id: i64,
    pub started_at: NaiveDateTime,
    pub finished_at: Option<NaiveDateTime>,
    pub aborted_at: Option<NaiveDateTime>,
    pub latest_completed_season: Option<i32>,
}

#[derive(Insertable)]
#[diesel(table_name = crate::data_schema::data::games)]
pub struct NewGame<'a> {
    pub ingest: i64,
    pub mmolb_game_id: &'a str,
    pub season: i32,
    pub day: i32,
    pub away_team_emoji: &'a str,
    pub away_team_name: &'a str,
    pub away_team_id: &'a str,
    pub home_team_emoji: &'a str,
    pub home_team_name: &'a str,
    pub home_team_id: &'a str,
}

#[derive(Identifiable, Queryable, Selectable, Associations)]
#[diesel(belongs_to(Ingest, foreign_key = ingest))]
#[diesel(table_name = crate::data_schema::data::games)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct DbGame {
    pub id: i64,
    pub ingest: i64,
    pub mmolb_game_id: String,
    pub season: i32,
    pub day: i32,
    pub away_team_emoji: String,
    pub away_team_name: String,
    pub away_team_id: String,
    pub home_team_emoji: String,
    pub home_team_name: String,
    pub home_team_id: String,
}

#[derive(Insertable)]
#[diesel(table_name = crate::data_schema::data::events)]
pub struct NewEvent<'a> {
    pub game_id: i64,
    pub game_event_index: i32,
    pub fair_ball_event_index: Option<i32>,
    pub inning: i32,
    pub top_of_inning: bool,
    pub event_type: i64,
    pub hit_type: Option<i64>,
    pub fair_ball_type: Option<i64>,
    pub fair_ball_direction: Option<i64>,
    pub fielding_error_type: Option<i64>,
    pub pitch_type: Option<i64>,
    pub pitch_speed: Option<f64>,
    pub described_as_sacrifice: Option<bool>,
    pub count_balls: i32,
    pub count_strikes: i32,
    pub outs_before: i32,
    pub outs_after: i32,
    pub batter_name: &'a str,
    pub pitcher_name: &'a str,
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
    pub hit_type: Option<i64>,
    pub fair_ball_type: Option<i64>,
    pub fair_ball_direction: Option<i64>,
    pub fielding_error_type: Option<i64>,
    pub pitch_type: Option<i64>,
    pub pitch_speed: Option<f64>,
    pub described_as_sacrifice: Option<bool>,
    pub count_balls: i32,
    pub count_strikes: i32,
    pub outs_before: i32,
    pub outs_after: i32,
    pub batter_name: String,
    pub pitcher_name: String,
}

#[derive(Insertable)]
#[diesel(table_name = crate::info_schema::info::raw_events)]
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

// This struct happens to be used in a context where it's more natural
// for it to own its data. A non-owning version is also perfectly
// possible.
#[derive(Insertable)]
#[diesel(table_name = crate::info_schema::info::event_ingest_log)]
pub struct NewEventIngestLogOwning {
    pub raw_event_id: i64,
    pub log_order: i32,
    pub log_level: i32,
    pub log_text: String,
}

#[derive(Identifiable, Queryable, Selectable, Associations)]
#[diesel(belongs_to(DbRawEvent, foreign_key = raw_event_id))]
#[diesel(table_name = crate::info_schema::info::event_ingest_log)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct DbEventIngestLog {
    pub id: i64,
    pub raw_event_id: i64,
    pub log_order: i32,
    pub log_level: i32,
    pub log_text: String,
}

#[derive(Insertable)]
#[diesel(table_name = crate::info_schema::info::game_ingest_timing)]
pub struct NewGameIngestTimings {
    pub game_id: i64,
    pub check_already_ingested_duration: f64,
    pub network_duration: f64,
    pub parse_duration: f64,
    pub sim_duration: f64,
    pub db_insert_duration: f64,
    pub db_fetch_for_check_get_game_id_duration: f64,
    pub db_fetch_for_check_get_events_duration: f64,
    pub db_fetch_for_check_get_runners_duration: f64,
    pub db_fetch_for_check_group_runners_duration: f64,
    pub db_fetch_for_check_get_fielders_duration: f64,
    pub db_fetch_for_check_group_fielders_duration: f64,
    pub db_fetch_for_check_post_process_duration: f64,
    pub db_fetch_for_check_duration: f64,
    pub db_duration: f64,
    pub check_round_trip_duration: f64,
    pub insert_extra_logs_duration: f64,
    pub total_duration: f64,
}

#[derive(Identifiable, Queryable, Selectable, Associations)]
#[diesel(belongs_to(DbGame, foreign_key = game_id))]
#[diesel(table_name = crate::info_schema::info::game_ingest_timing)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct DbGameIngestTimings {
    pub id: i64,
    pub game_id: i64,
    pub check_already_ingested_duration: f64,
    pub network_duration: f64,
    pub parse_duration: f64,
    pub sim_duration: f64,
    pub db_insert_duration: f64,
    pub db_fetch_for_check_get_game_id_duration: f64,
    pub db_fetch_for_check_get_events_duration: f64,
    pub db_fetch_for_check_get_runners_duration: f64,
    pub db_fetch_for_check_group_runners_duration: f64,
    pub db_fetch_for_check_get_fielders_duration: f64,
    pub db_fetch_for_check_group_fielders_duration: f64,
    pub db_fetch_for_check_post_process_duration: f64,
    pub db_fetch_for_check_duration: f64,
    pub db_duration: f64,
    pub check_round_trip_duration: f64,
    pub insert_extra_logs_duration: f64,
    pub total_duration: f64,
}

#[derive(Insertable)]
#[diesel(table_name = crate::data_schema::data::event_baserunners)]
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
pub struct NewFielder<'a> {
    pub event_id: i64,
    pub fielder_name: &'a str,
    pub fielder_position: i64,
    pub play_order: i32,
    pub perfect_catch: Option<bool>,
}

#[derive(Identifiable, Queryable, Selectable, Associations)]
#[diesel(belongs_to(DbEvent, foreign_key = event_id))]
#[diesel(table_name = crate::data_schema::data::event_fielders)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct DbFielder {
    pub id: i64,
    pub event_id: i64,
    pub fielder_name: String,
    pub fielder_position: i64,
    pub play_order: i32,
    pub perfect_catch: Option<bool>,
}
