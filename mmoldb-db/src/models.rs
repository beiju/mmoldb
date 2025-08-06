use chrono::NaiveDateTime;
use diesel::prelude::*;
use serde::Serialize;

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
    pub is_ongoing: bool,
    pub stadium_name: Option<&'a str>,
    pub from_version: NaiveDateTime,
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
    pub is_ongoing: bool,
    pub stadium_name: Option<String>,
    pub from_version: NaiveDateTime,
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
    pub errors_before: i32,
    pub errors_after: i32,
    pub away_team_score_before: i32,
    pub away_team_score_after: i32,
    pub home_team_score_before: i32,
    pub home_team_score_after: i32,
    pub pitcher_name: &'a str,
    pub pitcher_count: i32,
    pub batter_name: &'a str,
    pub batter_count: i32,
    pub batter_subcount: i32,
    // This is an owned string because it's generated at the last minute
    // TODO Set up a foreign relationship for cheers like weather has
    pub cheer: Option<String>,
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
    pub errors_before: i32,
    pub errors_after: i32,
    pub pitcher_name: String,
    pub pitcher_count: i32,
    pub batter_name: String,
    pub batter_count: i32,
    pub batter_subcount: i32,
    pub cheer: Option<String>,
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

    pub get_batch_to_process_duration: f64,
    pub deserialize_games_duration: f64,
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
    pub get_batch_to_process_duration: f64,
    pub deserialize_games_duration: f64,
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
    pub source_event_index: Option<i32>,
    pub is_earned: bool,
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
    pub source_event_index: Option<i32>,
    pub is_earned: bool,
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

#[derive(Debug, Insertable)]
#[diesel(table_name = crate::data_schema::data::modifications)]
#[diesel(treat_none_as_default_value = false)]
pub struct NewModification<'a> {
    pub name: &'a str,
    pub emoji: &'a str,
    pub description: &'a str,
}

#[derive(Debug, Identifiable, Queryable, Selectable, QueryableByName)]
#[diesel(table_name = crate::data_schema::data::modifications)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct DbModification {
    pub id: i64,
    pub name: String,
    pub emoji: String,
    pub description: String,
}

#[derive(Clone, Debug, Insertable, PartialEq)]
#[diesel(table_name = crate::data_schema::data::player_modification_versions)]
#[diesel(treat_none_as_default_value = false)]
pub struct NewPlayerModificationVersion<'a> {
    pub mmolb_player_id: &'a str,
    pub valid_from: NaiveDateTime,
    pub valid_until: Option<NaiveDateTime>,
    pub modification_order: i32,
    pub modification_id: i64,
}

#[derive(Clone, Debug, Insertable, PartialEq)]
#[diesel(table_name = crate::data_schema::data::player_versions)]
#[diesel(treat_none_as_default_value = false)]
pub struct NewPlayerVersion<'a> {
    pub mmolb_player_id: &'a str,
    pub valid_from: NaiveDateTime,
    pub valid_until: Option<NaiveDateTime>,
    pub first_name: &'a str,
    pub last_name: &'a str,
    pub batting_handedness: Option<i64>,
    pub pitching_handedness: Option<i64>,
    pub home: &'a str,
    pub birthseason: i32,
    pub birthday_type: Option<i64>,
    pub birthday_day: Option<i32>,
    pub birthday_superstar_day: Option<i32>,
    pub likes: &'a str,
    pub dislikes: &'a str,
    pub number: i32,
    pub mmolb_team_id: Option<&'a str>,
    pub slot: Option<i64>,
    pub durability: f64,
    pub greater_boon: Option<i64>,
    pub lesser_boon: Option<i64>,
    pub num_modifications: i32,
    pub occupied_equipment_slots: Vec<&'a str>,
}

#[derive(Debug, Identifiable, Queryable, Selectable, QueryableByName)]
#[diesel(table_name = crate::data_schema::data::player_versions)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct DbPlayerVersion {
    pub id: i64,
    pub mmolb_player_id: String,
    pub valid_from: NaiveDateTime,
    pub valid_until: Option<NaiveDateTime>,
    pub first_name: String,
    pub last_name: String,
    pub batting_handedness: Option<i64>,
    pub pitching_handedness: Option<i64>,
    pub home: String,
    pub birthseason: i32,
    pub birthday_type: Option<i64>,
    pub birthday_day: Option<i32>,
    pub birthday_superstar_day: Option<i32>,
    pub likes: String,
    pub dislikes: String,
    pub number: i32,
    pub mmolb_team_id: Option<String>,
    pub slot: Option<i64>,
    pub durability: f64,
    pub greater_boon: Option<i64>,
    pub lesser_boon: Option<i64>,
    pub num_modifications: i32,
    pub occupied_equipment_slots: Vec<Option<String>>,
}

#[derive(Debug, Identifiable, Queryable, Selectable, QueryableByName)]
#[diesel(table_name = crate::data_schema::data::player_augments)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct DbPlayerAugment {
    pub id: i64,
    pub mmolb_player_id: String,
    pub feed_event_index: i32,
    pub time: NaiveDateTime,
    pub season: i32,
    pub day_type: Option<i64>,
    pub day: Option<i32>,
    pub superstar_day: Option<i32>,
    pub attribute: i64,
    pub value: i32,
}

#[derive(Clone, Debug, Insertable, PartialEq)]
#[diesel(table_name = crate::data_schema::data::player_augments)]
#[diesel(treat_none_as_default_value = false)]
pub struct NewPlayerAugment<'a> {
    pub mmolb_player_id: &'a str,
    pub feed_event_index: i32,
    pub time: NaiveDateTime,
    pub season: i32,
    pub day_type: Option<i64>,
    pub day: Option<i32>,
    pub superstar_day: Option<i32>,
    pub attribute: i64,
    pub value: i32,
}

#[derive(Debug, Identifiable, Queryable, Selectable, QueryableByName)]
#[diesel(table_name = crate::data_schema::data::player_paradigm_shifts)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct DbPlayerParadigmShift {
    pub id: i64,
    pub mmolb_player_id: String,
    pub feed_event_index: i32,
    pub time: NaiveDateTime,
    pub season: i32,
    pub day_type: Option<i64>,
    pub day: Option<i32>,
    pub superstar_day: Option<i32>,
    pub attribute: i64,
}

#[derive(Clone, Debug, Insertable, PartialEq)]
#[diesel(table_name = crate::data_schema::data::player_paradigm_shifts)]
#[diesel(treat_none_as_default_value = false)]
pub struct NewPlayerParadigmShift<'a> {
    pub mmolb_player_id: &'a str,
    pub feed_event_index: i32,
    pub time: NaiveDateTime,
    pub season: i32,
    pub day_type: Option<i64>,
    pub day: Option<i32>,
    pub superstar_day: Option<i32>,
    pub attribute: i64,
}

#[derive(Debug, Identifiable, Queryable, Selectable, QueryableByName)]
#[diesel(table_name = crate::data_schema::data::player_recompositions)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct DbPlayerRecomposition {
    pub id: i64,
    pub mmolb_player_id: String,
    pub feed_event_index: i32,
    pub inferred_event_index: Option<i32>,
    pub time: NaiveDateTime,
    pub season: i32,
    pub day_type: Option<i64>,
    pub day: Option<i32>,
    pub superstar_day: Option<i32>,
    pub player_name_before: String,
    pub player_name_after: String,
}

#[derive(Clone, Debug, Insertable, PartialEq)]
#[diesel(table_name = crate::data_schema::data::player_recompositions)]
#[diesel(treat_none_as_default_value = false)]
pub struct NewPlayerRecomposition<'a> {
    pub mmolb_player_id: &'a str,
    pub feed_event_index: i32,
    pub inferred_event_index: Option<i32>,
    pub time: NaiveDateTime,
    pub season: i32,
    pub day_type: Option<i64>,
    pub day: Option<i32>,
    pub superstar_day: Option<i32>,
    pub player_name_before: &'a str,
    pub player_name_after: &'a str,
}

#[derive(Debug, Identifiable, Queryable, Selectable, QueryableByName)]
#[diesel(table_name = crate::data_schema::data::player_reports)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct DbPlayerReport {
    pub id: i64,
    pub mmolb_player_id: String,
    pub season: i32,
    pub day_type: Option<i64>,
    pub day: Option<i32>,
    pub superstar_day: Option<i32>,
    pub observed: NaiveDateTime,
    pub attribute: i64,
    pub stars: i32,
}

#[derive(Clone, Debug, Insertable, PartialEq)]
#[diesel(table_name = crate::data_schema::data::player_reports)]
#[diesel(treat_none_as_default_value = false)]
pub struct NewPlayerReport<'a> {
    pub mmolb_player_id: &'a str,
    pub season: i32,
    pub day_type: Option<i64>,
    pub day: Option<i32>,
    pub superstar_day: Option<i32>,
    pub observed: NaiveDateTime,
    pub attribute: i64,
    pub stars: i32,
}

#[derive(Debug, Identifiable, Queryable, Selectable, QueryableByName)]
#[diesel(table_name = crate::data_schema::data::player_feed_versions)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct DbPlayerFeedVersion {
    pub id: i64,
    pub mmolb_player_id: String,
    pub valid_from: NaiveDateTime,
    pub valid_until: Option<NaiveDateTime>,
    pub num_entries: i32,
}

#[derive(Clone, Debug, Insertable, PartialEq)]
#[diesel(table_name = crate::data_schema::data::player_feed_versions)]
#[diesel(treat_none_as_default_value = false)]
pub struct NewPlayerFeedVersion<'a> {
    pub mmolb_player_id: &'a str,
    pub valid_from: NaiveDateTime,
    pub valid_until: Option<NaiveDateTime>,
    pub num_entries: i32,
}

#[derive(Debug, Identifiable, Queryable, Selectable, QueryableByName)]
#[diesel(table_name = crate::data_schema::data::player_equipment_versions)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct DbPlayerEquipmentVersion {
    pub id: i64,
    pub mmolb_player_id: String,
    pub equipment_slot: String,
    pub valid_from: NaiveDateTime,
    pub valid_until: Option<NaiveDateTime>,
    pub emoji: String,
    pub name: String,
    pub special_type: Option<String>,
    pub description: Option<String>,
    pub rare_name: Option<String>,
    pub cost: Option<i32>,
    pub prefixes: Vec<Option<String>>,
    pub suffixes: Vec<Option<String>>,
    pub rarity: Option<String>,
    pub num_effects: i32,
}

#[derive(Clone, Debug, Insertable, PartialEq)]
#[diesel(table_name = crate::data_schema::data::player_equipment_versions)]
#[diesel(treat_none_as_default_value = false)]
pub struct NewPlayerEquipmentVersion<'a> {
    pub mmolb_player_id: &'a str,
    pub equipment_slot: String,
    pub valid_from: NaiveDateTime,
    pub valid_until: Option<NaiveDateTime>,
    pub emoji: String,
    pub name: String,
    pub special_type: Option<String>,
    pub description: Option<String>,
    pub rare_name: Option<String>,
    pub cost: Option<i32>,
    pub prefixes: Vec<String>,
    pub suffixes: Vec<String>,
    pub rarity: Option<String>,
    pub num_effects: i32,
}

#[derive(Debug, Identifiable, Queryable, Selectable, QueryableByName)]
#[diesel(table_name = crate::data_schema::data::player_equipment_effect_versions)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct DbPlayerEquipmentEffectVersion {
    pub id: i64,
    pub mmolb_player_id: String,
    pub equipment_slot: String,
    pub effect_index: i32,
    pub valid_from: NaiveDateTime,
    pub valid_until: Option<NaiveDateTime>,
    pub attribute: i64,
    pub effect_type: i64,
    pub value: f64,
}

#[derive(Clone, Debug, Insertable, PartialEq)]
#[diesel(table_name = crate::data_schema::data::player_equipment_effect_versions)]
#[diesel(treat_none_as_default_value = false)]
pub struct NewPlayerEquipmentEffectVersion<'a> {
    pub mmolb_player_id: &'a str,
    pub equipment_slot: String,
    pub effect_index: i32,
    pub valid_from: NaiveDateTime,
    pub valid_until: Option<NaiveDateTime>,
    pub attribute: i64,
    pub effect_type: i64,
    pub value: f64,
}
