use crate::taxa::{
    TaxaBase, TaxaBaseDescriptionFormat, TaxaEventType, TaxaFairBallType, TaxaFielderLocation,
    TaxaFieldingErrorType, TaxaPitchType, TaxaSlot,
};

#[derive(Debug, Clone)]
pub struct EventDetailRunner<StrT: Clone> {
    pub name: StrT,
    pub base_before: Option<TaxaBase>,
    pub base_after: TaxaBase,
    pub is_out: bool,
    pub base_description_format: Option<TaxaBaseDescriptionFormat>,
    pub is_steal: bool,
}

#[derive(Debug, Clone)]
pub struct EventDetailFielder<StrT: Clone> {
    pub name: StrT,
    pub slot: TaxaSlot,
}

#[derive(Debug, Clone)]
pub struct EventDetail<StrT: Clone> {
    pub game_event_index: usize,
    pub fair_ball_event_index: Option<usize>,
    pub inning: u8,
    pub top_of_inning: bool,
    pub balls_before: u8,
    pub strikes_before: u8,
    pub outs_before: i32,
    pub outs_after: i32,
    pub away_team_score_before: u8,
    pub away_team_score_after: u8,
    pub home_team_score_before: u8,
    pub home_team_score_after: u8,
    pub pitcher_name: StrT,
    pub batter_name: StrT,
    pub fielders: Vec<EventDetailFielder<StrT>>,

    pub detail_type: TaxaEventType,
    pub hit_base: Option<TaxaBase>,
    pub fair_ball_type: Option<TaxaFairBallType>,
    pub fair_ball_direction: Option<TaxaFielderLocation>,
    pub fielding_error_type: Option<TaxaFieldingErrorType>,
    pub pitch_type: Option<TaxaPitchType>,
    pub pitch_speed: Option<f64>,
    pub pitch_zone: Option<i32>,
    pub described_as_sacrifice: Option<bool>,
    pub is_toasty: Option<bool>,

    pub baserunners: Vec<EventDetailRunner<StrT>>,
    pub pitcher_count: i32,
    pub batter_count: i32,
    pub batter_subcount: i32,
}

#[derive(Debug)]
pub struct IngestLog {
    pub game_event_index: i32,
    pub log_level: i32,
    pub log_text: String,
}
