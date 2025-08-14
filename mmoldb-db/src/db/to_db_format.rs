use itertools::Itertools;
use crate::event_detail::{EventDetail, EventDetailFielder, EventDetailRunner};
use crate::models::{DbAuroraPhoto, DbEjection, DbEvent, DbFielder, DbRunner, NewAuroraPhoto, NewBaserunner, NewEjection, NewEvent, NewFielder};
use crate::taxa::Taxa;
use miette::Diagnostic;
use mmolb_parsing::parsed_event::{Cheer, Ejection, EjectionReason, EjectionReplacement, EmojiTeam, PlacedPlayer, SnappedPhotos, ViolationType};
use thiserror::Error;

pub fn event_to_row<'e>(
    taxa: &Taxa,
    game_id: i64,
    event: &'e EventDetail<&'e str>,
) -> NewEvent<'e> {
    NewEvent {
        game_id,
        game_event_index: event.game_event_index as i32,
        fair_ball_event_index: event.fair_ball_event_index.map(|i| i as i32),
        inning: event.inning as i32,
        top_of_inning: event.top_of_inning,
        event_type: taxa.event_type_id(event.detail_type),
        hit_base: event.hit_base.map(|ty| taxa.base_id(ty)),
        fair_ball_type: event.fair_ball_type.map(|ty| taxa.fair_ball_type_id(ty)),
        fair_ball_direction: event
            .fair_ball_direction
            .map(|ty| taxa.fielder_location(ty)),
        fielding_error_type: event
            .fielding_error_type
            .map(|ty| taxa.fielding_error_type_id(ty)),
        pitch_type: event.pitch_type.map(|ty| taxa.pitch_type_id(ty)),
        pitch_speed: event.pitch_speed,
        pitch_zone: event.pitch_zone,
        described_as_sacrifice: event.described_as_sacrifice,
        is_toasty: event.is_toasty,
        balls_before: event.balls_before as i32,
        strikes_before: event.strikes_before as i32,
        outs_before: event.outs_before,
        outs_after: event.outs_after,
        errors_before: event.errors_before,
        errors_after: event.errors_after,
        away_team_score_before: event.away_team_score_before as i32,
        away_team_score_after: event.away_team_score_after as i32,
        home_team_score_before: event.home_team_score_before as i32,
        home_team_score_after: event.home_team_score_after as i32,
        pitcher_name: event.pitcher_name,
        pitcher_count: event.pitcher_count,
        batter_name: event.batter_name,
        batter_count: event.batter_count,
        batter_subcount: event.batter_subcount,
        cheer: event.cheer.as_ref().map(|c| c.to_string()),
    }
}

pub fn event_to_baserunners<'e>(
    taxa: &Taxa,
    event_id: i64,
    event: &'e EventDetail<&'e str>,
) -> Vec<NewBaserunner<'e>> {
    event
        .baserunners
        .iter()
        .map(|runner| NewBaserunner {
            event_id,
            baserunner_name: runner.name,
            base_before: runner.base_before.map(|b| taxa.base_id(b)),
            base_after: taxa.base_id(runner.base_after),
            is_out: runner.is_out,
            base_description_format: runner
                .base_description_format
                .map(|f| taxa.base_description_format_id(f)),
            steal: runner.is_steal,
            source_event_index: runner.source_event_index.map(|idx| idx as i32),
            is_earned: runner.is_earned,
        })
        .collect()
}

pub fn event_to_fielders<'e>(
    taxa: &Taxa,
    event_id: i64,
    event: &'e EventDetail<&'e str>,
) -> Vec<NewFielder<'e>> {
    event
        .fielders
        .iter()
        .enumerate()
        .map(|(i, fielder)| NewFielder {
            event_id,
            fielder_name: fielder.name,
            fielder_slot: taxa.slot_id(fielder.slot),
            play_order: i as i32,
        })
        .collect()
}

pub fn event_to_aurora_photos<'e>(
    taxa: &Taxa,
    event_id: i64,
    event: &'e EventDetail<&'e str>,
) -> Vec<NewAuroraPhoto<'e>> {

    match &event.aurora_photos {
        None => Vec::new(),
        Some(photos) => vec![
            NewAuroraPhoto {
                is_listed_first: true,
                event_id,
                team_emoji: photos.first_team_emoji,
                player_slot: taxa.slot_id(photos.first_player.place.into()),
                player_name: photos.first_player.name,
            },
            NewAuroraPhoto {
                is_listed_first: false,
                event_id,
                team_emoji: photos.second_team_emoji,
                player_slot: taxa.slot_id(photos.second_player.place.into()),
                player_name: photos.second_player.name,
            }
        ]
    }
}

pub fn event_to_ejection<'e>(
    taxa: &Taxa,
    event_id: i64,
    event: &'e EventDetail<&'e str>,
) -> Vec<NewEjection<'e>> {

    match &event.ejection {
        None => Vec::new(),
        Some(photos) => vec![
            match photos.replacement {
                EjectionReplacement::BenchPlayer { player_name } => NewEjection {
                    event_id,
                    team_emoji: photos.team.emoji,
                    team_name: photos.team.name,
                    ejected_player_name: photos.ejected_player.name,
                    ejected_player_slot: taxa.slot_id(photos.ejected_player.place.into()),
                    violation_type: photos.violation_type.to_string(),
                    reason: photos.reason.to_string(),
                    replacement_player_name: player_name,
                    replacement_player_slot: None,
                },
                EjectionReplacement::RosterPlayer { player } => NewEjection {
                    event_id,
                    team_emoji: photos.team.emoji,
                    team_name: photos.team.name,
                    ejected_player_name: photos.ejected_player.name,
                    ejected_player_slot: taxa.slot_id(photos.ejected_player.place.into()),
                    violation_type: photos.violation_type.to_string(),
                    reason: photos.reason.to_string(),
                    replacement_player_name: player.name,
                    replacement_player_slot: Some(taxa.slot_id(player.place.into())),
                }
            }
        ]
    }
}

#[derive(Debug, Error, Diagnostic)]
pub enum RowToEventError {
    #[error("invalid event type id {0}")]
    InvalidEventTypeId(i64),

    #[error("invalid number of aurora photos on a single event (expected 0 or 2, not {0})")]
    InvalidNumberOfAuroraPhotos(usize),

    #[error("invalid number of ejections on a single event (expected 0 or 1, not {0})")]
    InvalidNumberOfEjections(usize),
}


pub fn row_to_event<'e>(
    taxa: &Taxa,
    event: DbEvent,
    runners: Vec<DbRunner>,
    fielders: Vec<DbFielder>,
    aurora_photo: Vec<DbAuroraPhoto>,
    ejection: Vec<DbEjection>,
) -> Result<EventDetail<String>, RowToEventError> {
    let baserunners = runners
        .into_iter()
        .map(|r| {
            assert_eq!(r.event_id, event.id);
            EventDetailRunner {
                name: r.baserunner_name,
                base_before: r.base_before.map(|id| taxa.base_from_id(id)),
                base_after: taxa.base_from_id(r.base_after),
                is_out: r.is_out,
                base_description_format: r
                    .base_description_format
                    .map(|id| taxa.base_description_format_from_id(id)),
                is_steal: r.steal,
                source_event_index: r.source_event_index,
                is_earned: r.is_earned,
            }
        })
        .collect();

    let fielders = fielders
        .into_iter()
        .map(|f| {
            assert_eq!(f.event_id, event.id);
            EventDetailFielder {
                name: f.fielder_name,
                slot: taxa.slot_from_id(f.fielder_slot).into(),
            }
        })
        .collect();

    let aurora_photos = match aurora_photo.len() {
        0 => None,
        2 => {
            let (first, second) = aurora_photo.into_iter().collect_tuple().unwrap();
            Some(SnappedPhotos {
                first_team_emoji: first.team_emoji,
                first_player: PlacedPlayer {
                    name: first.player_name,
                    place: taxa.slot_from_id(first.player_slot).into(),
                },
                second_team_emoji: second.team_emoji,
                second_player: PlacedPlayer {
                    name: second.player_name,
                    place: taxa.slot_from_id(second.player_slot).into(),
                },
            })
        },
        other => {
            return Err(RowToEventError::InvalidNumberOfAuroraPhotos(other));
        }
    };

    let ejection = match ejection.len() {
        0 => None,
        1 => {
            let (ejection,) = ejection.into_iter().collect_tuple().unwrap();
            Some(Ejection {
                team: EmojiTeam {
                    emoji: ejection.team_emoji,
                    name: ejection.team_name,
                },
                ejected_player: PlacedPlayer {
                    name: ejection.ejected_player_name,
                    place: taxa.slot_from_id(ejection.ejected_player_slot).into(),
                },
                violation_type: ViolationType::new(&ejection.violation_type),
                reason: EjectionReason::new(&ejection.reason),
                replacement: match ejection.replacement_player_slot {
                    None => EjectionReplacement::BenchPlayer {
                        player_name: ejection.replacement_player_name,
                    },
                    Some(replacement_player_slot) => EjectionReplacement::RosterPlayer {
                        player: PlacedPlayer {
                            name: ejection.replacement_player_name,
                            place: taxa.slot_from_id(replacement_player_slot).into(),
                        },
                    }
                },
            })
        },
        other => {
            return Err(RowToEventError::InvalidNumberOfEjections(other));
        }
    };

    Ok(EventDetail {
        game_event_index: event.game_event_index as usize,
        fair_ball_event_index: event.fair_ball_event_index.map(|i| i as usize),
        inning: event.inning as u8,
        top_of_inning: event.top_of_inning,
        balls_before: event.balls_before as u8,
        strikes_before: event.strikes_before as u8,
        outs_before: event.outs_before,
        outs_after: event.outs_after,
        errors_before: event.errors_before,
        errors_after: event.errors_after,
        away_team_score_before: event.away_team_score_before as u8,
        away_team_score_after: event.away_team_score_after as u8,
        home_team_score_before: event.home_team_score_before as u8,
        home_team_score_after: event.home_team_score_after as u8,
        batter_name: event.batter_name,
        pitcher_name: event.pitcher_name,
        detail_type: taxa
            .event_type_from_id(event.event_type)
            .ok_or_else(|| RowToEventError::InvalidEventTypeId(event.event_type))?,
        hit_base: event.hit_base.map(|id| taxa.base_from_id(id)),
        fair_ball_type: event
            .fair_ball_type
            .map(|id| taxa.fair_ball_type_from_id(id)),
        fair_ball_direction: event
            .fair_ball_direction
            .map(|id| taxa.fielder_location_from_id(id)),
        fielding_error_type: event
            .fielding_error_type
            .map(|id| taxa.fielding_error_type_from_id(id)),
        pitch_type: event.pitch_type.map(|id| taxa.pitch_type_from_id(id)),
        pitch_speed: event.pitch_speed,
        pitch_zone: event.pitch_zone,
        described_as_sacrifice: event.described_as_sacrifice,
        is_toasty: event.is_toasty,
        fielders,
        baserunners,
        pitcher_count: event.pitcher_count,
        batter_count: event.batter_count,
        batter_subcount: event.batter_subcount,
        cheer: event.cheer.as_deref().map(Cheer::new),
        aurora_photos,
        ejection,
    })
}
