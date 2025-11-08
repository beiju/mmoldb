use crate::event_detail::{EventDetail, EventDetailFielder, EventDetailRunner};
use crate::models::{DbAuroraPhoto, DbDoorPrize, DbDoorPrizeItem, DbEjection, DbEvent, DbFielder, DbRunner, DbWither, NewAuroraPhoto, NewBaserunner, NewDoorPrize, NewDoorPrizeItem, NewEjection, NewEvent, NewFielder, NewParty, NewPitcherChange, NewWither};
use crate::taxa::Taxa;
use crate::{PartyEvent, PitcherChange, WitherOutcome};
use itertools::Itertools;
use miette::Diagnostic;
use mmolb_parsing::enums::{ItemName, ItemPrefix, ItemSuffix};
use mmolb_parsing::parsed_event::{Cheer, DoorPrize, Ejection, EjectionReason, EjectionReplacement, EmojiTeam, Item, ItemAffixes, PlacedPlayer, Prize, SnappedPhotos, ViolationType, WitherStruggle};
use std::str::FromStr;
use log::warn;
use strum::ParseError;
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
            .map(|ty| taxa.fielder_location_id(ty)),
        fair_ball_fielder_name: event.fair_ball_fielder_name,
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
            },
        ],
    }
}

pub fn event_to_ejection<'e>(
    taxa: &Taxa,
    event_id: i64,
    event: &'e EventDetail<&'e str>,
) -> Vec<NewEjection<'e>> {
    match &event.ejection {
        None => Vec::new(),
        Some(photos) => vec![match photos.replacement {
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
            },
        }],
    }
}

pub fn event_to_door_prize<'e>(
    event_id: i64,
    event: &'e EventDetail<&'e str>,
) -> Vec<NewDoorPrize<'e>> {
    event
        .door_prizes
        .iter()
        .enumerate()
        .map(|(door_prize_index, prize)| NewDoorPrize {
            event_id,
            door_prize_index: door_prize_index as i32,
            player_name: prize.player,
            tokens: match prize.prize {
                None => None,
                Some(Prize::Tokens(num)) => Some(num as i32),
                Some(Prize::Items(_)) => None,
            },
        })
        .collect()
}

pub fn split_affixes<'e>(item: &Item<&'e str>) -> (Option<ItemPrefix>, Option<ItemSuffix>, Option<&'e str>) {
    match item.affixes {
        ItemAffixes::None => (None, None, None),
        ItemAffixes::PrefixSuffix(prefix, suffix) => (prefix, suffix, None),
        ItemAffixes::RareName(rare_name) => (None, None, Some(rare_name)),
    }
}

pub fn event_to_door_prize_items<'e>(
    event_id: i64,
    event: &'e EventDetail<&'e str>,
) -> Vec<NewDoorPrizeItem<'e>> {
    event
        .door_prizes
        .iter()
        .enumerate()
        .flat_map(|(door_prize_index, prize)| match &prize.prize {
            None | Some(Prize::Tokens(_)) => Vec::new(),
            Some(Prize::Items(items)) => items
                .iter()
                .enumerate()
                .map(|(item_index, (item, equipped))| {
                    let (prefix, suffix, rare_name) = split_affixes(item);

                    let (equipped_by, (
                        discarded_item_emoji,
                        discarded_item_name,
                        discarded_item_rare_name,
                        discarded_item_prefix,
                        discarded_item_suffix,
                    )) = if let Some((equipped_by, discarded_item)) = equipped {
                        let discarded_item = if let Some(discarded_item) = discarded_item {
                            let (prefix, suffix, rare_name) = split_affixes(discarded_item);
                            (Some(discarded_item.item_emoji), Some(discarded_item.item), rare_name, prefix, suffix)
                        } else {
                            (None, None, None, None, None)
                        };
                        (Some(*equipped_by), discarded_item)
                    } else {
                        (None, (None, None, None, None, None))
                    };

                    NewDoorPrizeItem {
                        event_id,
                        door_prize_index: door_prize_index as i32,
                        item_index: item_index as i32,
                        emoji: item.item_emoji,
                        name: item.item.into(),
                        rare_name,
                        prefix: prefix.map(Into::into),
                        suffix: suffix.map(Into::into),
                        equipped_by,
                        discarded_item_emoji,
                        discarded_item_name: discarded_item_name.map(Into::into),
                        discarded_item_rare_name,
                        discarded_item_prefix: discarded_item_prefix.map(Into::into),
                        discarded_item_suffix: discarded_item_suffix.map(Into::into),
                    }
                })
                .collect(),
        })
        .collect()
}

pub fn pitcher_change_to_row<'e>(
    taxa: &Taxa,
    game_id: i64,
    pitcher_change: &'e PitcherChange<&'e str>,
) -> NewPitcherChange<'e> {
    NewPitcherChange {
        game_id,
        game_event_index: pitcher_change.game_event_index as i32,
        previous_game_event_index: pitcher_change.previous_game_event_index.map(|i| i as i32),
        source: taxa.pitcher_change_source_id(pitcher_change.source),
        inning: pitcher_change.inning as i32,
        top_of_inning: pitcher_change.top_of_inning,
        pitcher_count: pitcher_change.pitcher_count,
        pitcher_name: pitcher_change.pitcher_name,
        pitcher_slot: taxa.slot_id(pitcher_change.pitcher_slot),
        new_pitcher_name: pitcher_change.new_pitcher_name,
        new_pitcher_slot: pitcher_change.new_pitcher_slot.map(|s| taxa.slot_id(s)),
    }
}

pub fn party_to_rows<'e>(
    taxa: &Taxa,
    game_id: i64,
    party: &'e PartyEvent<&'e str>,
) -> [NewParty<'e>; 2] {
    [
        NewParty {
            game_id,
            game_event_index: party.game_event_index as i32,
            is_pitcher: true,
            top_of_inning: party.top_of_inning,
            player_name: party.pitcher_name,
            attribute: taxa.attribute_id(party.pitcher_attribute),
            value: party.pitcher_amount_gained,
        },
        NewParty {
            game_id,
            game_event_index: party.game_event_index as i32,
            is_pitcher: false,
            top_of_inning: party.top_of_inning,
            player_name: party.batter_name,
            attribute: taxa.attribute_id(party.batter_attribute),
            value: party.batter_amount_gained,
        },
    ]
}

pub fn wither_to_rows<'e>(
    taxa: &Taxa,
    game_id: i64,
    wither: &'e WitherOutcome<&'e str>,
) -> NewWither<'e> {
    NewWither {
        game_id,
        struggle_game_event_index: wither.struggle_game_event_index,
        outcome_game_event_index: wither.outcome_game_event_index,
        team_emoji: wither.team_emoji,
        player_position: taxa.slot_id(wither.player_position),
        player_name: wither.player_name,
        corrupted: wither.corrupted,
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

    #[error("invalid number of wither struggles on a single event (expected 0 or 1, not {0})")]
    InvalidNumberOfWitherStruggles(usize),

    #[error(
        "{item_index}th prize {door_prize_index} for {player_name} failed to parse item name \
        (discarded: {discarded}): {parse_error}"
    )]
    InvalidItemNameInPrize {
        door_prize_index: i32, // index of this prize within the event
        item_index: i32,       // index of this item within the prize
        discarded: bool,       // whether this error comes from the discarded item
        player_name: String,
        parse_error: ParseError,
    },

    #[error(
        "{item_index}th prize {door_prize_index} for {player_name} prefix was null \
        (discarded: {discarded})"
    )]
    NullPrizeItemPrefix {
        door_prize_index: i32, // index of this prize within the event
        item_index: i32,       // index of this item within the prize
        discarded: bool,       // whether this error comes from the discarded item
        player_name: String,
    },

    #[error(
        "{item_index}th prize {door_prize_index} for {player_name} prefix failed to parse \
        (discarded: {discarded}): {parse_error}"
    )]
    InvalidPrizeItemPrefix {
        door_prize_index: i32, // index of this prize within the event
        item_index: i32,       // index of this item within the prize
        discarded: bool,       // whether this error comes from the discarded item
        player_name: String,
        parse_error: ParseError,
    },

    #[error(
        "{item_index}th prize {door_prize_index} for {player_name} suffix failed to parse \
        (discarded: {discarded}): {parse_error}"
    )]
    InvalidPrizeItemSuffix {
        door_prize_index: i32, // index of this prize within the event
        item_index: i32,       // index of this item within the prize
        discarded: bool,       // whether this error comes from the discarded item
        player_name: String,
        parse_error: ParseError,
    },

    #[error(
        "{item_index}th prize {door_prize_index} for {player_name} had both {num_tokens} tokens \
        and items {items:?}"
    )]
    SamePrizeHadTokensAndItems {
        door_prize_index: i32, // index of this prize within the event
        item_index: i32,       // index of this item within the prize
        player_name: String,
        num_tokens: i32,
        items: Vec<String>,
    },
}

pub fn build_item<'e>(
    door_prize_index: i32,
    item_index: i32,
    discarded: bool,
    player_name: &str,
    emoji: String,
    name: String,
    rare_name: Option<String>,
    prefix: Option<String>,
    suffix: Option<String>,
) -> Result<Item<String>, RowToEventError> {
    Ok(Item {
        item_emoji: emoji,
        item: match ItemName::from_str(&name) {
            Ok(name) => name,
            Err(parse_error) => {
                return Err(RowToEventError::InvalidItemNameInPrize {
                    door_prize_index,
                    item_index,
                    discarded,
                    player_name: player_name.to_string(),
                    parse_error,
                });
            }
        },
        affixes: if let Some(rare_name) = rare_name {
            // TODO Warn if there's also prefixes and suffixes
            ItemAffixes::RareName(rare_name)
        } else {
            match (prefix, suffix) {
                (None, None) => ItemAffixes::None,
                (prefix, suffix) => {
                    let prefix: Option<ItemPrefix> = prefix
                        .as_deref()
                        .map(ItemPrefix::from_str)
                        .transpose()
                        .map_err(|parse_error| {
                            RowToEventError::InvalidPrizeItemPrefix {
                                door_prize_index,
                                item_index,
                                discarded,
                                player_name: player_name.to_string(),
                                parse_error,
                            }
                        })?;

                    let suffix: Option<ItemSuffix> = suffix
                        .as_deref()
                        .map(ItemSuffix::from_str)
                        .transpose()
                        .map_err(|parse_error| {
                            RowToEventError::InvalidPrizeItemSuffix {
                                door_prize_index,
                                item_index,
                                discarded,
                                player_name: player_name.to_string(),
                                parse_error,
                            }
                        })?;

                    ItemAffixes::PrefixSuffix(prefix, suffix)
                }
            }
        },
    })
}

pub fn row_to_event<'e>(
    taxa: &Taxa,
    event: DbEvent,
    runners: Vec<DbRunner>,
    fielders: Vec<DbFielder>,
    aurora_photo: Vec<DbAuroraPhoto>,
    ejection: Vec<DbEjection>,
    door_prizes: Vec<DbDoorPrize>,
    door_prize_items: Vec<DbDoorPrizeItem>,
    wither: Vec<DbWither>
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
        }
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
                    },
                },
            })
        }
        other => {
            return Err(RowToEventError::InvalidNumberOfEjections(other));
        }
    };

    let mut door_prize_items_iter = door_prize_items.into_iter().peekable();

    let door_prizes = door_prizes
        .into_iter()
        .map(|door_prize| {
            let mut prize_items = Vec::new();
            while let Some(item_prize) =
                door_prize_items_iter.next_if(|i| i.door_prize_index == door_prize.door_prize_index)
            {
                prize_items.push(item_prize);
            }

            let prize = match (door_prize.tokens, prize_items) {
                (None, prize_items) if prize_items.is_empty() => None,
                (None, prize_items) => {
                    Some(Ok(Prize::Items(
                        prize_items
                            .into_iter()
                            .map(|prize_item| {
                                let item = build_item(
                                    prize_item.door_prize_index,
                                    prize_item.item_index,
                                    false,
                                    &door_prize.player_name,
                                    prize_item.emoji,
                                    prize_item.name,
                                    prize_item.rare_name,
                                    prize_item.prefix,
                                    prize_item.suffix,
                                )?;

                                let equip = if let Some(equipper_name) = prize_item.equipped_by {
                                    let discarded_item = match (prize_item.discarded_item_emoji, prize_item.discarded_item_name) {
                                        (None, None) => None,
                                        (Some(_), None) => {
                                            warn!("Got a discarded item emoji without an equipping player");
                                            None
                                        },
                                        (None, Some(_)) => {
                                            warn!("Got a discarded item name without an equipping player");
                                            None
                                        },
                                        (Some(emoji), Some(name)) => Some(build_item(
                                            prize_item.door_prize_index,
                                            prize_item.item_index,
                                            true,
                                            &door_prize.player_name,
                                            emoji,
                                            name,
                                            prize_item.discarded_item_rare_name,
                                            prize_item.discarded_item_prefix,
                                            prize_item.discarded_item_suffix,
                                        )?)
                                    };

                                    Some((equipper_name, discarded_item))
                                } else {
                                    if prize_item.discarded_item_emoji.is_some() {
                                        warn!("Got a discarded item emoji without an equipping player");
                                    }
                                    if prize_item.discarded_item_name.is_some() {
                                        warn!("Got a discarded item name without an equipping player");
                                    }
                                    None
                                };

                                Ok((item, equip))
                            })
                            .collect::<Result<Vec<_>, _>>()?,
                    )))
                }
                (Some(tokens), prize_items) if prize_items.is_empty() => {
                    Some(Ok(Prize::Tokens(tokens as u16)))
                }
                (Some(tokens), prize_items) => {
                    Some(Err(RowToEventError::SamePrizeHadTokensAndItems {
                        door_prize_index: door_prize.door_prize_index,
                        item_index: 0,
                        player_name: door_prize.player_name.clone(),
                        num_tokens: tokens,
                        items: prize_items
                            .into_iter()
                            .map(|prize_item| {
                                format!(
                                    "{:?}",
                                    prize_item // TODO Print prettily
                                )
                            })
                            .collect(),
                    }))
                }
            }
            .transpose()?;

            Ok(DoorPrize {
                player: door_prize.player_name,
                prize,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let wither = match wither.len() {
        0 => None,
        1 => {
            let (wither,) = wither.into_iter().collect_tuple().unwrap();
            Some(WitherStruggle {
                team_emoji: wither.team_emoji,
                player: PlacedPlayer {
                    name: wither.player_name,
                    place: taxa.slot_from_id(wither.player_position).into(),
                },
            })
        },
        other => {
            return Err(RowToEventError::InvalidNumberOfWitherStruggles(other));
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
        fair_ball_fielder_name: event.fair_ball_fielder_name,
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
        door_prizes,
        wither,
    })
}
