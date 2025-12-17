use crate::event_detail::{EventDetail, EventDetailFielder, EventDetailRunner};
use crate::models::{DbAuroraPhoto, DbDoorPrize, DbDoorPrizeItem, DbEfflorescence, DbEfflorescenceGrowth, DbEjection, DbEvent, DbFailedEjection, DbFielder, DbRunner, DbWither, NewAuroraPhoto, NewBaserunner, NewConsumptionContest, NewConsumptionContestEvent, NewDoorPrize, NewDoorPrizeItem, NewEfflorescence, NewEfflorescenceGrowth, NewEjection, NewEvent, NewFailedEjection, NewFielder, NewParty, NewPitcherChange, NewWither};
use crate::taxa::Taxa;
use crate::{ConsumptionContestEventForDb, ConsumptionContestForDb, PartyEvent, PitcherChange, WitherOutcome};
use itertools::Itertools;
use miette::Diagnostic;
use mmolb_parsing::enums::{ItemName, ItemPrefix, ItemSuffix};
use mmolb_parsing::parsed_event::{Cheer, DoorPrize, Efflorescence, EfflorescenceOutcome, GrowAttributeChange, Ejection, EjectionReason, EjectionReplacement, EmojiTeam, Item, ItemAffixes, ItemEquip, ItemPrize, PlacedPlayer, Prize, SnappedPhotos, ViolationType, WitherStruggle};
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
        home_run_distance: event.home_run_distance,
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
        Some(Ejection::Ejection { team, ejected_player, violation_type, reason, replacement }) => vec![match replacement {
            EjectionReplacement::BenchPlayer { player_name } => NewEjection {
                event_id,
                team_emoji: team.emoji,
                team_name: team.name,
                ejected_player_name: ejected_player.name,
                ejected_player_slot: taxa.slot_id(ejected_player.place.into()),
                violation_type: violation_type.to_string(),
                reason: reason.to_string(),
                replacement_player_name: player_name,
                replacement_player_slot: None,
            },
            EjectionReplacement::RosterPlayer { player } => NewEjection {
                event_id,
                team_emoji: team.emoji,
                team_name: team.name,
                ejected_player_name: ejected_player.name,
                ejected_player_slot: taxa.slot_id(ejected_player.place.into()),
                violation_type: violation_type.to_string(),
                reason: reason.to_string(),
                replacement_player_name: player.name,
                replacement_player_slot: Some(taxa.slot_id(player.place.into())),
            },
        }],
        Some(Ejection::FailedEjection { .. }) => Vec::new(),
    }
}

pub fn event_to_failed_ejection<'e>(
    event_id: i64,
    event: &'e EventDetail<&'e str>,
) -> Vec<NewFailedEjection<'e>> {
    match &event.ejection {
        None => Vec::new(),
        Some(Ejection::Ejection { .. }) => Vec::new(),
        Some(Ejection::FailedEjection { player_names: [player_name_1, player_name_2] }) => vec![NewFailedEjection {
            event_id,
            player_name_1,
            player_name_2,
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
                .map(|(item_index, prize)| {
                    let (prefix, suffix, rare_name) = split_affixes(&prize.item);

                    let (equipped_by, (
                        discarded_item_emoji,
                        discarded_item_name,
                        discarded_item_rare_name,
                        discarded_item_prefix,
                        discarded_item_suffix,
                    ), prize_discarded) = match &prize.equip {
                        ItemEquip::Equipped { player_name, discarded_item } => {
                            let discarded_item = if let Some(discarded_item) = discarded_item {
                                let (prefix, suffix, rare_name) = split_affixes(discarded_item);
                                (Some(discarded_item.item_emoji), Some(discarded_item.item), rare_name, prefix, suffix)
                            } else {
                                (None, None, None, None, None)
                            };
                            (Some(*player_name), discarded_item, Some(false))
                        },
                        ItemEquip::Discarded => {
                            (None, (None, None, None, None, None), Some(true))
                        },
                        ItemEquip::None => {
                            (None, (None, None, None, None, None), None)
                        }
                    };

                    NewDoorPrizeItem {
                        event_id,
                        door_prize_index: door_prize_index as i32,
                        item_index: item_index as i32,
                        emoji: prize.item.item_emoji,
                        name: prize.item.item.into(),
                        rare_name,
                        prefix: prefix.map(Into::into),
                        suffix: suffix.map(Into::into),
                        equipped_by,
                        discarded_item_emoji,
                        discarded_item_name: discarded_item_name.map(Into::into),
                        discarded_item_rare_name,
                        discarded_item_prefix: discarded_item_prefix.map(Into::into),
                        discarded_item_suffix: discarded_item_suffix.map(Into::into),
                        prize_discarded,
                    }
                })
                .collect(),
        })
        .collect()
}

pub fn event_to_efflorescence<'e>(
    event_id: i64,
    event: &'e EventDetail<&'e str>,
) -> Vec<NewEfflorescence<'e>> {
    event
        .efflorescences
        .iter()
        .enumerate()
        .map(|(efflorescence_index, eff)| {
            let effloresced = match &eff.outcome {
                EfflorescenceOutcome::Grow(_) => false,
                EfflorescenceOutcome::Effloresce => true,
            };

            NewEfflorescence {
                event_id,
                efflorescence_index: efflorescence_index as i32,
                player_name: eff.player,
                effloresced,
            }
        })
        .collect()
}

pub fn event_to_efflorescence_growths<'e>(
    taxa: &Taxa,
    event_id: i64,
    event: &'e EventDetail<&'e str>,
) -> Vec<NewEfflorescenceGrowth> {
    event
        .efflorescences
        .iter()
        .enumerate()
        .flat_map(|(efflorescence_index, eff)| match &eff.outcome {
            EfflorescenceOutcome::Effloresce => Vec::new(),
            EfflorescenceOutcome::Grow(growths) => growths
                .iter()
                .enumerate()
                .map(|(growth_index, growth)| {
                    NewEfflorescenceGrowth {
                        event_id,
                        efflorescence_index: efflorescence_index as i32,
                        growth_index: growth_index as i32,
                        value: growth.amount,
                        attribute: taxa.attribute_id(growth.attribute.into()),
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
            durability_loss: party.pitcher_durability_loss,
        },
        NewParty {
            game_id,
            game_event_index: party.game_event_index as i32,
            is_pitcher: false,
            top_of_inning: party.top_of_inning,
            player_name: party.batter_name,
            attribute: taxa.attribute_id(party.batter_attribute),
            value: party.batter_amount_gained,
            durability_loss: party.batter_durability_loss,
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
        attempt_game_event_index: wither.attempt_game_event_index,
        outcome_game_event_index: wither.outcome_game_event_index,
        team_emoji: wither.team_emoji,
        player_slot: taxa.slot_id(wither.player_slot),
        player_name: wither.player_name,
        source_player_name: wither.source_player_name,
        corrupted: wither.corrupted,
        contain_attempted: wither.contain_attempted,
        contain_replacement_player_name: wither.contain_replacement_player_name,
    }
}

pub fn consumption_contest_to_rows<'e>(
    game_id: i64,
    contest: &'e ConsumptionContestForDb<&'e str>,
) -> NewConsumptionContest<'e> {
    NewConsumptionContest {
        game_id,
        first_game_event_index: contest.first_game_event_index as i32,
        last_game_event_index: contest.last_game_event_index as i32,
        food_emoji: contest.food.food_emoji,
        food: contest.food.food.into(),
        batting_team_mmolb_id: contest.batting.team_mmolb_id,
        batting_team_emoji: contest.batting.team.emoji,
        batting_team_name: contest.batting.team.name,
        batting_team_player_name: contest.batting.player_name,
        batting_team_tokens: contest.batting.tokens as i32,
        batting_team_prize_emoji: contest.batting.prize.map(|p| p.item_emoji),
        batting_team_prize_name: contest.batting.prize.map(|p| p.item.into()),
        batting_team_prize_rare_name: contest.batting.prize.and_then(|p| if let ItemAffixes::RareName(n) = p.affixes { Some(n) } else { None }),
        batting_team_prize_prefix: contest.batting.prize.and_then(|p| if let ItemAffixes::PrefixSuffix(pre, _) = p.affixes { pre.map(Into::into) } else { None }),
        batting_team_prize_suffix: contest.batting.prize.and_then(|p| if let ItemAffixes::PrefixSuffix(_, suf) = p.affixes { suf.map(Into::into) } else { None }),
        defending_team_mmolb_id: contest.defending.team_mmolb_id,
        defending_team_emoji: contest.defending.team.emoji,
        defending_team_name: contest.defending.team.name,
        defending_team_player_name: contest.defending.player_name,
        defending_team_tokens: contest.defending.tokens as i32,
        defending_team_prize_emoji: contest.defending.prize.map(|p| p.item_emoji),
        defending_team_prize_name: contest.defending.prize.map(|p| p.item.into()),
        defending_team_prize_rare_name: contest.defending.prize.and_then(|p| if let ItemAffixes::RareName(n) = p.affixes { Some(n) } else { None }),
        defending_team_prize_prefix: contest.defending.prize.and_then(|p| if let ItemAffixes::PrefixSuffix(pre, _) = p.affixes { pre.map(Into::into) } else { None }),
        defending_team_prize_suffix: contest.defending.prize.and_then(|p| if let ItemAffixes::PrefixSuffix(_, suf) = p.affixes { suf.map(Into::into) } else { None }),
    }
}

pub fn consumption_contest_event_to_rows(
    game_id: i64,
    first_game_event_index: usize,
    contest: &ConsumptionContestEventForDb,
) -> NewConsumptionContestEvent {
    NewConsumptionContestEvent {
        game_id,
        first_game_event_index: first_game_event_index as i32,
        game_event_index: contest.game_event_index as i32,
        batting_team_consumed: contest.batting_consumed as i32,
        defending_team_consumed: contest.defending_consumed as i32,
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

    #[error("ejection and failed ejection on a single event")]
    EjectionAndFailedEjection,

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

    #[error(
        "{efflorescence_index}th efflorescence, for {player_name}, had too few growths. Expected \
        2, received {growths:?}"
    )]
    NotEnoughEfflorescenceGrowths {
        efflorescence_index: i32,
        player_name: String,
        growths: Vec<GrowAttributeChange>,
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
    failed_ejection: Vec<DbFailedEjection>,
    door_prizes: Vec<DbDoorPrize>,
    door_prize_items: Vec<DbDoorPrizeItem>,
    efflorescences: Vec<DbEfflorescence>,
    efflorescence_growths: Vec<DbEfflorescenceGrowth>,
    wither: Vec<DbWither>,
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
            Some(Ejection::Ejection {
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

    let failed_ejection = match failed_ejection.len() {
        0 => None,
        1 => {
            let (failed_ejection,) = failed_ejection.into_iter().collect_tuple().unwrap();
            Some(Ejection::FailedEjection {
                player_names: [failed_ejection.player_name_1, failed_ejection.player_name_2],
            })
        }
        other => {
            return Err(RowToEventError::InvalidNumberOfEjections(other));
        }
    };
    
    let ejection = match (ejection, failed_ejection) {
        (None, None) => None,
        (Some(e), None) => Some(e),
        (None, Some(e)) => Some(e),
        (Some(_), Some(_)) => return Err(RowToEventError::EjectionAndFailedEjection),
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

                                let equip = if let Some(player_name) = prize_item.equipped_by {
                                    if prize_item.prize_discarded.is_none() {
                                        warn!(
                                            "Got a None prize_discarded and a Some player_name. \
                                            This should not happen because None prize_discarded \
                                            means that this item went straight to the inventory."
                                        );
                                    } else if let Some(true) = prize_item.prize_discarded {
                                        warn!(
                                            "Got a true prize_discarded and a Some equipped_by, \
                                            which is contradictory."
                                        );
                                    }

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

                                    ItemEquip::Equipped { player_name, discarded_item }
                                } else {
                                    if prize_item.discarded_item_emoji.is_some() {
                                        warn!("Got a discarded item emoji without an equipping player");
                                    }
                                    if prize_item.discarded_item_name.is_some() {
                                        warn!("Got a discarded item name without an equipping player");
                                    }

                                    match prize_item.prize_discarded {
                                        None => ItemEquip::None,
                                        Some(true) => ItemEquip::Discarded,
                                        Some(false) => {
                                            warn!(
                                                "Got a None equipped_by and a Some(false) \
                                                prize_discarded, which is an invalid combination."
                                            );
                                            // There is no sensible value to return here, because
                                            // this is an invalid combination, but this probably
                                            // the least surprising value.
                                            ItemEquip::None
                                        },
                                    }
                                };

                                Ok(ItemPrize {
                                    item,
                                    equip,
                                })
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

    for remaining_door_prize_item in door_prize_items_iter {
        warn!("Door prize item abandoned: {remaining_door_prize_item:?}");
    }

    let mut efflorescence_growths_iter = efflorescence_growths.into_iter().peekable();

    let efflorescences = efflorescences
        .into_iter()
        .map(|efflorescence| {
            let mut growths = Vec::new();
            while let Some(growth) =
                efflorescence_growths_iter.next_if(|i| i.efflorescence_index == efflorescence.efflorescence_index)
            {
                growths.push(GrowAttributeChange {
                    attribute: taxa.attribute_from_id(growth.attribute).into(),
                    amount: growth.value,
                });
            }

            if efflorescence.effloresced {
                if !growths.is_empty() {
                    warn!("Efflorescence had effloresced=true and growth items {:?}", growths);
                }

                Ok(Efflorescence {
                    player: efflorescence.player_name,
                    outcome: EfflorescenceOutcome::Effloresce,
                })
            } else {
                let growths = if growths.len() > 2 {
                    warn!("Efflorescence had more than 2 growths {:?}", growths);
                    [growths[0], growths[1]]
                } else if growths.len() == 2 {
                    [growths[0], growths[1]]
                } else {
                    return Err(RowToEventError::NotEnoughEfflorescenceGrowths {
                        efflorescence_index: efflorescence.efflorescence_index,
                        player_name: efflorescence.player_name,
                        growths,
                    });
                };

                Ok(Efflorescence {
                    player: efflorescence.player_name,
                    outcome: EfflorescenceOutcome::Grow(growths),
                })
            }
        })
        .collect::<Result<Vec<_>, _>>()?;

    for remaining_efflorescence_growth in efflorescence_growths_iter {
        warn!("Efflorescence growth abandoned: {remaining_efflorescence_growth:?}");
    }

    let wither = match wither.len() {
        0 => None,
        1 => {
            let (wither,) = wither.into_iter().collect_tuple().unwrap();
            Some(WitherStruggle {
                team_emoji: wither.team_emoji,
                target: PlacedPlayer {
                    name: wither.player_name,
                    place: taxa.slot_from_id(wither.player_slot).into(),
                },
                source_name: wither.source_player_name,
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
        home_run_distance: event.home_run_distance,
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
        efflorescences,
    })
}
