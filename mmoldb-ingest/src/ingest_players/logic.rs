use std::fmt::Display;
use chron::ChronEntity;
use chrono::{NaiveDateTime, Utc};
use hashbrown::HashMap;
use itertools::Itertools;
use log::{debug, error, info, warn};
use miette::IntoDiagnostic;
use mmolb_parsing::{AddedLater, RemovedLater, NotRecognized, MaybeRecognizedResult, RemovedLaterResult, AddedLaterResult};
use mmolb_parsing::enums::{Day, EquipmentSlot, Handedness, Position};
use mmolb_parsing::player::{PlayerEquipment, TalkCategory};
use mmoldb_db::db::NameEmojiTooltip;
use mmoldb_db::models::{NewPlayerAttributeAugment, NewPlayerEquipmentEffectVersion, NewPlayerEquipmentVersion, NewPlayerFeedVersion, NewPlayerModificationVersion, NewPlayerParadigmShift, NewPlayerRecomposition, NewPlayerReportVersion, NewPlayerReportAttributeVersion, NewPlayerVersion, NewVersionIngestLog};
use mmoldb_db::taxa::{Taxa, TaxaAttributeCategory, TaxaDayType, TaxaSlot};
use mmoldb_db::{PgConnection, QueryResult, db};
use rayon::prelude::*;
use thiserror::Error;
use crate::ingest::{batch_by_entity, IngestFatalError, VersionIngestLogs};
use crate::ingest_feed::chron_player_feed_as_new;
use crate::ingest_players::PLAYER_KIND;

pub fn ingest_page_of_players(
    taxa: &Taxa,
    raw_players: Vec<ChronEntity<serde_json::Value>>,
    conn: &mut PgConnection,
    worker_id: usize,
) -> miette::Result<i32> {
    debug!(
        "Starting ingest page of {} players on worker {worker_id}",
        raw_players.len()
    );
    let save_start = Utc::now();

    let deserialize_start = Utc::now();
    // TODO Gracefully handle player deserialize failure
    let players = raw_players
        .into_par_iter()
        .map(|game_json| {
            Ok::<ChronEntity<mmolb_parsing::player::Player>, serde_json::Error>(ChronEntity {
                kind: game_json.kind,
                entity_id: game_json.entity_id,
                valid_from: game_json.valid_from,
                valid_to: game_json.valid_to,
                data: serde_json::from_value(game_json.data)?,
            })
        })
        .collect::<Result<Vec<_>, _>>().into_diagnostic()?;
    let deserialize_duration = (Utc::now() - deserialize_start).as_seconds_f64();
    debug!(
        "Deserialized page of {} players in {deserialize_duration:.2} seconds on worker {worker_id}",
        players.len()
    );

    let latest_time = players
        .last()
        .map(|version| version.valid_from)
        .unwrap_or(Utc::now());
    let time_ago = latest_time.signed_duration_since(Utc::now());
    let human_time_ago = chrono_humanize::HumanTime::from(time_ago);

    // Collect all modifications that appear in this batch so we can ensure they're all added
    let unique_modifications = players
        .iter()
        .flat_map(|version| {
            version
                .data
                .modifications
                .iter()
                .chain(version.data.lesser_boon.as_ref())
                .chain(version.data.greater_boon.as_ref())
                .map(|m| {
                    // TODO Do this for extra_fields in other types
                    if !m.extra_fields.is_empty() {
                        warn!(
                            "Modification had extra fields that were not captured: {:?}",
                            m.extra_fields
                        );
                    }
                    (m.name.as_str(), m.emoji.as_str(), m.description.as_str())
                })
        })
        .unique()
        .collect_vec();

    let modifications = get_filled_modifications_map(conn, &unique_modifications).into_diagnostic()?;

    // Convert to Insertable type
    let new_players = players
        .iter()
        .map(|v| chron_player_as_new(&v, &taxa, &modifications))
        .collect_vec();

    // The handling of valid_until is entirely in the database layer, but its logic
    // requires that a given batch of players does not have the same player twice. We
    // provide that guarantee here.
    let mut stored_batch: Option<HashMap<&str, NaiveDateTime>> = None;
    let mut total_inserted = 0;
    for batch in batch_by_entity(new_players, |v| v.0.mmolb_player_id) {
        let prev_batch = stored_batch.take();
        let new_stored_batch = stored_batch.insert(HashMap::new());
        for (player, _, _, _, _, _) in &batch {
            if let Some(prev_batch) = &prev_batch {
                if let Some(prev_valid_from) = prev_batch.get(player.mmolb_player_id) {
                    assert!(&player.valid_from >= prev_valid_from);
                }
            }
            let prev = new_stored_batch.insert(player.mmolb_player_id, player.valid_from);
            assert!(prev.is_none());
        }

        let to_insert = batch.len();
        info!(
            "Sent {} new player versions out of {} to the database.",
            to_insert,
            players.len(),
        );

        let (inserted, errs) = db::insert_player_versions_with_retry(conn, &batch);
        total_inserted += inserted as i32;

        for (player, err) in errs {
            let (
                version,
                _modification_version,
                _feed_version,
                _report,
                _equipment,
                _logs,
            ): &(NewPlayerVersion, _, _, _, _, _) = player;
            error!(
                "Error {err} ingesting player {} at {}",
                version.mmolb_player_id, version.valid_from,
            );
        }

        info!(
            "Sent {} new player versions out of {} to the database. \
            {inserted} versions were actually inserted, the rest were duplicates. \
            Currently processing players from {human_time_ago}.",
            to_insert,
            players.len(),
        );
    }

    let save_duration = (Utc::now() - save_start).as_seconds_f64();

    info!(
        "Ingested page of {} players in {save_duration:.3} seconds.",
        players.len(),
    );

    Ok(total_inserted)
}

pub fn get_filled_modifications_map(
    conn: &mut PgConnection,
    modifications_to_ensure: &[(&str, &str, &str)],
) -> QueryResult<HashMap<NameEmojiTooltip, i64>> {
    // Put everything in a loop to handle insert conflicts with other
    // ingest threads
    Ok(loop {
        let mut modifications = db::get_modifications_table(conn)?;

        let modifications_to_add = modifications_to_ensure
            .iter()
            .filter(|key| !modifications.contains_key(*key))
            .collect_vec();

        if modifications_to_add.is_empty() {
            break modifications;
        }

        match db::insert_modifications(conn, modifications_to_add.as_slice())? {
            None => {
                // Indicates that we should try again
                warn!("Conflict inserting modifications; trying again");
                continue;
            }
            Some(new_values) => {
                modifications.extend(new_values);

                // For debugging only; remove once we're sure it works
                for m in modifications_to_ensure {
                    assert!(modifications.contains_key(m));
                }

                break modifications;
            }
        }
    })
}

pub fn day_to_db(
    day: &Result<Day, NotRecognized>,
    taxa: &Taxa,
) -> (Option<i64>, Option<i32>, Option<i32>) {
    match day {
        Ok(Day::Preseason) => (Some(taxa.day_type_id(TaxaDayType::Preseason)), None, None),
        Ok(Day::SuperstarBreak) => (
            Some(taxa.day_type_id(TaxaDayType::SuperstarBreak)),
            None,
            None,
        ),
        Ok(Day::PostseasonPreview) => (
            Some(taxa.day_type_id(TaxaDayType::PostseasonPreview)),
            None,
            None,
        ),
        Ok(Day::PostseasonRound(1)) => (
            Some(taxa.day_type_id(TaxaDayType::PostseasonRound1)),
            None,
            None,
        ),
        Ok(Day::PostseasonRound(2)) => (
            Some(taxa.day_type_id(TaxaDayType::PostseasonRound2)),
            None,
            None,
        ),
        Ok(Day::PostseasonRound(3)) => (
            Some(taxa.day_type_id(TaxaDayType::PostseasonRound3)),
            None,
            None,
        ),
        Ok(Day::PostseasonRound(other)) => {
            error!("Unexpected postseason day {other} (expected 1-3)");
            (None, None, None)
        }
        Ok(Day::Election) => (Some(taxa.day_type_id(TaxaDayType::Election)), None, None),
        Ok(Day::Holiday) => (Some(taxa.day_type_id(TaxaDayType::Holiday)), None, None),
        Ok(Day::Day(day)) => (
            Some(taxa.day_type_id(TaxaDayType::RegularDay)),
            Some(*day as i32),
            None,
        ),
        Ok(Day::SuperstarGame) => (
            Some(taxa.day_type_id(TaxaDayType::SuperstarDay)),
            None,
            None,
        ),
        Ok(Day::SuperstarDay(day)) => (
            Some(taxa.day_type_id(TaxaDayType::SuperstarDay)),
            None,
            Some(*day as i32),
        ),
        Ok(Day::Event) => (Some(taxa.day_type_id(TaxaDayType::Event)), None, None),
        Ok(Day::SpecialEvent) => (
            Some(taxa.day_type_id(TaxaDayType::SpecialEvent)),
            None,
            None,
        ),
        Err(err) => {
            error!("Unrecognized day {err}");
            (None, None, None)
        }
    }
}

fn maybe_recognized_str<T: Display>(val: Result<T, NotRecognized>) -> Result<String, serde_json::Value> {
    match val {
        Ok(v) => {
            Ok(v.to_string())
        }
        Err(NotRecognized(value)) => match value.as_str() {
            None => Err(value),
            Some(str) => Ok(str.to_string()),
        }
    }
}

fn equipment_affixes<T: Display>(affixes: impl IntoIterator<Item=Result<T, NotRecognized>>, affix_type: &str) -> Vec<String> {
    affixes
        .into_iter()
        .flat_map(|p| match maybe_recognized_str(p) {
            Ok(val) => Some(val),
            Err(non_string_value) => {
                // TODO Expose this error on the web interface
                error!(
                    "Ignoring equipment with non-string inside {affix_type} {:?}",
                    non_string_value,
                );
                None
            }
        })
        .collect_vec()
}


fn equipment_affix_plural_or_singular<T: Display>(
    affix_singular: RemovedLaterResult<Option<MaybeRecognizedResult<T>>>,
    affix_plural: AddedLaterResult<Vec<MaybeRecognizedResult<T>>>, 
    affix_type_singular: &str,
    affix_type_plural: &str,
) -> Vec<String> {
    match (affix_singular, affix_plural) {
        (Err(RemovedLater), Err(AddedLater)) => {
            // This occurs with (at least some) modifierless items, so I can't
            // issue a warning for it
            Vec::new()
        }
        (Err(RemovedLater), Ok(affixes)) => {
            equipment_affixes(affixes, affix_type_plural)
        }
        (Ok(prefix), Err(AddedLater)) => {
            equipment_affixes(prefix, affix_type_singular)
        }
        (Ok(_), Ok(prefixes)) => {
            // TODO Expose this warning on the web interface
            warn!(
                "Equipment was both before `{affix_type_singular}` and after `{affix_type_plural}`.\
                Using `{affix_type_plural}` and ignoring `{affix_type_singular}`.",
            );
            equipment_affixes(prefixes, affix_type_plural)
        }
    }
}

#[derive(Debug, Error)]
#[error("Expected a string but got {0}")]
struct NonStringTypeError(serde_json::Value);

fn equipment_slot_to_str(slot: &Result<EquipmentSlot, NotRecognized>) -> Result<&str, NonStringTypeError> {
    Ok(match slot {
        Ok(EquipmentSlot::Accessory) => { "Accessory" }
        Ok(EquipmentSlot::Head) => { "Head" }
        Ok(EquipmentSlot::Feet) => { "Feet" }
        Ok(EquipmentSlot::Hands) => { "Hands" }
        Ok(EquipmentSlot::Body) => { "Body" }
        Err(NotRecognized(value)) => value.as_str()
            .ok_or(NonStringTypeError(value.clone()))?
    })
}

fn chron_player_as_new<'a>(
    entity: &'a ChronEntity<mmolb_parsing::player::Player>,
    taxa: &Taxa,
    modifications: &HashMap<NameEmojiTooltip, i64>,
) -> (
    NewPlayerVersion<'a>,
    Vec<NewPlayerModificationVersion<'a>>,
    Option<(
        NewPlayerFeedVersion<'a>,
        Vec<NewPlayerAttributeAugment<'a>>,
        Vec<NewPlayerParadigmShift<'a>>,
        Vec<NewPlayerRecomposition<'a>>,
        Vec<NewVersionIngestLog<'a>>,
    )>,
    Vec<(
        NewPlayerReportVersion<'a>,
        Vec<NewPlayerReportAttributeVersion<'a>>,
    )>,
    Vec<(
        NewPlayerEquipmentVersion<'a>,
        Vec<NewPlayerEquipmentEffectVersion<'a>>,
    )>,
    Vec<NewVersionIngestLog<'a>>,
) {
    let mut ingest_logs = VersionIngestLogs::new(PLAYER_KIND, &entity.entity_id, entity.valid_from);
    let (birthday_type, birthday_day, birthday_superstar_day) =
        day_to_db(&entity.data.birthday, taxa);

    let get_modification_id = |modification: &mmolb_parsing::player::Modification| {
        *modifications
            .get(&(
                modification.name.as_str(),
                modification.emoji.as_str(),
                modification.description.as_str(),
            ))
            .expect("All modifications should have been added to the modifications table")
    };

    let get_handedness_id = |handedness: &Result<Handedness, NotRecognized>, ingest_logs: &mut VersionIngestLogs| match handedness {
        Ok(handedness) => Some(taxa.handedness_id((*handedness).into())),
        Err(err) => {
            ingest_logs.error(format!("Player had unexpected batting handedness {err}"));
            None
        }
    };

    let modifications = entity
        .data
        .modifications
        .iter()
        .enumerate()
        .map(|(i, m)| NewPlayerModificationVersion {
            mmolb_player_id: &entity.entity_id,
            valid_from: entity.valid_from.naive_utc(),
            valid_until: None,
            modification_index: i as i32,
            modification_id: get_modification_id(m),
        })
        .collect_vec();

    let slot = match &entity.data.position {
        Ok(position) => Some(taxa.slot_id(match position {
            Position::Pitcher => TaxaSlot::Pitcher,
            Position::Catcher => TaxaSlot::Catcher,
            Position::FirstBaseman => TaxaSlot::FirstBase,
            Position::SecondBaseman => TaxaSlot::SecondBase,
            Position::ThirdBaseman => TaxaSlot::ThirdBase,
            Position::ShortStop => TaxaSlot::Shortstop,
            Position::LeftField => TaxaSlot::LeftField,
            Position::CenterField => TaxaSlot::CenterField,
            Position::RightField => TaxaSlot::RightField,
            Position::StartingPitcher => TaxaSlot::StartingPitcher,
            Position::ReliefPitcher => TaxaSlot::ReliefPitcher,
            Position::Closer => TaxaSlot::Closer,
        })),
        Err(err) => {
            ingest_logs.error(format!("Player position not recognized: {err}"));
            None
        }
    };

    let mut occupied_equipment_slots = match &entity.data.equipment {
        Ok(equipment) => {
             equipment.inner
                 .iter()
                 .filter_map(|(slot, equipment)| {
                     if equipment.is_none() {
                         None
                     } else {
                         match equipment_slot_to_str(&slot) {
                             Ok(slot) => Some(slot),
                             Err(err) => {
                                 ingest_logs.error(format!(
                                     "Error processing player equipment slot: {err}. This slot will be ignored.",
                                 ));
                                 None
                             }
                         }
                     }
                 })
                 .collect_vec()
        }
        Err(AddedLater) => Vec::new(),
    };

    // Important, because they can be returned in arbitrary order
    occupied_equipment_slots.sort();

    // No sort necessary because order is hard-coded
    let included_report_categories = entity.data.talk.as_ref().map(|t| {
        [
            if t.batting.is_some() { Some(TaxaAttributeCategory::Batting) } else { None },
            if t.pitching.is_some() { Some(TaxaAttributeCategory::Pitching) } else { None },
            if t.defense.is_some() { Some(TaxaAttributeCategory::Defense) } else { None },
            if t.baserunning.is_some() { Some(TaxaAttributeCategory::Baserunning) } else { None },
        ]
            .into_iter()
            .flatten()
            .map(|category| taxa.attribute_category_id(category))
            .collect_vec()
    }).unwrap_or_default();

    let player = NewPlayerVersion {
        mmolb_player_id: &entity.entity_id,
        valid_from: entity.valid_from.naive_utc(),
        valid_until: None,
        first_name: &entity.data.first_name,
        last_name: &entity.data.last_name,
        batting_handedness: get_handedness_id(&entity.data.bats, &mut ingest_logs),
        pitching_handedness: get_handedness_id(&entity.data.throws, &mut ingest_logs),
        home: &entity.data.home,
        birthseason: entity.data.birthseason as i32,
        birthday_type,
        birthday_day,
        birthday_superstar_day,
        likes: &entity.data.likes,
        dislikes: &entity.data.dislikes,
        number: entity.data.number as i32,
        mmolb_team_id: entity.data.team_id.as_deref(),
        slot,
        durability: entity.data.durability,
        greater_boon: entity.data.greater_boon.as_ref().map(get_modification_id),
        lesser_boon: entity.data.lesser_boon.as_ref().map(get_modification_id),
        num_modifications: entity.data.modifications.len() as i32,
        occupied_equipment_slots,
        included_report_categories,
    };

    let player_full_name = format!("{} {}", player.first_name, player.last_name);
    let feed_as_new = match &entity.data.feed {
        Ok(entries) => Some(chron_player_feed_as_new(taxa, &entity.entity_id, entity.valid_from, entries, Some(&player_full_name))),
        Err(_) => None,
    };

    let mut report_versions = Vec::new();
    if let Some(talk) = &entity.data.talk {
        if let Some(report) = &talk.batting {
            report_versions.push(report_as_new(TaxaAttributeCategory::Batting, report, entity, taxa));
        }
        if let Some(report) = &talk.pitching {
            report_versions.push(report_as_new(TaxaAttributeCategory::Pitching, report, entity, taxa));
        }
        if let Some(report) = &talk.defense {
            report_versions.push(report_as_new(TaxaAttributeCategory::Defense, report, entity, taxa));
        }
        if let Some(report) = &talk.baserunning {
            report_versions.push(report_as_new(TaxaAttributeCategory::Baserunning, report, entity, taxa));
        }
    }

    let equipment = match &entity.data.equipment {
        Ok(equipment) => {
            // TODO I've requested a way to do this without clone(). Switch to
            //   that once it's implemented in mmolb_parsing
            let map: std::collections::HashMap<Result<EquipmentSlot, NotRecognized>, Option<PlayerEquipment>> = equipment.clone().into();
            map.into_iter()
                .filter_map(|(slot, equipment)| equipment.and_then(|equipment| {
                    let equipment_slot = match maybe_recognized_str(slot) {
                        Ok(equipment_slot) => equipment_slot,
                        Err(non_string_value) => {
                            ingest_logs.error(format!(
                                "Ignoring equipment with non-string slot {non_string_value:?}",
                            ));
                            return None;
                        }
                    };

                    let name = match maybe_recognized_str(equipment.name) {
                        Ok(name) => name,
                        Err(non_string_value) => {
                            ingest_logs.error(format!(
                                "Ignoring equipment with non-string name {non_string_value:?}",
                            ));
                            return None;
                        }
                    };

                    let rarity = match equipment.rarity {
                        Ok(rarity) => match maybe_recognized_str(rarity) {
                            Ok(rarity) => Some(rarity),
                            Err(non_string_value) => {
                                ingest_logs.error(format!(
                                    "Ignoring non-string equipment rarity {non_string_value:?}",
                                ));
                                None
                            }
                        }
                        Err(AddedLater) => {
                            None
                        }
                    };

                    let new_equipment = NewPlayerEquipmentVersion {
                        mmolb_player_id: &entity.entity_id,
                        equipment_slot: equipment_slot.clone(),
                        valid_from: entity.valid_from.naive_utc(),
                        valid_until: None,
                        emoji: equipment.emoji.clone(),
                        name,
                        special_type: equipment.special_type.map(|t| t.to_string()),
                        description: equipment.description.clone(),
                        rare_name: equipment.rare_name.clone(),
                        cost: equipment.cost.map(|v| v as i32),
                        prefixes: equipment_affix_plural_or_singular(equipment.prefix, equipment.prefixes, "prefix", "prefixes"),
                        suffixes: equipment_affix_plural_or_singular(equipment.suffix, equipment.suffixes, "suffix", "suffixes"),
                        rarity,
                        num_effects: equipment.effects.as_ref().map_or(0, |e| e.len() as i32),
                    };

                    let effects = match equipment.effects {
                        None => {
                            // Presumably None effects is the same as empty list of effects
                            Vec::new()
                        }
                        Some(effects) => effects
                            .into_iter()
                            .enumerate()
                            .filter_map(|(index, effect)| {
                                let effect = match effect {
                                    Ok(effect) => effect,
                                    Err(NotRecognized(value)) => {
                                        ingest_logs.error(format!(
                                            "Skipping unrecognized equipment effect {value:?}",
                                        ));
                                        return None;
                                    }
                                };

                                let attribute = match effect.attribute {
                                    Ok(attribute) => attribute,
                                    Err(NotRecognized(value)) => {
                                        ingest_logs.error(format!(
                                            "Skipping unrecognized equipment effect attribute {:?}",
                                            value,
                                        ));
                                        return None;
                                    }
                                };

                                let effect_type = match effect.effect_type {
                                    Ok(effect_type) => effect_type,
                                    Err(NotRecognized(value)) => {
                                        ingest_logs.error(format!(
                                            "Skipping unrecognized equipment effect type {:?}",
                                            value,
                                        ));
                                        return None;
                                    }
                                };

                                Some(NewPlayerEquipmentEffectVersion {
                                    mmolb_player_id: &entity.entity_id,
                                    equipment_slot: equipment_slot.clone(),
                                    effect_index: index as i32,
                                    valid_from: entity.valid_from.naive_utc(),
                                    valid_until: None,
                                    attribute: taxa.attribute_id(attribute.into()),
                                    effect_type: taxa.effect_type_id(effect_type.into()),
                                    value: effect.value,
                                })
                            })
                            .collect_vec()
                    };

                    Some((new_equipment, effects))
                }))
                .collect_vec()
        }
        Err(AddedLater) => Vec::new(), // No equipment
    };

    (
        player,
        modifications,
        feed_as_new,
        report_versions,
        equipment,
        ingest_logs.into_vec(),
    )
}

fn report_as_new<'e>(
    category: TaxaAttributeCategory,
    report: &'e TalkCategory,
    entity: &'e ChronEntity<mmolb_parsing::player::Player>,
    taxa: &Taxa,
) -> (NewPlayerReportVersion<'e>, Vec<NewPlayerReportAttributeVersion<'e>>) {
    let season = match report.season {
        Ok(season) => Some(season as i32),
        Err(AddedLater) => None,
    };

    let (day_type, day, superstar_day) = match &report.day {
        Ok(maybe_day) => day_to_db(maybe_day, taxa),
        Err(AddedLater) => (None, None, None),
    };

    let mut included_attributes = report.stars.iter()
        .map(|(attribute, _)| taxa.attribute_id((*attribute).into()))
        .collect_vec();
    // This is meant to be a set. Sort it to make sure that order
    // never causes two identical sets to be considered different.
    included_attributes.sort();

    let report_version = NewPlayerReportVersion {
        mmolb_player_id: &entity.entity_id,
        category: taxa.attribute_category_id(category),
        valid_from: entity.valid_from.naive_utc(),
        valid_until: None,
        season,
        day_type,
        day,
        superstar_day,
        quote: &report.quote,
        included_attributes,
    };

    let report_attribute_versions = report.stars.iter()
        .map(|(attribute, stars)| NewPlayerReportAttributeVersion {
            mmolb_player_id: &entity.entity_id,
            category: taxa.attribute_category_id(category),
            attribute: taxa.attribute_id((*attribute).into()),
            valid_from: entity.valid_from.naive_utc(),
            valid_until: None,
            stars: *stars as i32,
        })
        .collect_vec();

    (report_version, report_attribute_versions)
}
