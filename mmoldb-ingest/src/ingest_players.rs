use std::fmt::Display;
use std::iter;
use std::sync::Arc;
use chrono::NaiveDateTime;
use futures::Stream;
use hashbrown::HashMap;
use itertools::Itertools;
use log::{error, warn};
use mmolb_parsing::enums::{Attribute, Day, EquipmentSlot, Handedness, Position};
use mmolb_parsing::{AddedLater, AddedLaterResult, MaybeRecognizedResult, NotRecognized, RemovedLater, RemovedLaterResult};
use mmolb_parsing::player::{ComplexTalkStars, PlayerEquipment, TalkCategory, TalkStars};
use thiserror::Error;

use mmoldb_db::{async_db, db, AsyncPgConnection, PgConnection, QueryResult};
use chron::ChronEntity;
use mmoldb_db::db::NameEmojiTooltip;
use mmoldb_db::models::{NewPlayerEquipmentEffectVersion, NewPlayerEquipmentVersion, NewPlayerModificationVersion, NewPlayerPitchTypeVersion, NewPlayerReportAttributeVersion, NewPlayerReportVersion, NewPlayerVersion, NewVersionIngestLog};
use mmoldb_db::taxa::{Taxa, TaxaAttributeCategory, TaxaDayType, TaxaModificationType, TaxaSlot};
use crate::config::IngestibleConfig;
use crate::{IngestStage, Ingestable, IngestibleFromVersions, Stage2Ingest, VersionIngestLogs, VersionStage1Ingest};

pub struct PlayerIngestFromVersions;

impl IngestibleFromVersions for PlayerIngestFromVersions {
    type Entity = mmolb_parsing::player::Player;

    fn get_start_cursor(conn: &mut PgConnection) -> QueryResult<Option<(NaiveDateTime, String)>> {
        db::get_player_ingest_start_cursor(conn)
    }

    fn trim_unused(version: &serde_json::Value) -> serde_json::Value {
        match version {
            serde_json::Value::Object(obj) => obj
                .iter()
                .filter(|(k, _)| *k != "Stats" && *k != "SeasonStats")
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            other => other.clone(),
        }
    }

    fn insert_batch(conn: &mut PgConnection, taxa: &Taxa, versions: &Vec<ChronEntity<Self::Entity>>) -> QueryResult<usize> {
        // Collect all modifications that appear in this batch so we can ensure they're all added
        let unique_modifications = versions
            .iter()
            .flat_map(|version| {
                version
                    .data
                    .modifications
                    .iter()
                    .chain(version.data.lesser_boon.iter())
                    .chain(version.data.greater_boon.iter())
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

        let modifications = get_filled_modifications_map(conn, &unique_modifications)?;
        let new_versions = versions.iter()
            .map(|entity| chron_player_as_new(taxa, &entity, &modifications))
            .collect_vec();

        db::insert_player_versions(conn, &new_versions)
    }

    async fn stream_versions_at_cursor(conn: &mut AsyncPgConnection, kind: &str, cursor: Option<(NaiveDateTime, String)>) -> QueryResult<impl Stream<Item=QueryResult<ChronEntity<serde_json::Value>>>> {
        async_db::stream_versions_at_cursor(conn, kind, cursor).await
    }
}

pub struct PlayerIngest(&'static IngestibleConfig);

impl PlayerIngest {
    pub fn new(config: &'static IngestibleConfig) -> PlayerIngest {
        PlayerIngest(config)
    }
}

impl Ingestable for PlayerIngest {
    const KIND: &'static str = "player";

    fn config(&self) -> &'static IngestibleConfig {
        &self.0
    }

    fn stages(&self) -> Vec<Arc<dyn IngestStage>> {
        vec![
            Arc::new(VersionStage1Ingest::new(Self::KIND)),
            Arc::new(Stage2Ingest::new(Self::KIND, PlayerIngestFromVersions))
        ]
    }
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
    day: Option<&Result<Day, NotRecognized>>,
    taxa: &Taxa,
) -> (Option<i64>, Option<i32>, Option<i32>) {
    match day {
        None => (None, None, None),
        Some(Ok(Day::Preseason)) => (Some(taxa.day_type_id(TaxaDayType::Preseason)), None, None),
        Some(Ok(Day::SuperstarBreak)) => (
            Some(taxa.day_type_id(TaxaDayType::SuperstarBreak)),
            None,
            None,
        ),
        Some(Ok(Day::PostseasonPreview)) => (
            Some(taxa.day_type_id(TaxaDayType::PostseasonPreview)),
            None,
            None,
        ),
        Some(Ok(Day::PostseasonRound(1))) => (
            Some(taxa.day_type_id(TaxaDayType::PostseasonRound1)),
            None,
            None,
        ),
        Some(Ok(Day::PostseasonRound(2))) => (
            Some(taxa.day_type_id(TaxaDayType::PostseasonRound2)),
            None,
            None,
        ),
        Some(Ok(Day::PostseasonRound(3))) => (
            Some(taxa.day_type_id(TaxaDayType::PostseasonRound3)),
            None,
            None,
        ),
        Some(Ok(Day::PostseasonRound(other))) => {
            error!("Unexpected postseason day {other} (expected 1-3)");
            (None, None, None)
        }
        Some(Ok(Day::Election)) => (Some(taxa.day_type_id(TaxaDayType::Election)), None, None),
        Some(Ok(Day::Holiday)) => (Some(taxa.day_type_id(TaxaDayType::Holiday)), None, None),
        Some(Ok(Day::Day(day))) => (
            Some(taxa.day_type_id(TaxaDayType::RegularDay)),
            Some(*day as i32),
            None,
        ),
        Some(Ok(Day::SuperstarGame)) => (
            Some(taxa.day_type_id(TaxaDayType::SuperstarDay)),
            None,
            None,
        ),
        Some(Ok(Day::SuperstarDay(day))) => (
            Some(taxa.day_type_id(TaxaDayType::SuperstarDay)),
            None,
            Some(*day as i32),
        ),
        Some(Ok(Day::Event)) => (Some(taxa.day_type_id(TaxaDayType::Event)), None, None),
        Some(Ok(Day::SpecialEvent)) => (
            Some(taxa.day_type_id(TaxaDayType::SpecialEvent)),
            None,
            None,
        ),
        Some(Ok(Day::Offseason)) => (
            Some(taxa.day_type_id(TaxaDayType::Offseason)),
            None,
            None,  // In this context, offseason day isn't available
        ),
        Some(Err(err)) => {
            error!("Unrecognized day {err}");
            (None, None, None)
        }
    }
}

fn maybe_recognized_str<T: Display>(
    val: Result<T, NotRecognized>,
) -> Result<String, serde_json::Value> {
    match val {
        Ok(v) => Ok(v.to_string()),
        Err(NotRecognized(value)) => match value.as_str() {
            None => Err(value),
            Some(str) => Ok(str.to_string()),
        },
    }
}

fn equipment_affixes<T: Display>(
    affixes: impl IntoIterator<Item = Result<T, NotRecognized>>,
    affix_type: &str,
) -> Vec<String> {
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
        (Err(RemovedLater), Ok(affixes)) => equipment_affixes(affixes, affix_type_plural),
        (Ok(prefix), Err(AddedLater)) => equipment_affixes(prefix, affix_type_singular),
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

fn equipment_slot_to_str(
    slot: &Result<EquipmentSlot, NotRecognized>,
) -> Result<&str, NonStringTypeError> {
    Ok(match slot {
        Ok(EquipmentSlot::Accessory) => "Accessory",
        Ok(EquipmentSlot::Head) => "Head",
        Ok(EquipmentSlot::Feet) => "Feet",
        Ok(EquipmentSlot::Hands) => "Hands",
        Ok(EquipmentSlot::Body) => "Body",
        Err(NotRecognized(value)) => value.as_str().ok_or(NonStringTypeError(value.clone()))?,
    })
}

fn chron_player_as_new<'a>(
    taxa: &Taxa,
    entity: &'a ChronEntity<mmolb_parsing::player::Player>,
    modifications: &HashMap<NameEmojiTooltip, i64>,
) -> (
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
    Vec<NewPlayerPitchTypeVersion<'a>>,
    Vec<NewVersionIngestLog<'a>>,
) {
    let mut ingest_logs = VersionIngestLogs::new(PlayerIngest::KIND, &entity.entity_id, entity.valid_from);
    let (birthday_type, birthday_day, birthday_superstar_day) =
        day_to_db(Some(&entity.data.birthday), taxa);

    let get_modification_id = |modification: &mmolb_parsing::player::Modification| {
        *modifications
            .get(&(
                modification.name.as_str(),
                modification.emoji.as_str(),
                modification.description.as_str(),
            ))
            .expect("All modifications should have been added to the modifications table")
    };

    let get_handedness_id = |handedness: &Result<Handedness, NotRecognized>,
                             ingest_logs: &mut VersionIngestLogs| {
        match handedness {
            Ok(handedness) => Some(taxa.handedness_id((*handedness).into())),
            Err(err) => {
                ingest_logs.error(format!("Player had unexpected batting handedness {err}"));
                None
            }
        }
    };

    let make_modification = |i, m, ty| {
        NewPlayerModificationVersion {
            mmolb_player_id: &entity.entity_id,
            valid_from: entity.valid_from.naive_utc(),
            valid_until: None,
            modification_index: i as i32,
            modification_id: get_modification_id(m),
            modification_type: taxa.modification_type_id(ty),
        }
    };

    let modifications = entity
        .data
        .modifications
        .iter()
        .enumerate()
        .map(|(i, m)| make_modification(i, m, TaxaModificationType::Modification))
        .chain(
            entity.data.lesser_boon.iter()
                .enumerate()
                .map(|(i, m)| make_modification(i, m, TaxaModificationType::LesserBoon)),
        )
        .chain(
            entity.data.greater_boon.iter()
                .enumerate()
                .map(|(i, m)| make_modification(i, m, TaxaModificationType::GreaterBoon)),
        )
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

    let included_report_categories = if let Some(_) = entity.data.attribute_stars.as_ref().ok() {
        // If this field exists, that means all "talk" pages are revealed
        // (aka this is past the time of talk pages)
        [
            TaxaAttributeCategory::Batting,
            TaxaAttributeCategory::Pitching,
            TaxaAttributeCategory::Defense,
            TaxaAttributeCategory::Baserunning,
        ]
            .into_iter()
            .map(|category| taxa.attribute_category_id(category))
            .collect_vec()

    } else {
        // No sort necessary because order is hard-coded
        entity
            .data
            .talk
            .as_ref()
            .map(|t| {
                [
                    t.batting.as_ref().map(|_| TaxaAttributeCategory::Batting),
                    t.pitching.as_ref().map(|_| TaxaAttributeCategory::Pitching),
                    t.defense.as_ref().map(|_| TaxaAttributeCategory::Defense),
                    t.baserunning.as_ref().map(|_| TaxaAttributeCategory::Baserunning),
                ]
                    .into_iter()
                    .flatten()
                    .map(|category| taxa.attribute_category_id(category))
                    .collect_vec()
            })
            .unwrap_or_default()
    };

    let priority = entity.data.base_attributes.as_ref().ok().and_then(|base_attributes| {
        if let Some(priority) = base_attributes.attributes.get(&Attribute::Priority) {
            Some(*priority)
        } else {
            ingest_logs.warn(
                "Player version had BaseAttributes, but BaseAttributes did not contain the \
                Priority attribute",
            );
            None
        }

    });

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
        num_modifications: modifications.len() as i32,
        occupied_equipment_slots,
        included_report_categories,
        priority,
        xp: entity.data.xp.as_ref().ok().map(|xp| *xp as i32),
        name_suffix: entity.data.suffix.as_ref().ok().and_then(|x| x.as_deref()),
    };
    
    let mut report_versions = Vec::new();
    if let Ok(attrs) = &entity.data.attribute_stars {
        for (category, attrs) in attrs {
            let included_attributes = attrs
                .iter()
                .map(|(attribute, _)| taxa.attribute_id((*attribute).into()))
                .collect_vec();

            let report_version = NewPlayerReportVersion {
                mmolb_player_id: &entity.entity_id,
                category: taxa.attribute_category_id((*category).into()),
                valid_from: entity.valid_from.naive_utc(),
                valid_until: None,
                season: None,
                day_type: None,
                day: None,
                superstar_day: None,
                quote: None,
                included_attributes,
            };

            let report_attribute_versions = attrs
                .iter()
                .map(|(attribute, stars)| {
                    // Every value in attribute_stars is expected to match with the corresponding
                    // value in base_attributes
                    if let Some(base_attrs) = entity.data.base_attributes.as_ref().ok() {
                        if let Some(base_attr) = base_attrs.attributes.get(attribute) {
                            // I'm writing this as a super-strict comparison so I can detect
                            // how equal the equalities are. It may need to be relaxed in
                            // the near future.
                            if &stars.base_total != base_attr {
                                ingest_logs.warn(format!(
                                    "Base {} value in BaseAttributes ({}) did not exactly \
                                    match the value in AttributeStars ({})",
                                    <Attribute as Into<&'static str>>::into(*attribute),
                                    stars.base_total,
                                    base_attr,
                                ))
                            }
                        } else {
                            ingest_logs.warn(format!(
                                "BaseAttributes field on player entity exists, but does not \
                                contain an entry for attribute {}. An entry was expected because \
                                this attribute appears in the {:?} section of AttributeStars.",
                                <Attribute as Into<&'static str>>::into(*attribute),
                                // TODO I've requested derives for Into<&'static str> on category
                                //   too, update that to match once it exists
                                category,
                            ));
                        }
                    }

                    player_report_attribute_version_as_new_from_complex((*category).into(), entity, taxa, attribute, stars)
                })
                .collect_vec();

            report_versions.push((report_version, report_attribute_versions))
        }
    } else if let Some(talk) = &entity.data.talk {
        if let Some(report) = &talk.batting {
            report_versions.push(report_from_talk(
                TaxaAttributeCategory::Batting,
                report,
                entity,
                taxa,
            ));
        }
        if let Some(report) = &talk.pitching {
            report_versions.push(report_from_talk(
                TaxaAttributeCategory::Pitching,
                report,
                entity,
                taxa,
            ));
        }
        if let Some(report) = &talk.defense {
            report_versions.push(report_from_talk(
                TaxaAttributeCategory::Defense,
                report,
                entity,
                taxa,
            ));
        }
        if let Some(report) = &talk.baserunning {
            report_versions.push(report_from_talk(
                TaxaAttributeCategory::Baserunning,
                report,
                entity,
                taxa,
            ));
        }
    }

    let equipment = match &entity.data.equipment {
        Ok(equipment) => {
            // TODO I've requested a way to do this without clone(). Switch to
            //   that once it's implemented in mmolb_parsing
            let map: std::collections::HashMap<
                Result<EquipmentSlot, NotRecognized>,
                Option<PlayerEquipment>,
            > = equipment.clone().into();
            map.into_iter()
                .filter_map(|(slot, equipment)| {
                    equipment.and_then(|equipment| {
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
                            },
                            Err(AddedLater) => None,
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
                            prefixes: equipment_affix_plural_or_singular(
                                equipment.prefix,
                                equipment.prefixes,
                                "prefix",
                                "prefixes",
                            ),
                            suffixes: equipment_affix_plural_or_singular(
                                equipment.suffix,
                                equipment.suffixes,
                                "suffix",
                                "suffixes",
                            ),
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
                                .collect_vec(),
                        };

                        Some((new_equipment, effects))
                    })
                })
                .collect_vec()
        }
        Err(AddedLater) => Vec::new(), // No equipment
    };

    // First, try to get pitch types out of BaseAttributes, since it's the most accurate source.
    // Leave pitch_types as an empty vec if that doesn't succeed for any reason.
    let pitch_types = if let Ok(base_attributes) = &entity.data.base_attributes {
        if base_attributes.pitch_types.len() == base_attributes.pitch_selection.len() {
            if let Ok(pitch_types) = &entity.data.pitch_types {
                if pitch_types.len() == base_attributes.pitch_types.len() {
                    for (index, (root_ty, base_attributes_ty)) in iter::zip(pitch_types.iter(), base_attributes.pitch_types.iter()).enumerate() {
                        if root_ty != base_attributes_ty {
                            ingest_logs.warn(format!(
                                "{}th pitch type in BaseAttributes ({}) does not match the \
                                corresponding pitch type in the root object ({})",
                                index,
                                base_attributes_ty,
                                root_ty,
                            ));
                        }
                    }
                } else {
                    ingest_logs.warn(format!(
                        "PitchTypes in BaseAttributes has length {}, but PitchTypes on the root \
                        object has length {} (expected equal length)",
                        base_attributes.pitch_types.len(),
                        pitch_types.len()
                    ));
                }
            }

            if let Ok(pitch_selection) = &entity.data.pitch_selection {
                if pitch_selection.len() == base_attributes.pitch_selection.len() {
                    for (index, (root_freq, base_attributes_freq)) in iter::zip(pitch_selection.iter(), base_attributes.pitch_selection.iter()).enumerate() {
                        // Formatting as string is the most straightforward way to check the kind
                        // of correspondence we want
                        if format!("{:.2}", root_freq) != format!("{:.2}", base_attributes_freq) {
                            ingest_logs.warn(format!(
                                "{}th pitch frequency in BaseAttributes ({}) does not match the \
                                corresponding pitch frequency in the root object ({})",
                                index,
                                base_attributes_freq,
                                root_freq,
                            ));
                        }
                    }
                } else {
                    ingest_logs.warn(format!(
                        "PitchSelection in BaseAttributes has length {}, but PitchSelection on the \
                        root object has length {} (expected equal length)",
                        base_attributes.pitch_selection.len(),
                        pitch_selection.len()
                    ));
                }
            }

            iter::zip(&base_attributes.pitch_types, &base_attributes.pitch_selection)
                .enumerate()
                .map(|(index, (ty, freq))| NewPlayerPitchTypeVersion {
                    mmolb_player_id: &entity.entity_id,
                    pitch_type_index: index as i32,
                    valid_from: entity.valid_from.naive_utc(),
                    valid_until: None,
                    pitch_type: taxa.pitch_type_id((*ty).into()),
                    frequency: *freq,
                    // This source is full precision as of this writing
                    expect_full_precision: true,
                })
                .collect_vec()
        } else {
             ingest_logs.error(format!(
                 "Can't extract pitch types from BaseAttributes: PitchTypes was length {}, but \
                 PitchSelection was length {} (expected equal length)",
                 base_attributes.pitch_types.len(),
                 base_attributes.pitch_selection.len()
             ));
            Vec::new()
        }
    } else {
        Vec::new()
    };

    // If we don't have pitch types yet, try to get them out of the root object
    let pitch_types = if pitch_types.is_empty() {
        if let Ok(pitch_types) = &entity.data.pitch_types {
            if let Ok(pitch_selection) = &entity.data.pitch_selection {
                if pitch_types.len() == pitch_selection.len() {
                    iter::zip(pitch_types, pitch_selection)
                        .enumerate()
                        .map(|(index, (ty, freq))| NewPlayerPitchTypeVersion {
                            mmolb_player_id: &entity.entity_id,
                            pitch_type_index: index as i32,
                            valid_from: entity.valid_from.naive_utc(),
                            valid_until: None,
                            pitch_type: taxa.pitch_type_id((*ty).into()),
                            frequency: *freq,
                            // This source is not full precision as of this writing
                            expect_full_precision: false,
                        })
                        .collect_vec()
                } else {
                    ingest_logs.error(format!(
                        "Can't extract pitch types from object root: PitchTypes was length {}, but \
                        PitchSelection was length {} (expected equal length)",
                        pitch_types.len(),
                        pitch_selection.len()
                    ));
                    Vec::new()
                }
            } else {
                ingest_logs.warn(format!(
                    "Can't extract pitch types from object root: PitchTypes exists with {} \
                    entries, but PitchSelection does not exist.",
                    pitch_types.len()
                ));
                Vec::new()
            }
        } else if let Ok(pitch_selection) = &entity.data.pitch_selection {
            ingest_logs.warn(format!(
                "Can't extract pitch types from object root: PitchSelection exists with {} \
                entries, but PitchTypes does not exist.",
                pitch_selection.len()
            ));
            Vec::new()
        } else {
            Vec::new()
        }
    } else {
        pitch_types
    };

    (
        player,
        modifications,
        report_versions,
        equipment,
        pitch_types,
        ingest_logs.into_vec(),
    )
}

fn report_from_talk<'e>(
    category: TaxaAttributeCategory,
    report: &'e TalkCategory,
    entity: &'e ChronEntity<mmolb_parsing::player::Player>,
    taxa: &Taxa,
) -> (
    NewPlayerReportVersion<'e>,
    Vec<NewPlayerReportAttributeVersion<'e>>,
) {
    let season = match report.season {
        Ok(Some(season)) => Some(season as i32),
        Ok(None) => {
            // TODO Publicize the implications of this None
            None
        },
        Err(AddedLater) => None,
    };

    let (day_type, day, superstar_day) = match &report.day {
        Ok(maybe_day) => day_to_db(maybe_day.as_ref(), taxa),
        Err(AddedLater) => (None, None, None),
    };

    let mut included_attributes = report
        .stars
        .iter()
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
        quote: Some(&report.quote),
        included_attributes,
    };

    let report_attribute_versions = report
        .stars
        .iter()
        .map(|(attribute, stars)| player_report_attribute_version_as_new(category, entity, taxa, attribute, stars))
        .collect_vec();

    (report_version, report_attribute_versions)
}

fn player_report_attribute_version_as_new<'a>(
    category: TaxaAttributeCategory,
    entity: &'a ChronEntity<mmolb_parsing::player::Player>,
    taxa: &Taxa,
    attribute: &'a Attribute,
    stars: &'a TalkStars,
) -> NewPlayerReportAttributeVersion<'a> {
    NewPlayerReportAttributeVersion {
        mmolb_player_id: &entity.entity_id,
        category: taxa.attribute_category_id(category),
        attribute: taxa.attribute_id((*attribute).into()),
        valid_from: entity.valid_from.naive_utc(),
        valid_until: None,
        base_stars: match stars {
            TalkStars::Simple(stars) => Some(*stars as i32),
            TalkStars::Intermediate { .. } => None,
            TalkStars::Complex(ComplexTalkStars { base_stars, .. }) => Some(*base_stars as i32),
        },
        base_total: match stars {
            TalkStars::Simple(_) => None,
            TalkStars::Intermediate { .. } => None,
            TalkStars::Complex(ComplexTalkStars { base_total, .. }) => Some(*base_total),
        },
        modified_stars: match stars {
            TalkStars::Simple(_) => None,
            TalkStars::Intermediate { stars, .. } => Some(*stars as i32),
            TalkStars::Complex(ComplexTalkStars { stars, .. }) => Some(*stars as i32)
        },
        modified_total: match stars {
            TalkStars::Simple(_) => None,
            TalkStars::Intermediate { total, .. } => Some(*total),
            TalkStars::Complex(ComplexTalkStars { total, .. }) => Some(*total),
        },
    }
}

fn player_report_attribute_version_as_new_from_complex<'a>(
    category: TaxaAttributeCategory,
    entity: &'a ChronEntity<mmolb_parsing::player::Player>,
    taxa: &Taxa,
    attribute: &'a Attribute,
    stars: &'a ComplexTalkStars,
) -> NewPlayerReportAttributeVersion<'a> {
    NewPlayerReportAttributeVersion {
        mmolb_player_id: &entity.entity_id,
        category: taxa.attribute_category_id(category),
        attribute: taxa.attribute_id((*attribute).into()),
        valid_from: entity.valid_from.naive_utc(),
        valid_until: None,
        base_stars: Some(stars.base_stars as i32),
        base_total: Some(stars.base_total),
        modified_stars: Some(stars.stars as i32),
        modified_total: Some(stars.total),
    }
}
