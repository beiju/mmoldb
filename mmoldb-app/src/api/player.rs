use crate::Db;
use crate::api::error::ApiError;
use chrono::{DateTime, Utc};
use hashbrown::HashMap;
use itertools::Itertools;
use log::warn;
use mmoldb_db::taxa::{AsInsertable, Taxa, TaxaAttribute, TaxaAttributeCategory, TaxaDayType, TaxaEffectType, TaxaHandedness, TaxaSlot};
use rocket::serde::Serialize;
use rocket::serde::json::Json;
use rocket::{State, get};

#[derive(Clone, Serialize)]
pub struct ApiModification {
    pub name: String,
    pub emoji: String,
    pub description: String,
}

#[derive(Clone, Serialize)]
pub struct ApiEquipmentEffect {
    pub attribute: TaxaAttribute,
    pub effect_type: TaxaEffectType,
    pub value: f64,
}

#[derive(Clone, Serialize)]
pub struct ApiEquipment {
    pub emoji: String,
    pub name: String,
    pub special_type: Option<String>,
    pub description: Option<String>,
    pub rare_name: Option<String>,
    pub cost: Option<i32>,
    pub prefixes: Vec<Option<String>>,
    pub suffixes: Vec<Option<String>>,
    pub rarity: Option<String>,
    pub effects: Vec<Option<ApiEquipmentEffect>>,
}

#[derive(Clone, Serialize)]
pub struct ApiReportAttribute {
    pub stars: i32,
}

#[derive(Clone, Serialize)]
pub struct ApiReport {
    pub season: Option<i32>,
    pub day_type: Option<TaxaDayType>,
    pub day: Option<i32>,
    pub superstar_day: Option<i32>,
    pub quote: String,
    pub attributes: HashMap<TaxaAttribute, Option<ApiReportAttribute>>,
}

#[derive(Clone, Serialize)]
pub struct ApiPlayerVersion {
    pub id: String,
    pub valid_from: DateTime<Utc>,
    pub valid_until: Option<DateTime<Utc>>,
    pub first_name: String,
    pub last_name: String,
    pub batting_handedness: Option<TaxaHandedness>,
    pub pitching_handedness: Option<TaxaHandedness>,
    pub home: String,
    pub birthseason: i32,
    pub birthday_type: Option<TaxaDayType>,
    pub birthday_day: Option<i32>,
    pub birthday_superstar_day: Option<i32>,
    pub likes: String,
    pub dislikes: String,
    pub number: i32,
    pub mmolb_team_id: Option<String>,
    pub slot: Option<TaxaSlot>,
    pub durability: f64,
    pub greater_boon: Option<ApiModification>,
    pub lesser_boon: Option<ApiModification>,
    pub modifications: Vec<Option<ApiModification>>,
    pub equipment: HashMap<String, Option<ApiEquipment>>,
    pub reports: HashMap<TaxaAttributeCategory, Option<ApiReport>>,
    pub events: Vec<ApiPlayerEvent>,
}

#[derive(Clone, Serialize)]
#[serde(tag = "event_type")]
pub enum ApiPlayerEvent {
    Recomposition {
        time: DateTime<Utc>,
        new_name: String,
        reverts_recomposition: Option<DateTime<Utc>>,
    },
    AttributeAugment {
        time: DateTime<Utc>,
        category: TaxaAttributeCategory,
        attribute: TaxaAttribute,
        value: i32,
    },
    Party {
        category: TaxaAttributeCategory,
        attribute: TaxaAttribute,
        value: i32,
    }
}

#[derive(Serialize)]
pub struct ApiPlayerVersions<'a> {
    pub player_id: &'a str,
    pub versions: Vec<ApiPlayerVersion>,
}

struct NextChangeTime(Option<DateTime<Utc>>);

impl NextChangeTime {
    pub fn new() -> Self {
        Self(None)
    }

    pub fn with_change(&mut self, time: DateTime<Utc>) {
        if let Some(t) = &mut self.0 {
            if time < *t {
                *t = time;
            }
        } else {
            self.0 = Some(time);
        }
    }

    pub fn with_possible_change(&mut self, time: Option<DateTime<Utc>>) {
        if let Some(time) = time {
            self.with_change(time);
        }
    }

    pub fn into_inner(self) -> Option<DateTime<Utc>> {
        self.0
    }
}

#[get("/player_versions/<player_id>")]
pub async fn player_versions<'a>(
    player_id: &'a str,
    db: Db,
    taxa: &State<Taxa>,
) -> Result<Json<ApiPlayerVersions<'a>>, ApiError> {
    let mmolb_player_id = player_id.to_string();
    let (
        player_versions,
        player_modification_versions,
        player_equipment_versions,
        player_equipment_effect_versions,
        player_report_versions,
        player_report_attribute_versions,
        modifications,
        mut player_recompositions,
        attribute_augments,
        player_parties,
    ) = db
        .run(move |conn| {
            let players = mmoldb_db::db::get_player_versions(conn, &mmolb_player_id)?;
            let player_modifications =
                mmoldb_db::db::get_player_modification_versions(conn, &mmolb_player_id)?;
            let player_equipment =
                mmoldb_db::db::get_player_equipment_versions(conn, &mmolb_player_id)?;
            let player_equipment_effects =
                mmoldb_db::db::get_player_equipment_effect_versions(conn, &mmolb_player_id)?;
            let player_reports =
                mmoldb_db::db::get_player_report_versions(conn, &mmolb_player_id)?;
            let player_report_attributes =
                mmoldb_db::db::get_player_report_attribute_versions(conn, &mmolb_player_id)?;

            let mod_ids = player_modifications
                .iter()
                .map(|pm| pm.modification_id)
                .chain(players.iter().map(|pm| pm.greater_boon).flatten())
                .chain(players.iter().map(|pm| pm.lesser_boon).flatten())
                .collect_vec();
            let modifications = mmoldb_db::db::get_modifications(conn, &mod_ids)?;

            let player_recompositions = mmoldb_db::db::get_player_recompositions(conn, &mmolb_player_id)?;
            let player_attribute_augments = mmoldb_db::db::get_player_attribute_augments(conn, &mmolb_player_id)?;
            let player_parties = mmoldb_db::db::get_player_parties(conn, &mmolb_player_id)?;

            Ok::<_, ApiError>((
                players,
                player_modifications,
                player_equipment,
                player_equipment_effects,
                player_reports,
                player_report_attributes,
                modifications,
                player_recompositions,
                player_attribute_augments,
                player_parties,
            ))
        })
        .await?;

    let modifications_table: HashMap<_, _> = modifications
        .into_iter()
        .map(|m| {
            (
                m.id,
                ApiModification {
                    name: m.name,
                    emoji: m.emoji,
                    description: m.description,
                },
            )
        })
        .collect();

    let mut next_player_version = player_versions.into_iter().peekable();
    let mut next_player_modification_version = player_modification_versions.into_iter().peekable();
    let mut next_player_equipment_version = player_equipment_versions.into_iter().peekable();
    let mut next_player_equipment_effect_version = player_equipment_effect_versions.into_iter().peekable();
    let mut next_player_report_version = player_report_versions.into_iter().peekable();
    let mut next_player_report_attribute_version = player_report_attribute_versions.into_iter().peekable();
    let mut next_attribute_augment = attribute_augments.into_iter().peekable();
    let mut next_player_party = player_parties.into_iter().peekable();

    let mut versions: Vec<ApiPlayerVersion> = Default::default();
    let mut active_player = None;
    let mut modifications: Vec<Option<ApiModification>> = Default::default();
    let mut equipment: HashMap<String, Option<ApiEquipment>> = Default::default();
    let mut reports: HashMap<TaxaAttributeCategory, Option<ApiReport>> = Default::default();
    loop {
        let mut next_change_time = NextChangeTime::new();
        next_change_time
            .with_possible_change(next_player_version.peek().map(|v| v.valid_from.and_utc()));
        next_change_time.with_possible_change(
            next_player_modification_version
                .peek()
                .map(|v| v.valid_from.and_utc()),
        );
        next_change_time.with_possible_change(
            next_player_equipment_version
                .peek()
                .map(|v| v.valid_from.and_utc()),
        );
        next_change_time.with_possible_change(
            next_player_equipment_effect_version
                .peek()
                .map(|v| v.valid_from.and_utc()),
        );
        next_change_time.with_possible_change(
            next_player_report_version
                .peek()
                .map(|v| v.valid_from.and_utc()),
        );
        next_change_time.with_possible_change(
            next_player_report_attribute_version
                .peek()
                .map(|v| v.valid_from.and_utc()),
        );

        let Some(time) = next_change_time.into_inner() else {
            break;
        };

        // Note that there's no need to check for closed out mods, equipment, or equipment
        // effects because player.num_modifications, player.occupied_equipment_slots, and
        // equipment.num_effects will do that for us.

        while let Some(player) = next_player_version.next_if(|p| p.valid_from.and_utc() == time) {
            active_player = Some(player);
        }

        let Some(player) = &mut active_player else {
            warn!("A player child table entry became valid before the first player entry");
            continue;
        };

        modifications.resize(player.num_modifications as usize, None);
        while let Some(modif) =
            next_player_modification_version.next_if(|p| p.valid_from.and_utc() == time)
        {
            if let Some(elem) = modifications.get_mut(modif.modification_index as usize) {
                if let Some(api_mod) = modifications_table.get(&modif.modification_id) {
                    *elem = Some(api_mod.clone());
                } else {
                    warn!("Unrecognized modification id {}", modif.modification_id);
                    *elem = None;
                }
            } else {
                warn!(
                    "player_modification_versions table had more modifications than player \
                    num_modifications indicated"
                );
            }
        }

        // There might be a more efficient way to do this
        // This step is the hashmap equivalent to modifications.resize()
        equipment = player
            .occupied_equipment_slots
            .iter()
            .map(|slot| {
                let Some(slot) = slot else {
                    // slot is Option here because of a limitation of the Postgres api: it can't
                    // guarantee non-nullability of array elements. There's no valid reason for
                    // slot to be null, including if there's invalid input data.
                    panic!("Occupied slot should never be None");
                };

                match equipment.remove_entry(slot) {
                    None => {
                        // This is a new slot, populate it with None and it will get overwritten
                        // in the next step
                        (slot.clone(), None)
                    }
                    Some((slot, equipment)) => {
                        // This is a previously occupied slot, carry over its value and it may
                        // get overwritten in the new step
                        (slot, equipment)
                    }
                }
            })
            .collect();

        while let Some(eq) =
            next_player_equipment_version.next_if(|e| e.valid_from.and_utc() == time)
        {
            if let Some(elem) = equipment.get_mut(&eq.equipment_slot) {
                // Updated and new effects will be filled in by the next step
                let effects = if let Some(mut elem) = elem.take() {
                    elem.effects.resize(eq.num_effects as usize, None);
                    elem.effects
                } else {
                    vec![None; eq.num_effects as usize]
                };

                *elem = Some(ApiEquipment {
                    emoji: eq.emoji,
                    name: eq.name,
                    special_type: eq.special_type,
                    description: eq.description,
                    rare_name: eq.rare_name,
                    cost: eq.cost,
                    prefixes: eq.prefixes,
                    suffixes: eq.suffixes,
                    rarity: eq.rarity,
                    effects,
                })
            } else {
                warn!(
                    "player_equipment_versions table had equipment for a slot not present in \
                    occupied_equipment_slots"
                );
            }
        }
        while let Some(effect) =
            next_player_equipment_effect_version.next_if(|e| e.valid_from.and_utc() == time)
        {
            if let Some(eq) = equipment.get_mut(&effect.equipment_slot) {
                if let Some(eq) = eq {
                    if let Some(effect_slot) = eq.effects.get_mut(effect.effect_index as usize) {
                        *effect_slot = Some(ApiEquipmentEffect {
                            attribute: taxa.attribute_from_id(effect.attribute),
                            effect_type: taxa.effect_type_from_id(effect.effect_type),
                            value: effect.value,
                        })
                    } else {
                        warn!(
                            "player_equipment_effect_versions table had more effects than \
                            player_equipment_versions indicated"
                        );
                    }
                } else {
                    warn!(
                        "player_equipment_effect_versions table had an equipment effect for a slot \
                        not present in player_equipment_versions"
                    );
                }
            } else {
                warn!(
                    "player_equipment_effect_versions table had an equipment effect for a slot not \
                    present in occupied_equipment_slots"
                );
            }
        }

        // There might be a more efficient way to do this
        // This step is the hashmap equivalent to modifications.resize()
        reports = player
            .included_report_categories
            .iter()
            .map(|category| {
                let Some(category) = category else {
                    // category is Option here because of a limitation of the Postgres api: it can't
                    // guarantee non-nullability of array elements. There's no valid reason for
                    // category to be null, including if there's invalid input data.
                    panic!("Included category should never be None");
                };

                let category = taxa.attribute_category_from_id(*category);
                match reports.remove_entry(&category) {
                    None => {
                        // This is a new category, populate it with None and it will get overwritten
                        // in the next step
                        (category, None)
                    }
                    Some((category, report)) => {
                        // This is a previously occupied category, carry over its value and it may
                        // get overwritten in the next step
                        (category, report)
                    }
                }
            })
            .collect();

        while let Some(report) =
            next_player_report_version.next_if(|e| e.valid_from.and_utc() == time)
        {
            let category = taxa.attribute_category_from_id(report.category);
            if let Some(elem) = reports.get_mut(&category) {
                // Updated and new attributes will be filled in by the next step
                let attributes = if let Some(mut elem) = elem.take() {
                    report.included_attributes
                        .iter()
                        .map(|attr| {
                            let Some(attr) = attr else {
                                // attr is Option here because of a limitation of the Postgres api: it can't
                                // guarantee non-nullability of array elements. There's no valid reason for
                                // attr to be null, including if there's invalid input data.
                                panic!("Included attribute should never be None");
                            };

                            let attr = taxa.attribute_from_id(*attr);
                            match elem.attributes.remove_entry(&attr) {
                                None => {
                                    // This is a new attribute, populate it with None and it will get overwritten
                                    // in the next step
                                    (attr, None)
                                }
                                Some((attr, value)) => {
                                    // This is a previously occupied attribute, carry over its value and it may
                                    // get overwritten in the next step
                                    (attr, value)
                                }
                            }
                        })
                        .collect()
                } else {
                    report.included_attributes
                        .iter()
                        .map(|attr| {
                            let Some(attr) = attr else {
                                // attr is Option here because of a limitation of the Postgres api: it can't
                                // guarantee non-nullability of array elements. There's no valid reason for
                                // attr to be null, including if there's invalid input data.
                                panic!("Included attribute should never be None");
                            };

                            let attr = taxa.attribute_from_id(*attr);
                            // This is a new attribute because there is no previous report
                            (attr, None)
                        })
                        .collect()
                };

                *elem = Some(ApiReport {
                    season: report.season,
                    day_type: report.day_type.map(|d| taxa.day_type_from_id(d)),
                    day: report.day,
                    superstar_day: report.superstar_day,
                    quote: report.quote,
                    attributes,
                })
            } else {
                warn!(
                    "player_report_versions table had equipment for a slot not present in \
                    included_attributes"
                );
            }
        }
        while let Some(attribute) =
            next_player_report_attribute_version.next_if(|e| e.valid_from.and_utc() == time)
        {
            let category = taxa.attribute_category_from_id(attribute.category);
            let attr = taxa.attribute_from_id(attribute.attribute);
            if let Some(report) = reports.get_mut(&category) {
                if let Some(report) = report {
                    if let Some(effect_slot) = report.attributes.get_mut(&attr) {
                        *effect_slot = Some(ApiReportAttribute {
                            stars: attribute.stars,
                        })
                    } else {
                        warn!(
                            "player_report_attribute_versions table had an entry for an attribute \
                            not present in included_attributes"
                        );
                    }
                } else {
                    warn!(
                        "player_report_attribute_versions table had an entry for an attribute \
                        category not present in player_report_versions"
                    );
                }
            } else {
                warn!(
                    "player_report_attribute_versions table had an entry for an attribute category \
                    not present in included_report_categories"
                );
            }
        }

        let mut events = Vec::new();
        // Handle the unlikely event of getting multiple recomposes between one player version
        let player_name = format!("{} {}", player.first_name, player.last_name);
        // Scan forward to find a recomposition into this player name
        // I don't yet do anything to ensure the time is appropriate given the versions times
        let recomposition_split = player_recompositions.iter()
            .position(|r| r.player_name_after == player_name);
        if let Some(split) = recomposition_split {
            for recomposition in player_recompositions.splice(0..=split, None) {
                events.push(ApiPlayerEvent::Recomposition {
                    time: recomposition.time.and_utc(),
                    new_name: recomposition.player_name_after.clone(),
                    reverts_recomposition: recomposition.reverts_recomposition.map(|dt| dt.and_utc()),
                });
            }
        }

        // For now, assume that events belong to the earliest version whose valid_until
        // is after they occurred
        while let Some(augment) = next_attribute_augment.next_if(|e| e.time.and_utc() <= time) {
            let attribute = taxa.attribute_from_id(augment.attribute);
            events.push(ApiPlayerEvent::AttributeAugment {
                time: augment.time.and_utc(),
                category: taxa.attribute_category_from_id(attribute.as_insertable().category),
                attribute,
                value: augment.value,
            })
        }

        // For now, assume that parties belong to the earliest version whose valid_until
        // is after their game started. I think this still might be accurate enough because
        // players can't change in any other way during party weather.
        while let Some(party) = next_player_party.next_if(|p| p.game_start_time.and_utc() <= time) {
            let attribute = taxa.attribute_from_id(party.attribute);
            events.push(ApiPlayerEvent::Party {
                category: taxa.attribute_category_from_id(attribute.as_insertable().category),
                attribute,
                value: party.value,
            })
        }

        if let Some(last_version) = versions.last_mut() {
            last_version.valid_until = Some(time);
        }
        versions.push(ApiPlayerVersion {
            id: player.mmolb_player_id.clone(),
            valid_from: time,
            valid_until: None, // Will be retroactively populated by the next version
            first_name: player.first_name.clone(),
            last_name: player.last_name.clone(),
            batting_handedness: player
                .batting_handedness
                .map(|h| taxa.handedness_from_id(h)),
            pitching_handedness: player
                .pitching_handedness
                .map(|h| taxa.handedness_from_id(h)),
            home: player.home.clone(),
            birthseason: player.birthseason,
            birthday_type: player.birthday_type.map(|d| taxa.day_type_from_id(d)),
            birthday_day: player.birthday_day,
            birthday_superstar_day: player.birthday_superstar_day,
            likes: player.likes.clone(),
            dislikes: player.dislikes.clone(),
            number: player.number,
            mmolb_team_id: player.mmolb_team_id.clone(),
            slot: player.slot.map(|s| taxa.slot_from_id(s)),
            durability: player.durability,
            greater_boon: player.greater_boon.and_then(|m| {
                if let Some(api_mod) = modifications_table.get(&m) {
                    Some(api_mod.clone())
                } else {
                    warn!("Unrecognized greater boon id {}", m);
                    None
                }
            }),
            lesser_boon: player.lesser_boon.and_then(|m| {
                if let Some(api_mod) = modifications_table.get(&m) {
                    Some(api_mod.clone())
                } else {
                    warn!("Unrecognized lesser boon id {}", m);
                    None
                }
            }),
            modifications: modifications.clone(),
            equipment: equipment.clone(),
            reports: reports.clone(),
            events,
        });
    }

    Ok(Json(ApiPlayerVersions {
        player_id,
        versions,
    }))
}
