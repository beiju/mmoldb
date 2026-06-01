use std::collections::HashMap;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use mmoldb_db::{Connection, PgConnection, QueryResult};
use mmoldb_db::taxa::{Taxa, TaxaAttribute, TaxaEffectType};
use mmoldb_db::models::NewModificationEffects;
use mmoldb_db::db;

const MODIFIER_VALUES_JSON: &str = include_str!("../mmolb-modifiers/modifiers.json");

#[derive(Debug, Clone, Deserialize)]
struct ModifierEffects {
    valid_from: DateTime<Utc>,
    valid_until: Option<DateTime<Utc>>,
    effects: HashMap<TaxaAttribute, f64>,
    bonus_type: TaxaEffectType,
}

type ModifiersEffects = HashMap<String, Vec<ModifierEffects>>;

pub fn update_modifier_effects_values(
    conn: &mut PgConnection,
    taxa: &Taxa,
) -> QueryResult<()> {
    let modifier_list: ModifiersEffects = serde_json::from_str(MODIFIER_VALUES_JSON)
        .expect("Failed to deserialize modifiers list");

    let modifiers_for_db = modifier_list.iter()
        .flat_map(|(modification_name, modifier_effects_configs)| {
            modifier_effects_configs.iter()
                .flat_map(|effects_config| {
                    effects_config.effects.iter()
                        .map(|(effect_attribute, effect_value)| {
                            NewModificationEffects {
                                modification_name,
                                valid_from: effects_config.valid_from.naive_utc(),
                                valid_until: effects_config.valid_until.as_ref().map(DateTime::naive_utc),
                                attribute: taxa.attribute_id(*effect_attribute),
                                bonus_type: taxa.effect_type_id(effects_config.bonus_type),
                                value: *effect_value,
                            }
                        })
                })
        })
        .collect();

    conn.transaction(move |conn| {
        db::replace_modifier_effects(conn, modifiers_for_db)
    })
}