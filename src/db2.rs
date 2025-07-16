use chrono::NaiveDateTime;
use diesel::{PgConnection, prelude::*};
use itertools::Itertools;
use log::warn;
use crate::chron::ChronEntity;

pub fn get_latest_entity_valid_from(conn: &mut PgConnection, kind: &str) -> QueryResult<Option<NaiveDateTime>> {
    use crate::data_schema::data::entities::dsl as entities_dsl;

    entities_dsl::entities
        .filter(entities_dsl::kind.eq(kind))
        .select(entities_dsl::valid_from)
        .order_by(entities_dsl::valid_from.desc())
        .limit(1)
        .get_result(conn)
        .optional()
}

#[derive(Insertable)]
#[diesel(table_name = crate::data_schema::data::entities)]
#[diesel(treat_none_as_default_value = false)]
struct NewEntity<'a> {
    pub kind: &'a str,
    pub entity_id: &'a str,
    pub valid_from: NaiveDateTime,
    pub data: &'a serde_json::Value,
}

pub fn insert_entities(conn: &mut PgConnection, entities: Vec<ChronEntity<serde_json::Value>>) -> QueryResult<usize> {
    use crate::data_schema::data::entities::dsl as entities_dsl;

    let new_entities = entities.iter()
        .map(|v| {
            if v.valid_until.is_some() {
                warn!("Chron returned a {} with a non-null valid_until: {}", v.kind, v.entity_id);
            }

            NewEntity {
                kind: &v.kind,
                entity_id: &v.entity_id,
                valid_from: v.valid_from.naive_utc(),
                data: &v.data,
            }
        })
        .collect_vec();

    diesel::copy_from(entities_dsl::entities)
        .from_insertable(&new_entities)
        .execute(conn)
}