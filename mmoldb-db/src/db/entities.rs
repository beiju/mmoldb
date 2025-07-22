// entities.rs and versions.rs (as of this writing) contain exactly equivalent functions
// for the two different tables. They do need to be different tables because there are
// database-layer functions that treat them differently.

use chron::ChronEntity;
use chrono::NaiveDateTime;
use diesel::{PgConnection, prelude::*};
use itertools::Itertools;
use log::warn;

use crate::data_schema::data::entities::dsl as entities_dsl;

pub fn get_latest_entity_valid_from(
    conn: &mut PgConnection,
    kind: &str,
) -> QueryResult<Option<NaiveDateTime>> {
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

pub fn insert_entities(
    conn: &mut PgConnection,
    entities: Vec<ChronEntity<serde_json::Value>>,
) -> QueryResult<usize> {
    let new_entities = entities
        .iter()
        .map(|v| {
            if v.valid_to.is_some() {
                warn!(
                    "Chron returned a {} with a non-null valid_to: {}",
                    v.kind, v.entity_id
                );
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

#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::data_schema::data::entities)]
#[diesel(check_for_backend(diesel::pg::Pg))]
struct Entity {
    pub kind: String,
    pub entity_id: String,
    pub valid_from: NaiveDateTime,
    pub data: serde_json::Value,
}

#[diesel::dsl::auto_type]
fn entity_cursor_query_diesel<'a, 'b>(
    kind: &'a str,
    cursor_date: NaiveDateTime,
    cursor_id: &'b str,
) -> _ {
    entities_dsl::entities
        .filter(
            entities_dsl::kind
                .eq(kind)
                // Select entities that are after the cursor time, or from the
                // same time and with higher ids
                .and(
                    entities_dsl::valid_from
                        .gt(cursor_date)
                        .or(entities_dsl::valid_from
                            .eq(cursor_date)
                            .and(entities_dsl::entity_id.gt(cursor_id))),
                ),
        )
        // Callers of this function rely on the results being sorted by
        // (valid_from, entity_id) with the highest id last
        .order_by((
            entities_dsl::valid_from.asc(),
            entities_dsl::entity_id.asc(),
        ))
}

fn entity_cursor_query<'a, 'b>(
    kind: &'a str,
    cursor: Option<(NaiveDateTime, &'b str)>,
) -> entity_cursor_query_diesel<'a, 'b> {
    // The default values need to be some value that compares less than all
    // valid values in the database
    let cursor_date = cursor.map_or(NaiveDateTime::default(), |(dt, _)| dt);
    let cursor_id = cursor.map_or("", |(_, id)| id);

    entity_cursor_query_diesel(kind, cursor_date, cursor_id)
}

pub fn advance_entity_cursor(
    conn: &mut PgConnection,
    kind: &str,
    cursor: Option<(NaiveDateTime, &str)>,
    advance_by: usize,
) -> QueryResult<Option<(NaiveDateTime, String)>> {
    // There may be a way to use offset() here, but it needs to return the last
    // value even if there are fewer than advance_by values. I don't think
    // offset() does that. And the amount of data transferred is hopefully
    // negligible anyway.
    entity_cursor_query(kind, cursor)
        .select((entities_dsl::valid_from, entities_dsl::entity_id))
        .limit(advance_by as i64)
        .get_results::<(NaiveDateTime, String)>(conn)
        .map(|vec| vec.into_iter().last())
}

pub fn get_entities_at_cursor(
    conn: &mut PgConnection,
    kind: &str,
    batch_size: usize,
    cursor: Option<(NaiveDateTime, &str)>,
) -> QueryResult<Vec<ChronEntity<serde_json::Value>>> {
    entity_cursor_query(kind, cursor)
        .select(Entity::as_select())
        .limit(batch_size as i64)
        .get_results(conn)
        .map(|entities| {
            entities
                .into_iter()
                .map(|e| ChronEntity {
                    kind: e.kind,
                    entity_id: e.entity_id,
                    valid_from: e.valid_from.and_utc(),
                    valid_to: None,
                    data: e.data,
                })
                .collect_vec()
        })
}
