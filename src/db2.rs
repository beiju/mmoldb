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

#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::data_schema::data::entities)]
#[diesel(check_for_backend(diesel::pg::Pg))]
struct Entity {
    pub kind: String,
    pub entity_id: String,
    pub valid_from: NaiveDateTime,
    pub data: serde_json::Value,
}

pub fn get_batch_of_unprocessed_games(conn: &mut PgConnection, batch_size: usize) -> QueryResult<Vec<ChronEntity<serde_json::Value>>> {
    use crate::data_schema::data::entities::dsl as entities_dsl;
    use crate::data_schema::data::games::dsl as games_dsl;

    entities_dsl::entities
        .filter(entities_dsl::kind.eq("game"))
        // TODO Also store the valid_from that this Game was generated from, and
        //   use it to re-process games that have a new version. This should be rare.
        .left_join(games_dsl::games.on(entities_dsl::entity_id.eq(games_dsl::mmolb_game_id)))
        .filter(games_dsl::mmolb_game_id.is_null())
        .limit(batch_size as i64)
        .select(Entity::as_select())
        .get_results(conn)
        .map(|entities| entities
            .into_iter()
            .map(|e| ChronEntity {
                kind: e.kind,
                entity_id: e.entity_id,
                valid_from: e.valid_from.and_utc(),
                valid_until: None,
                data: e.data,
            })
            .collect_vec()
        )
}