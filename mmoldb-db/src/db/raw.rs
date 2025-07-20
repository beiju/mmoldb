use chron::ChronEntity;
use chrono::NaiveDateTime;
use diesel::{PgConnection, prelude::*};
use itertools::Itertools;
use log::warn;

pub fn get_latest_entity_valid_from(
    conn: &mut PgConnection,
    kind: &str,
) -> QueryResult<Option<NaiveDateTime>> {
    use crate::data_schema::data::entities::dsl as entities_dsl;

    entities_dsl::entities
        .filter(entities_dsl::kind.eq(kind))
        .select(entities_dsl::valid_from)
        .order_by(entities_dsl::valid_from.desc())
        .limit(1)
        .get_result(conn)
        .optional()
}

pub fn get_ingest_cursor(
    conn: &mut PgConnection,
) -> QueryResult<Option<(NaiveDateTime, String)>> {
    use crate::data_schema::data::games::dsl as games_dsl;

    games_dsl::games
        .select((games_dsl::from_version, games_dsl::mmolb_game_id))
        .order_by((games_dsl::from_version.desc(), games_dsl::mmolb_game_id.desc()))
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
    use crate::data_schema::data::entities::dsl as entities_dsl;

    let new_entities = entities
        .iter()
        .map(|v| {
            if v.valid_until.is_some() {
                warn!(
                    "Chron returned a {} with a non-null valid_until: {}",
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

pub fn get_batch_of_unprocessed_games(
    conn: &mut PgConnection,
    batch_size: usize,
    cursor: Option<(NaiveDateTime, &str)>,
) -> QueryResult<Vec<ChronEntity<serde_json::Value>>> {
    use crate::data_schema::data::entities::dsl as entities_dsl;
    use crate::data_schema::data::games::dsl as games_dsl;

    // The default values need to be some value that compares less than all
    // valid values in the database
    let cursor_date = cursor.map_or(NaiveDateTime::default(), |(dt, _)| dt);
    let cursor_id = cursor.map_or("", |(_, id)| id);

    entities_dsl::entities
        .filter(entities_dsl::kind.eq("game")
            // Select entities that are after the cursor time, or from the
            // same time and with higher ids
            .and(
                entities_dsl::valid_from.gt(cursor_date)
                    .or(entities_dsl::valid_from.eq(cursor_date).and(entities_dsl::entity_id.gt(cursor_id))),
            )
        )
        // don't join games any more -- instead, caller is responsible for ensuring that cursor
        // is accurate
        // // Join on data.games to see if we have a game imported _from this game version_...
        // .left_join(games_dsl::games.on(
        //     entities_dsl::entity_id.eq(games_dsl::mmolb_game_id)
        //         .and(entities_dsl::valid_from.eq(games_dsl::from_version))
        // ))
        // // ...then filter to only games where we do _not_.
        // .filter(games_dsl::mmolb_game_id.is_null())
        .limit(batch_size as i64)
        .select(Entity::as_select())
        // Callers of this function rely on the results being sorted by
        // (valid_from, entity_id) with the highest id last
        .order_by((entities_dsl::valid_from.asc(), entities_dsl::entity_id.asc()))
        .get_results(conn)
        .map(|entities| {
            entities
                .into_iter()
                .map(|e| ChronEntity {
                    kind: e.kind,
                    entity_id: e.entity_id,
                    valid_from: e.valid_from.and_utc(),
                    valid_until: None,
                    data: e.data,
                })
                .collect_vec()
        })
}

pub fn get_next_cursor(
    conn: &mut PgConnection,
    batch_size: usize,
    cursor: Option<(NaiveDateTime, &str)>,
) -> QueryResult<Option<(NaiveDateTime, String)>> {
    use crate::data_schema::data::entities::dsl as entities_dsl;
    use crate::data_schema::data::games::dsl as games_dsl;

    // The default values need to be some value that compares less than all
    // valid values in the database
    let cursor_date = cursor.map_or(NaiveDateTime::default(), |(dt, _)| dt);
    let cursor_id = cursor.map_or("", |(_, id)| id);

    entities_dsl::entities
        .filter(entities_dsl::kind.eq("game")
            // Select entities that are after the cursor time, or from the
            // same time and with higher ids
            .and(
                entities_dsl::valid_from.gt(cursor_date)
                    .or(entities_dsl::valid_from.eq(cursor_date).and(entities_dsl::entity_id.gt(cursor_id))),
            )
        )
        // don't join games any more -- instead, caller is responsible for ensuring that cursor
        // is accurate
        // // Join on data.games to see if we have a game imported _from this game version_...
        // .left_join(games_dsl::games.on(
        //     entities_dsl::entity_id.eq(games_dsl::mmolb_game_id)
        //         .and(entities_dsl::valid_from.eq(games_dsl::from_version))
        // ))
        // // ...then filter to only games where we do _not_.
        // .filter(games_dsl::mmolb_game_id.is_null())
        .limit(batch_size as i64)
        .select((entities_dsl::valid_from, entities_dsl::entity_id))
        // Callers of this function rely on the results being sorted by
        // (valid_from, entity_id) with the highest id last
        .order_by((entities_dsl::valid_from.asc(), entities_dsl::entity_id.asc()))
        .get_results::<(NaiveDateTime, String)>(conn)
        .map(|vec| vec.into_iter().last())
}
