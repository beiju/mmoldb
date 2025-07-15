use chrono::NaiveDateTime;
use diesel::{PgConnection, prelude::*};
use itertools::Itertools;
use crate::chron::ChronEntity;

pub fn get_latest_game_version_valid_from(conn: &mut PgConnection) -> QueryResult<Option<NaiveDateTime>> {
    use crate::data_schema::data::versions::dsl as versions_dsl;

    versions_dsl::versions
        .select(versions_dsl::valid_from)
        .order_by(versions_dsl::valid_from.desc())
        .limit(1)
        .get_result(conn)
        .optional()
}

#[derive(Insertable)]
#[diesel(table_name = crate::data_schema::data::versions)]
#[diesel(treat_none_as_default_value = false)]
struct NewVersion<'a> {
    pub kind: &'a str,
    pub entity_id: &'a str,
    pub valid_from: NaiveDateTime,
    pub valid_to: Option<NaiveDateTime>,
    pub data: &'a serde_json::Value,
}

pub fn insert_versions(conn: &mut PgConnection, versions: Vec<ChronEntity<serde_json::Value>>) -> QueryResult<usize> {
    use crate::data_schema::data::versions::dsl as versions_dsl;

    let new_versions = versions.iter()
        .map(|v| NewVersion {
            kind: &v.kind,
            entity_id: &v.entity_id,
            valid_from: v.valid_from.naive_utc(),
            valid_to: v.valid_until.map(|d| d.naive_utc()),
            data: &v.data,
        })
        .collect_vec();

    diesel::copy_from(versions_dsl::versions)
        .from_insertable(&new_versions)
        .execute(conn)
}