// entities.rs and versions.rs (as of this writing) contain exactly equivalent functions
// for the two different tables. They do need to be different tables because there are
// database-layer functions that treat them differently.

use chron::ChronEntity;
use chrono::NaiveDateTime;
use diesel::{PgConnection, prelude::*};
use itertools::Itertools;
use log::warn;

use crate::data_schema::data::versions::dsl as versions_dsl;

pub fn get_latest_version_valid_from(
    conn: &mut PgConnection,
    kind: &str,
) -> QueryResult<Option<NaiveDateTime>> {
    versions_dsl::versions
        .filter(versions_dsl::kind.eq(kind))
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
    pub data: &'a serde_json::Value,
}

pub fn insert_versions(
    conn: &mut PgConnection,
    versions: Vec<ChronEntity<serde_json::Value>>,
) -> QueryResult<usize> {
    let new_versions = versions
        .iter()
        .map(|v| {
            if v.valid_to.is_some() {
                warn!(
                    "Chron returned a {} with a non-null valid_to: {}",
                    v.kind, v.entity_id
                );
            }

            NewVersion {
                kind: &v.kind,
                entity_id: &v.entity_id,
                valid_from: v.valid_from.naive_utc(),
                data: &v.data,
            }
        })
        .collect_vec();

    diesel::copy_from(versions_dsl::versions)
        .from_insertable(&new_versions)
        .execute(conn)
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::data_schema::data::versions)]
#[diesel(check_for_backend(diesel::pg::Pg))]
struct Version {
    pub kind: String,
    pub entity_id: String,
    pub valid_from: NaiveDateTime,
    pub valid_to: Option<NaiveDateTime>,
    pub data: serde_json::Value,
}

#[diesel::dsl::auto_type]
fn version_cursor_query_diesel<'a, 'b>(
    kind: &'a str,
    cursor_date: NaiveDateTime,
    cursor_id: &'b str,
) -> _ {
    versions_dsl::versions
        .filter(
            versions_dsl::kind
                .eq(kind)
                // Select versions that are after the cursor time, or from the
                // same time and with higher ids
                .and(
                    versions_dsl::valid_from
                        .gt(cursor_date)
                        .or(versions_dsl::valid_from
                            .eq(cursor_date)
                            .and(versions_dsl::entity_id.gt(cursor_id))),
                ),
        )
        // Callers of this function rely on the results being sorted by
        // (valid_from, entity_id) with the highest id last
        .order_by((
            versions_dsl::valid_from.asc(),
            versions_dsl::entity_id.asc(),
        ))
}

fn version_cursor_query<'a, 'b>(
    kind: &'a str,
    cursor: Option<(NaiveDateTime, &'b str)>,
) -> version_cursor_query_diesel<'a, 'b> {
    // The default values need to be some value that compares less than all
    // valid values in the database
    let cursor_date = cursor.map_or(NaiveDateTime::default(), |(dt, _)| dt);
    let cursor_id = cursor.map_or("", |(_, id)| id);

    version_cursor_query_diesel(kind, cursor_date, cursor_id)
}

pub fn advance_version_cursor(
    conn: &mut PgConnection,
    kind: &str,
    cursor: Option<(NaiveDateTime, &str)>,
    advance_by: usize,
) -> QueryResult<Option<(NaiveDateTime, String)>> {
    // There may be a way to use offset() here, but it needs to return the last
    // value even if there are fewer than advance_by values. I don't think
    // offset() does that. And the amount of data transferred is hopefully
    // negligible anyway.
    version_cursor_query(kind, cursor)
        .select((versions_dsl::valid_from, versions_dsl::entity_id))
        .limit(advance_by as i64)
        .get_results::<(NaiveDateTime, String)>(conn)
        .map(|vec| vec.into_iter().last())
}

pub fn get_versions_at_cursor(
    conn: &mut PgConnection,
    kind: &str,
    batch_size: usize,
    cursor: Option<(NaiveDateTime, &str)>,
) -> QueryResult<Vec<ChronEntity<serde_json::Value>>> {
    version_cursor_query(kind, cursor)
        .select(Version::as_select())
        .limit(batch_size as i64)
        .get_results(conn)
        .map(|versions| {
            versions
                .into_iter()
                .map(|v| ChronEntity {
                    kind: v.kind,
                    entity_id: v.entity_id,
                    valid_from: v.valid_from.and_utc(),
                    valid_to: v.valid_to.map(|dt| dt.and_utc()),
                    data: v.data,
                })
                .collect_vec()
        })
}
