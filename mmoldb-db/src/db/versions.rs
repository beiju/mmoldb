// entities.rs and versions.rs (as of this writing) contain exactly equivalent functions
// for the two different tables. They do need to be different tables because there are
// database-layer functions that treat them differently.

use chron::ChronEntity;
use chrono::NaiveDateTime;
use diesel::{PgConnection, prelude::*};
use itertools::Itertools;
use crate::QueryError;
use crate::data_schema::data::versions::dsl as versions_dsl;
use crate::data_schema::data::feed_event_versions::dsl as feed_event_versions_dsl;

pub fn get_latest_raw_version_cursor(
    conn: &mut PgConnection,
    kind: &str,
) -> QueryResult<Option<(NaiveDateTime, String)>> {
    versions_dsl::versions
        .filter(versions_dsl::kind.eq(kind))
        .select((versions_dsl::valid_from, versions_dsl::entity_id))
        .order_by((
            versions_dsl::valid_from.desc(),
            versions_dsl::entity_id.desc(),
        ))
        .limit(1)
        .get_result(conn)
        .optional()
}

pub fn get_latest_raw_feed_event_version_cursor(
    conn: &mut PgConnection,
    kind: &str,
) -> QueryResult<Option<(NaiveDateTime, String, i32)>> {
    feed_event_versions_dsl::feed_event_versions
        .filter(feed_event_versions_dsl::kind.eq(kind))
        .select((
            feed_event_versions_dsl::valid_from,
            feed_event_versions_dsl::entity_id,
            feed_event_versions_dsl::feed_event_index,
        ))
        .order_by((
            feed_event_versions_dsl::valid_from.desc(),
            feed_event_versions_dsl::entity_id.desc(),
            feed_event_versions_dsl::feed_event_index.desc(),
        ))
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

// WARNING: This will insert the values that succeed and leave holes for the
// values that fail. Probably not what you want except for debugging.
pub fn insert_versions_with_recovery<'v>(
    conn: &mut PgConnection,
    versions: &'v [ChronEntity<serde_json::Value>],
) -> Vec<Result<(), (QueryError, &'v ChronEntity<serde_json::Value>)>> {
    // This is really inefficient with all the intermediate `Vec`s, but it
    // is useful for debugging
    let num_versions = versions.len();
    if num_versions == 0 {
        return Vec::new();
    }

    match insert_versions(conn, &versions) {
        Ok(results) => {
            assert_eq!(results, num_versions);
            let vecs = (0..results).map(|_| Ok(())).collect_vec();
            assert_eq!(vecs.len(), num_versions); // Make sure I did the ranges syntax right
            vecs
        }
        Err(err) => match versions.len() {
            0 => panic!("This function recursed too far"),
            1 => {
                let (version,) = versions
                    .into_iter()
                    .collect_tuple()
                    .expect("Should have one version");
                vec![Err((err, version))]
            }
            len => {
                let split = len / 2;
                assert_eq!(
                    versions.len(),
                    versions[..split].len() + versions[split..].len()
                );
                let mut vec = insert_versions_with_recovery(conn, &versions[..split]);
                vec.extend(insert_versions_with_recovery(conn, &versions[split..]));
                vec
            }
        },
    }
}

pub fn insert_versions(
    conn: &mut PgConnection,
    versions: &[ChronEntity<serde_json::Value>],
) -> QueryResult<usize> {
    let new_versions = versions
        .iter()
        .map(|v| NewVersion {
            kind: &v.kind,
            entity_id: &v.entity_id,
            valid_from: v.valid_from.naive_utc(),
            data: &v.data,
        })
        .collect_vec();

    diesel::copy_from(versions_dsl::versions)
        .from_insertable(&new_versions)
        .execute(conn)
}

#[derive(Insertable)]
#[diesel(table_name = crate::data_schema::data::feed_event_versions)]
#[diesel(treat_none_as_default_value = false)]
struct NewFeedEventVersion<'a> {
    pub kind: &'a str,
    pub entity_id: &'a str,
    pub feed_event_index: i32,
    pub valid_from: NaiveDateTime,
    pub data: &'a serde_json::Value,
}

pub fn insert_feed_event_versions(
    conn: &mut PgConnection,
    kind: &str,
    versions: &[(String, i32, NaiveDateTime, serde_json::Value)],
) -> QueryResult<usize> {
    let new_versions = versions
        .iter()
        .map(|(id, idx, dt, v)| NewFeedEventVersion {
            kind,
            entity_id: id,
            feed_event_index: *idx,
            valid_from: *dt,
            data: v,
        })
        .collect_vec();

    diesel::copy_from(feed_event_versions_dsl::feed_event_versions)
        .from_insertable(&new_versions)
        .execute(conn)
}
