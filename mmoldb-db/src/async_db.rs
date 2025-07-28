use chrono::NaiveDateTime;
use diesel::prelude::*;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use chron::ChronEntity;
use futures::{Stream, StreamExt, TryStreamExt};
use itertools::Itertools;

use crate::schema::data_schema::data::versions::dsl as versions_dsl;

#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::data_schema::data::versions)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub(crate) struct Version {
    pub kind: String,
    pub entity_id: String,
    pub valid_from: NaiveDateTime,
    pub valid_to: Option<NaiveDateTime>,
    pub data: serde_json::Value,
}

#[diesel::dsl::auto_type]
pub(crate) fn version_cursor_query_diesel<'a, 'b>(
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

pub(crate) fn version_cursor_query<'a, 'b>(
    kind: &'a str,
    cursor: Option<(NaiveDateTime, &'b str)>,
) -> version_cursor_query_diesel<'a, 'b> {
    // The default values need to be some value that compares less than all
    // valid values in the database
    let cursor_date = cursor.map_or(NaiveDateTime::default(), |(dt, _)| dt);
    let cursor_id = cursor.map_or("", |(_, id)| id);

    version_cursor_query_diesel(kind, cursor_date, cursor_id)
}
pub async fn stream_versions_at_cursor(
    conn: &mut AsyncPgConnection,
    kind: &str,
    cursor: Option<(NaiveDateTime, &str)>,
) -> QueryResult<impl Stream<Item = QueryResult<ChronEntity<serde_json::Value>>>> {
    let stream = version_cursor_query(kind, cursor)
        .select(Version::as_select())
        .load_stream(conn)
        .await?
        .map_ok(|v| ChronEntity {
            kind: v.kind,
            entity_id: v.entity_id,
            valid_from: v.valid_from.and_utc(),
            valid_to: v.valid_to.map(|dt| dt.and_utc()),
            data: v.data,
        });

    Ok(stream)
}
