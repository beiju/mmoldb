use chron::ChronEntity;
use chrono::NaiveDateTime;
use diesel::prelude::*;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use futures::{Stream, TryStreamExt};
use futures::future::Either;
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

fn version_to_chron(v: Version) -> ChronEntity<serde_json::Value> {
    ChronEntity {
        kind: v.kind,
        entity_id: v.entity_id,
        valid_from: v.valid_from.and_utc(),
        valid_to: v.valid_to.map(|dt| dt.and_utc()),
        data: v.data,
    }
}

pub async fn stream_versions_at_cursor(
    conn: &mut AsyncPgConnection,
    kind: &str,
    cursor: Option<(NaiveDateTime, String)>,
) -> QueryResult<impl Stream<Item = QueryResult<ChronEntity<serde_json::Value>>>> {
    let cursor = cursor
        .as_ref()
        .map(|(dt, id)| (*dt, id.as_str()));

    let stream = version_cursor_query(kind, cursor)
        .select(Version::as_select())
        .load_stream(conn)
        .await?
        .map_ok(version_to_chron);

    Ok(stream)
}

pub async fn stream_versions_at_cursor_until(
    conn: &mut AsyncPgConnection,
    kind: &str,
    cursor: Option<(NaiveDateTime, String)>,
    until: Option<NaiveDateTime>,
) -> QueryResult<impl Stream<Item = QueryResult<ChronEntity<serde_json::Value>>>> {
    let Some(until) = until else {
        return stream_versions_at_cursor(conn, kind, cursor).await
            .map(Either::Left);
    };

    let cursor = cursor
        .as_ref()
        .map(|(dt, id)| (*dt, id.as_str()));

    let stream = version_cursor_query(kind, cursor)
        .filter(versions_dsl::valid_from.lt(until))
        .select(Version::as_select())
        .load_stream(conn)
        .await?
        .map_ok(version_to_chron);

    Ok(Either::Right(stream))
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::data_schema::data::feed_event_versions)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub(crate) struct FeedEventVersion {
    pub kind: String,
    pub entity_id: String,
    pub feed_event_index: i32,
    pub valid_from: NaiveDateTime,
    pub valid_until: Option<NaiveDateTime>,
    pub data: serde_json::Value,
}

pub async fn stream_unprocessed_feed_event_versions(
    conn: &mut AsyncPgConnection,
    kind: &str,
) -> QueryResult<impl Stream<Item = QueryResult<ChronEntity<serde_json::Value>>>> {
    use crate::schema::data_schema::data::feed_event_versions::dsl as fev_dsl;
    use crate::schema::data_schema::data::feed_events_processed::dsl as fep_dsl;

    let stream = fev_dsl::feed_event_versions
        .filter(fev_dsl::kind.eq(kind))
        .filter(diesel::dsl::not(diesel::dsl::exists(
            // This subquery is meant to check if there is a corresponding entry in feed_events_processed
            fep_dsl::feed_events_processed
                .filter(fep_dsl::kind.eq(fev_dsl::kind))
                .filter(fep_dsl::entity_id.eq(fev_dsl::entity_id))
                .filter(fep_dsl::feed_event_index.eq(fev_dsl::feed_event_index))
                .filter(fep_dsl::valid_from.eq(fev_dsl::valid_from))
        )))
        // Callers of this function rely on the results being sorted by
        // (valid_from, entity_id) with the highest id last
        .order_by((
            fev_dsl::valid_from.asc(),
            fev_dsl::entity_id.asc(),
            fev_dsl::feed_event_index.asc(),
        ))
        .select(FeedEventVersion::as_select())
        .load_stream(conn)
        .await?
        .map_ok(|v| ChronEntity {
            kind: v.kind,
            entity_id: v.entity_id,
            valid_from: v.valid_from.and_utc(),
            valid_to: v.valid_until.map(|dt| dt.and_utc()),
            // Kind of a hack to smuggle extra data through the machinery
            data: serde_json::json!({
                "feed_event_index": v.feed_event_index,
                "data": v.data,
            }),
        });

    Ok(stream)
}


#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::data_schema::data::entities)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub(crate) struct DbEntity {
    pub kind: String,
    pub entity_id: String,
    pub valid_from: NaiveDateTime,
    pub data: serde_json::Value,
}

pub async fn stream_unprocessed_game_versions(
    conn: &mut AsyncPgConnection,
) -> QueryResult<impl Stream<Item = QueryResult<ChronEntity<serde_json::Value>>>> {
    use crate::schema::data_schema::data::entities::dsl as entities_dsl;
    use crate::schema::data_schema::data::games::dsl as games_dsl;

    let stream = entities_dsl::entities
        .filter(entities_dsl::kind.eq("game"))
        .filter(diesel::dsl::not(diesel::dsl::exists(
            // This subquery is meant to check if there is a corresponding entry in games
            games_dsl::games
                .filter(games_dsl::mmolb_game_id.eq(entities_dsl::entity_id))
                // We want to consider this entity processed if there exists a game
                // from its valid_from *or any later valid_from*
                .filter(games_dsl::from_version.ge(entities_dsl::valid_from))
        )))
        // I don't actually know if return order matters for this one
        .order_by((
            entities_dsl::valid_from.asc(),
            entities_dsl::entity_id.asc(),
        ))
        .select(DbEntity::as_select())
        .load_stream(conn)
        .await?
        .map_ok(|v| ChronEntity {
            kind: v.kind,
            entity_id: v.entity_id,
            valid_from: v.valid_from.and_utc(),
            valid_to: None, // Anything in `entities` by definition is the latest value
            data: v.data,
        });

    Ok(stream)
}
