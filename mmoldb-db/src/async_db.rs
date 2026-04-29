use crate::schema::data_schema::data::versions::dsl as versions_dsl;
use chron::ChronEntity;
use chrono::NaiveDateTime;
use diesel::prelude::*;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use futures::future::Either;
use futures::{Stream, TryStreamExt};
use crate::models::DbVersion;

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

pub async fn stream_unprocessed_versions(
    conn: &mut AsyncPgConnection,
    kind: &str,
) -> QueryResult<impl Stream<Item = QueryResult<ChronEntity<serde_json::Value>>>> {
    use crate::schema::data_schema::data::versions::dsl as v_dsl;
    use crate::schema::data_schema::data::versions_processed::dsl as vp_dsl;

    let stream = v_dsl::versions
        .filter(v_dsl::kind.eq(kind))
        .filter(diesel::dsl::not(diesel::dsl::exists(
            // This subquery is meant to check if there is a corresponding entry in versions_processed
            vp_dsl::versions_processed
                .filter(vp_dsl::kind.eq(v_dsl::kind))
                .filter(vp_dsl::entity_id.eq(v_dsl::entity_id))
                .filter(vp_dsl::valid_from.eq(v_dsl::valid_from))
        )))
        // Callers of this function rely on the results being sorted by
        // (valid_from, entity_id) with the highest id last
        .order_by((
            v_dsl::valid_from.asc(),
            v_dsl::entity_id.asc(),
        ))
        .select(DbVersion::as_select())
        .load_stream::<DbVersion>(conn)
        .await?
        .map_ok(|v| {
            ChronEntity {
                kind: v.kind,
                entity_id: v.entity_id,
                valid_from: v.valid_from.and_utc(),
                valid_to: v.valid_to.map(|dt| dt.and_utc()),
                // Kind of a hack to smuggle extra data through the machinery
                data: v.data,
            }
        });

    Ok(stream)
}

pub async fn stream_unprocessed_feed_event_versions(
    conn: &mut AsyncPgConnection,
    kind: &str,
) -> QueryResult<impl Stream<Item = QueryResult<ChronEntity<serde_json::Value>>>> {
    use crate::schema::data_schema::data::feed_event_versions::dsl as fev_dsl;
    use crate::schema::data_schema::data::feed_events_processed::dsl as fep_dsl;

    let prev_version =
        diesel::alias!(crate::schema::data_schema::data::feed_event_versions as prev_version1);

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
        .left_join(
            // Select the previous version by using its valid_until
            prev_version.on(
                fev_dsl::kind.eq(prev_version.field(fev_dsl::kind))
                    .and(fev_dsl::entity_id.eq(prev_version.field(fev_dsl::entity_id)))
                    .and(fev_dsl::feed_event_index.eq(prev_version.field(fev_dsl::feed_event_index)))
                    // This is the line that makes the association between
                    // one entry's valid_from and the other's valid_until
                    .and(fev_dsl::valid_from.nullable().eq(prev_version.field(fev_dsl::valid_until)))
            )
        )
        // Callers of this function rely on the results being sorted by
        // (valid_from, entity_id) with the highest id last
        .order_by((
            fev_dsl::valid_from.asc(),
            fev_dsl::entity_id.asc(),
            fev_dsl::feed_event_index.asc(),
        ))
        // Unfortunately ::as_select() can't be composed, so all the fields must be listed manually
        .select((
            // This is the version to be processed
            fev_dsl::kind,
            fev_dsl::entity_id,
            fev_dsl::feed_event_index,
            fev_dsl::valid_from,
            fev_dsl::valid_until,
            fev_dsl::data,
            // This are the previous version, nullable because it may not exist
            prev_version.field(fev_dsl::valid_from).nullable(),
            prev_version.field(fev_dsl::data).nullable(),
        ))
        // .select((FeedEventVersion::as_select(), FeedEventVersion::as_select().nullable()))
        .load_stream::<(
            // This is the version to be processed
            String,
            String,
            i32,
            NaiveDateTime,
            Option<NaiveDateTime>,
            serde_json::Value,
            // This is the previous version, nullable because it may not exist
            Option<NaiveDateTime>,
            Option<serde_json::Value>,
        )>(conn)
        .await?
        .map_ok(|(
             kind,
             entity_id,
             feed_event_index,
             valid_from,
             valid_until,
             data,
             prev_valid_from,
             prev_data,
         )| {
            ChronEntity {
                kind,
                entity_id,
                valid_from: valid_from.and_utc(),
                valid_to: valid_until.map(|dt| dt.and_utc()),
                // Kind of a hack to smuggle extra data through the machinery
                data: serde_json::json!({
                    "feed_event_index": feed_event_index,
                    "data": data,
                    // All other prev_* fields are constrained to be equal to the
                    // corresponding field from the current version, except
                    // prev_valid_until is the current version's valid_from
                    "prev_valid_from": prev_valid_from.map(|dt| dt.and_utc()),
                    "prev_data": prev_data,
                }),
            }
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
                .filter(games_dsl::from_version.ge(entities_dsl::valid_from)),
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
