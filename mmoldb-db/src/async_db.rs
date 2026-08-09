use crate::models::DbVersion;
use chron::{ChronEntity, ChronFeedEvent};
use chrono::NaiveDateTime;
use diesel::prelude::*;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use futures::{Stream, TryStreamExt};

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

pub async fn stream_unprocessed_feed_events(
    conn: &mut AsyncPgConnection,
) -> QueryResult<impl Stream<Item = QueryResult<ChronFeedEvent<serde_json::Value>>>> {
    use crate::schema::data_schema::data::feed_events::dsl as fe_dsl;
    use crate::schema::data_schema::data::feed_events_processed::dsl as fep_dsl;

    let stream = fe_dsl::feed_events
        .filter(diesel::dsl::not(diesel::dsl::exists(
            // This subquery is meant to check if there is a corresponding entry in feed_events_processed
            fep_dsl::feed_events_processed
                .filter(fep_dsl::event_id.eq(fe_dsl::event_id))
                .filter(fep_dsl::timestamp.eq(fe_dsl::timestamp))
        )))
        // Callers of this function rely on the results being sorted by
        // (timestamp, event_id) with the highest id last
        .order_by((
            fe_dsl::timestamp.asc(),
            fe_dsl::event_id.asc(),
        ))
        // Unfortunately ::as_select() can't be composed, so all the fields must be listed manually
        .select((
            // This is the version to be processed
            fe_dsl::event_id,
            fe_dsl::subject_type,
            fe_dsl::subject_id,
            fe_dsl::timestamp,
            fe_dsl::data,
        ))
        // .select((FeedEventVersion::as_select(), FeedEventVersion::as_select().nullable()))
        .load_stream::<(
            // This is the version to be processed
            String,
            String,
            String,
            NaiveDateTime,
            serde_json::Value,
        )>(conn)
        .await?
        .map_ok(|(
             event_id,
             subject_type,
             subject_id,
             timestamp,
             data,
         )| {
            ChronFeedEvent {
                event_id,
                subject_type,
                subject_id,
                timestamp: timestamp.and_utc(),
                data,
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
