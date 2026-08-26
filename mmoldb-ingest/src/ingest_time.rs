use crate::PreparedIngestItem;
use crate::ingest::{IngestibleFromVersions, VersionIngestLogs};
use crate::util::day_to_db;
use chron::ChronEntity;
use chrono::{DateTime, Utc};
use futures::Stream;
use itertools::Itertools;
use mmoldb_db::models::{NewTimeVersion, NewVersionIngestLog, NewVersionProcessed};
use mmoldb_db::taxa::Taxa;
use mmoldb_db::{AsyncPgConnection, PgConnection, QueryResult, async_db, db};

pub struct TimeIngestFromVersions;

impl IngestibleFromVersions for TimeIngestFromVersions {
    type Entity = mmolb_parsing::time::Time;
    type Ident = String;

    fn trim_unused(version: &serde_json::Value) -> serde_json::Value {
        // No trimming for `time`
        version.clone()
    }

    fn ident_raw(entity: &ChronEntity<serde_json::Value>) -> Self::Ident {
        entity.entity_id.to_string()
    }

    fn ident(entity: &ChronEntity<Self::Entity>) -> Self::Ident {
        entity.entity_id.to_string()
    }

    fn insert_batch(
        conn: &mut PgConnection,
        taxa: &Taxa,
        versions: &Vec<PreparedIngestItem<Self::Ident, Self::Entity>>,
    ) -> QueryResult<(usize, usize)> {
        let new_time_versions = versions
            .iter()
            .map(|item| match item {
                PreparedIngestItem::MarkAsSkipped(entity_id, valid_from) => {
                    let vp = NewVersionProcessed {
                        kind: "time", // TODO Don't hard-code this
                        entity_id,
                        valid_from: valid_from.naive_utc(),
                        skipped: true,
                        fatal_error: false,
                    };
                    (vp, None, Vec::new())
                }
                PreparedIngestItem::MarkAsFatalError(entity_id, valid_from) => {
                    let vp = NewVersionProcessed {
                        kind: "time", // TODO Don't hard-code this
                        entity_id,
                        valid_from: valid_from.naive_utc(),
                        skipped: false,
                        fatal_error: true,
                    };
                    (vp, None, Vec::new())
                }
                PreparedIngestItem::DoIngest(time) => chron_time_as_new(taxa, &time.entity_id, time.valid_from, &time.data),
            })
            .collect_vec();

        db::insert_time_versions_all(conn, &new_time_versions)
    }

    async fn stream_unprocessed_versions(
        conn: &mut AsyncPgConnection,
        kind: &str,
    ) -> QueryResult<impl Stream<Item = QueryResult<ChronEntity<serde_json::Value>>>> {
        async_db::stream_unprocessed_versions(conn, kind).await
    }
}

fn chron_time_as_new<'a>(
    taxa: &Taxa,
    time_id: &'a str,
    valid_from: DateTime<Utc>,
    time: &'a mmolb_parsing::time::Time,
) -> (
    NewVersionProcessed<'a>,
    Option<NewTimeVersion>,
    Vec<NewVersionIngestLog<'a>>,
) {
    // TODO Can I avoid repeating this string constant?
    let mut ingest_logs = VersionIngestLogs::new("time", time_id, valid_from);

    let new_processed = NewVersionProcessed {
        kind: "time", // TODO Avoid hard-coding this
        entity_id: time_id,
        valid_from: valid_from.naive_utc(),
        skipped: false,
        fatal_error: false,
    };

    let (day_type, day, superstar_day) = day_to_db(Some(&time.season_day), taxa);

    let new_time = NewTimeVersion {
        valid_from: valid_from.naive_utc(),
        valid_until: None,
        season: time.season_number as i32,
        day_type,
        day,
        superstar_day,
    };

    (new_processed, Some(new_time), ingest_logs.into_vec())
}
