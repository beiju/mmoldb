use crate::ingest::{VersionIngestLogs, batch_by_entity, IngestFatalError};
use chron::ChronEntity;
use chrono::{DateTime, Utc};
use itertools::{Itertools};
use log::{debug, info};
use mmolb_parsing::enums::{LinkType};
use mmolb_parsing::feed_event::{FeedEvent, ParsedFeedEventText};
use mmoldb_db::taxa::Taxa;
use mmoldb_db::{PgConnection, db, ConnectionPool};
use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;
use serde::Deserialize;
use tokio_util::sync::CancellationToken;
use mmoldb_db::models::{NewTeamFeedVersion, NewTeamGamePlayed, NewVersionIngestLog};
use crate::config::IngestibleConfig;

// I made this a constant because I'm constant-ly terrified of typoing
// it and introducing a difficult-to-find bug
const TEAM_FEED_KIND: &'static str = "team_feed";

pub async fn ingest_team_feeds(
    ingest_id: i64,
    pool: ConnectionPool,
    abort: CancellationToken,
    config: &IngestibleConfig,
) -> Result<(), IngestFatalError> {
    crate::ingest::ingest(
        ingest_id,
        TEAM_FEED_KIND,
        config,
        pool,
        abort,
        |version| version.clone(),
        db::get_team_feed_ingest_start_cursor,
        ingest_page_of_team_feeds,
    )
    .await
}

pub fn ingest_page_of_team_feeds(
    _taxa: &Taxa,
    raw_team_feeds: Vec<ChronEntity<serde_json::Value>>,
    conn: &mut PgConnection,
    worker_id: usize,
) -> Result<i32, IngestFatalError> {
    debug!(
        "Starting of {} team feeds on worker {worker_id}",
        raw_team_feeds.len()
    );
    let save_start = Utc::now();

    #[derive(Deserialize)]
    struct FeedContainer {
        feed: Vec<FeedEvent>,
    }

    let deserialize_start = Utc::now();
    // TODO Gracefully handle team feed deserialize failure
    let team_feeds = raw_team_feeds
        .into_par_iter()
        .map(|game_json| {
            Ok::<ChronEntity<FeedContainer>, serde_json::Error>(ChronEntity {
                kind: game_json.kind,
                entity_id: game_json.entity_id,
                valid_from: game_json.valid_from,
                valid_to: game_json.valid_to,
                data: serde_json::from_value(game_json.data)?,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let deserialize_duration = (Utc::now() - deserialize_start).as_seconds_f64();
    debug!(
        "Deserialized page of {} team feeds in {:.2} seconds on worker {}",
        team_feeds.len(),
        deserialize_duration,
        worker_id
    );

    let latest_time = team_feeds
        .last()
        .map(|version| version.valid_from)
        .unwrap_or(Utc::now());
    let time_ago = latest_time.signed_duration_since(Utc::now());
    let human_time_ago = chrono_humanize::HumanTime::from(time_ago);

    // Convert to Insertable type
    let new_team_feeds = team_feeds
        .iter()
        .map(|v| chron_team_feed_as_new(&v.entity_id, v.valid_from, &v.data.feed))
        .collect_vec();

    // The handling of valid_until is entirely in the database layer, but its logic
    // requires that a given batch of teams does not have the same team twice. We
    // provide that guarantee here.
    let new_team_feeds_len = new_team_feeds.len();
    let mut total_inserted = 0;
    for batch in batch_by_entity(new_team_feeds, |v| v.0.mmolb_team_id) {
        let to_insert = batch.len();
        info!(
            "Sent {} new team feed versions out of {} to the database.",
            to_insert, new_team_feeds_len,
        );

        let inserted = db::insert_team_feed_versions(conn, &batch)?;
        total_inserted += inserted as i32;

        info!(
            "Sent {} new team feed versions out of {} to the database. \
            {inserted} versions were actually inserted, the rest were duplicates. \
            Currently processing team feeds from {human_time_ago}.",
            to_insert,
            team_feeds.len(),
        );
    }

    let save_duration = (Utc::now() - save_start).as_seconds_f64();

    info!(
        "Ingested page of {} team feeds in {save_duration:.3} seconds.",
        team_feeds.len(),
    );

    Ok(total_inserted)
}


pub fn chron_team_feed_as_new<'a>(
    team_id: &'a str,
    valid_from: DateTime<Utc>,
    feed_items: &'a [FeedEvent],
) -> (
    NewTeamFeedVersion<'a>,
    Vec<NewTeamGamePlayed<'a>>,
    Vec<NewVersionIngestLog<'a>>,
) {
    let mut ingest_logs = VersionIngestLogs::new(TEAM_FEED_KIND, team_id, valid_from);

    let team_feed_version = NewTeamFeedVersion {
        mmolb_team_id: team_id,
        valid_from: valid_from.naive_utc(),
        valid_until: None,
        num_entries: feed_items.len() as i32,
    };

    let mut game_outcomes = Vec::new();

    for (index, event) in feed_items.iter().enumerate() {
        let feed_event_index = index as i32;
        let time = event.timestamp.naive_utc();

        // There is a bug in mmolb_parsing that causes a panic when an
        // augment's text is empty
        if event.text.is_empty() {
            continue;
        }

        let parsed_event = mmolb_parsing::feed_event::parse_feed_event(event);

        match parsed_event {
            ParsedFeedEventText::ParseError { error, text } => {
                // I'm making this a warning because we don't care about most event types
                // (and we can handle having a game for which we don't know the end time)
                ingest_logs.warn(format!("Error parsing \"{text}\": {error}"));
            }
            ParsedFeedEventText::GameResult { .. } => {
                let game_link = event.links
                    .iter()
                    .filter(|link| link.link_type == Ok(LinkType::Game))
                    .exactly_one();

                match game_link {
                    Ok(game_link) => {
                        game_outcomes.push(NewTeamGamePlayed {
                            mmolb_team_id: team_id,
                            feed_event_index,
                            time,
                            mmolb_game_id: &game_link.id,
                        });
                    }
                    Err(err) => {
                        ingest_logs.warn(format!(
                            "Game outcome in {} feed index {} had {} game links (expected 1)",
                            team_id, index, err.count()
                        ))
                    }
                }
            }
            // We don't care about any other type of event
            _ => {}
        }
    }


    (
        team_feed_version,
        game_outcomes,
        ingest_logs.into_vec(),
    )
}
