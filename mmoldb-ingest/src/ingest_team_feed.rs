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
    todo!()
    // crate::ingest::ingest(
    //     ingest_id,
    //     TEAM_FEED_KIND,
    //     config,
    //     pool,
    //     abort,
    //     |version| version.clone(),
    //     db::get_team_feed_ingest_start_cursor,
    //     ingest_page_of_team_feeds,
    // )
    // .await
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
