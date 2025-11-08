use crate::config::IngestibleConfig;
use crate::ingest::VersionIngestLogs;
use crate::{IngestStage, Ingestable, IngestibleFromVersions, Stage2Ingest, VersionStage1Ingest};
use chron::ChronEntity;
use chrono::{DateTime, NaiveDateTime, Utc};
use itertools::Itertools;
use mmolb_parsing::enums::LinkType;
use mmolb_parsing::feed_event::FeedEvent;
use mmoldb_db::models::{NewTeamFeedVersion, NewTeamGamePlayed, NewVersionIngestLog};
use mmoldb_db::taxa::Taxa;
use mmoldb_db::{db, PgConnection, QueryResult};
use serde::Deserialize;
use std::sync::Arc;
use mmolb_parsing::team_feed::ParsedTeamFeedEventText;

#[derive(Deserialize)]
pub struct TeamFeedContainer {
    feed: Vec<FeedEvent>,
}

pub struct TeamFeedIngestFromVersions;

impl IngestibleFromVersions for TeamFeedIngestFromVersions {
    type Entity = TeamFeedContainer;

    fn get_start_cursor(conn: &mut PgConnection) -> QueryResult<Option<(NaiveDateTime, String)>> {
        db::get_team_feed_ingest_start_cursor(conn)
    }

    fn trim_unused(version: &serde_json::Value) -> serde_json::Value {
        version.clone()
    }

    fn insert_batch(conn: &mut PgConnection, _: &Taxa, versions: &Vec<ChronEntity<Self::Entity>>) -> QueryResult<usize> {
        let new_versions = versions.iter()
            .map(|team| chron_team_feed_as_new(&team.entity_id, team.valid_from, &team.data.feed))
            .collect_vec();

        db::insert_team_feed_versions(conn, &new_versions)
    }
}

pub struct TeamFeedIngest(&'static IngestibleConfig);

impl TeamFeedIngest {
    pub fn new(config: &'static IngestibleConfig) -> TeamFeedIngest {
        TeamFeedIngest(config)
    }
}

impl Ingestable for TeamFeedIngest {
    const KIND: &'static str = "team_feed";

    fn config(&self) -> &'static IngestibleConfig {
        &self.0
    }

    fn stages(&self) -> Vec<Arc<dyn IngestStage>> {
        vec![
            Arc::new(VersionStage1Ingest::new(Self::KIND)),
            Arc::new(Stage2Ingest::new(Self::KIND, TeamFeedIngestFromVersions))
        ]
    }
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
    let mut ingest_logs = VersionIngestLogs::new(TeamFeedIngest::KIND, team_id, valid_from);

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

        let parsed_event = mmolb_parsing::team_feed::parse_team_feed_event(event);

        match parsed_event {
            ParsedTeamFeedEventText::ParseError { error, text } => {
                // I'm making this a warning because we don't care about most event types
                // (and we can handle having a game for which we don't know the end time)
                ingest_logs.warn(format!("Error parsing \"{text}\": {error}"));
            }
            ParsedTeamFeedEventText::GameResult { .. } => {
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
            // TODO Add NewTeamGamePlayed for every other event type that happens at the
            //   end of the game
            _ => {}
        }
    }


    (
        team_feed_version,
        game_outcomes,
        ingest_logs.into_vec(),
    )
}
