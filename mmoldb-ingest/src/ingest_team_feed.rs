use crate::config::IngestibleConfig;
use crate::ingest::VersionIngestLogs;
use crate::ingest_player_feed::ingest_feed_shared::{FeedItemContainer, IGNORE_EVENTS_ENDING, IGNORE_EVENTS_STARTING};
use crate::{FeedEventVersionStage1Ingest, IngestStage, Ingestable, IngestibleFromVersions, Stage2Ingest};
use chron::ChronEntity;
use chrono::{DateTime, NaiveDateTime, Utc};
use futures::Stream;
use itertools::Itertools;
use mmolb_parsing::enums::LinkType;
use mmolb_parsing::team_feed::ParsedTeamFeedEventText;
use mmoldb_db::models::{NewFeedEventProcessed, NewTeamGamePlayed, NewVersionIngestLog};
use mmoldb_db::taxa::Taxa;
use mmoldb_db::{async_db, db, AsyncPgConnection, Connection, PgConnection, QueryResult};
use std::sync::Arc;


pub struct TeamFeedIngestFromVersions;

impl IngestibleFromVersions for TeamFeedIngestFromVersions {
    type Entity = FeedItemContainer;

    fn get_start_cursor(_: &mut PgConnection) -> QueryResult<Option<(NaiveDateTime, String)>> {
        // TODO: This is None because I'm not using a cursor for team feeds any more. Update
        //   the infrastructure to not require a stub.
        Ok(None)
    }

    fn trim_unused(version: &serde_json::Value) -> serde_json::Value {
        version.clone()
    }

    fn insert_batch(conn: &mut PgConnection, _: &Taxa, versions: &Vec<ChronEntity<Self::Entity>>) -> QueryResult<(usize, usize)> {
        let new_versions = versions.iter()
            .map(|team| chron_team_feed_as_new(&team.entity_id, team.valid_from, &team.data))
            .collect_vec();

        conn.transaction(|c| {
            db::insert_team_feed_versions(c, &new_versions)
        })
    }

    async fn stream_versions_at_cursor(
        conn: &mut AsyncPgConnection,
        kind: &str,
        _: Option<(NaiveDateTime, String)>,
    ) -> QueryResult<impl Stream<Item=QueryResult<ChronEntity<serde_json::Value>>>> {
        // This ingestible doesn't use a cursor. I used to have an assert that cursor
        // was None, but that's incorrect because the machinery opportunistically updates
        // the cursor based on values that are passing through
        async_db::stream_unprocessed_feed_event_versions(conn, kind).await
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
            Arc::new(FeedEventVersionStage1Ingest::new(Self::KIND, "team")),
            Arc::new(Stage2Ingest::new(Self::KIND, TeamFeedIngestFromVersions)),
        ]
    }
}

pub fn chron_team_feed_as_new<'a>(
    team_id: &'a str,
    valid_from: DateTime<Utc>,
    item: &'a FeedItemContainer,
) -> (
    NewFeedEventProcessed<'a>,
    Option<NewTeamGamePlayed<'a>>,
    Vec<NewVersionIngestLog<'a>>,
) {
    let mut ingest_logs = VersionIngestLogs::new(TeamFeedIngest::KIND, team_id, valid_from);

    let processed = NewFeedEventProcessed {
        kind: "team_feed",
        entity_id: team_id,
        feed_event_index: item.feed_event_index,
        valid_from: valid_from.naive_utc(),
    };

    if IGNORE_EVENTS_STARTING <= valid_from && valid_from <= IGNORE_EVENTS_ENDING {
        // See the corresponding statement in ingest_player_feed for an explanation
        // TODO Apply the same TODOs from ingest_player_feed
        ingest_logs.info("Ignoring event version from the Feed Inversion Event");

        return (processed, None, ingest_logs.into_vec());
    }

    if let Some(prev_event) = &item.prev_data {
        if item.prev_valid_from.is_none() {
            ingest_logs.warn(format!(
                "Team {} feed event index {} had a previous event, but \
                did not have prev_valid_from",
                team_id,
                item.feed_event_index,
            ));
        }

        if item.prev_valid_from.is_none_or(|prev_valid_from| IGNORE_EVENTS_STARTING <= prev_valid_from && prev_valid_from <= IGNORE_EVENTS_ENDING) {
            if item.prev_valid_from.is_none() {
                ingest_logs.warn(format!(
                    "Can't check whether team {} feed event index {}'s previous event \
                    was from the Feed Inversion Event because it's missing prev_valid_from. \
                    Assuming it was to avoid losing data.",
                    team_id,
                    item.feed_event_index,
                ));
            } else {
                ingest_logs.info(format!(
                    "Team {} feed event index {} had a previous event, but it was from \
                    the Feed Inversion Event so we can proceed with processing this event",
                    team_id,
                    item.feed_event_index,
                ));
            }
        } else if item.data.timestamp == prev_event.timestamp && item.data.text == prev_event.text {
            // I'm not early-exiting here because we don't check all the
            // fields, so this could incorrectly match an actually meaningful
            // change. If that happens, the database layer checks will find it.
            ingest_logs.info(format!(
                "Team {} feed event index {} had a previous event, but it's identical \
                in text and timestamp to this event. Assuming it's just a data format change\
                and that the database will deduplicate it.",
                team_id,
                item.feed_event_index,
            ));
        } else {
            ingest_logs.error(format!(
                "Team {} feed event index {} had a previous version without special \
                handling. Skipping this version.\n\
                previous version text: {}\n\
                previous version valid_from: {}\n\
                this version text: {}\n\
                this version valid_from: {}",
                team_id,
                item.feed_event_index,
                prev_event.text,
                if let Some(dt) = item.prev_valid_from { format!("{dt}") } else { "(missing)".to_string() },
                item.data.text,
                valid_from,
            ));

            return (processed, None, ingest_logs.into_vec());
        }
    }

    // There is a bug in mmolb_parsing that causes a panic when an
    // augment's text is empty
    if item.data.text.is_empty() {
        return (processed, None, ingest_logs.into_vec());
    }

    let parsed_event = mmolb_parsing::team_feed::parse_team_feed_event(&item.data);

    let is_game_result = if let ParsedTeamFeedEventText::GameResult { .. } = &parsed_event { true } else { false };

    let game_outcome = match parsed_event {
        ParsedTeamFeedEventText::ParseError { error, text } => {
            // I'm making this a warning because we don't care about most event types
            // (and we can handle having a game for which we don't know the end time)
            ingest_logs.warn(format!("Error parsing \"{text}\": {error}"));
            None
        }
        // Get game
        ParsedTeamFeedEventText::GameResult { .. } |
        ParsedTeamFeedEventText::Shipment { .. } |
        ParsedTeamFeedEventText::PhotoContest { .. } |
        ParsedTeamFeedEventText::SpecialDelivery { .. } |
        ParsedTeamFeedEventText::ClaimedLinealBelt { .. } |
        ParsedTeamFeedEventText::LostLinealBelt { .. } |
        ParsedTeamFeedEventText::PlayerReflected { .. } |
        ParsedTeamFeedEventText::SimulacrumPayout { .. } => {
            let game_link = item.data.links
                .iter()
                .filter(|link| link.link_type == Ok(LinkType::Game))
                .exactly_one();

            match game_link {
                Ok(game_link) => {
                    Some(NewTeamGamePlayed {
                        mmolb_team_id: team_id,
                        feed_event_index: item.feed_event_index,
                        time: item.data.timestamp.naive_utc(),
                        mmolb_game_id: &game_link.id,
                    })
                }
                Err(err) => {
                    let msg = format!(
                        "Game outcome in {} feed index {} had {} game links (expected 1)",
                        team_id, item.feed_event_index, err.count()
                    );
                    if is_game_result {
                        ingest_logs.warn(msg);
                    } else {
                        ingest_logs.info(msg);
                    }
                    None
                }
            }
        }
        // Delivery is an end-of-game event but didn't have game links
        ParsedTeamFeedEventText::Delivery { .. } |
        ParsedTeamFeedEventText::Party { .. } |
        ParsedTeamFeedEventText::DoorPrize { .. } |
        ParsedTeamFeedEventText::Prosperous { .. } |
        ParsedTeamFeedEventText::DonatedToLottery { .. } |
        ParsedTeamFeedEventText::WonLottery { .. } |
        ParsedTeamFeedEventText::Enchantment { .. } |
        ParsedTeamFeedEventText::AttributeChanges { .. } |
        ParsedTeamFeedEventText::MassAttributeEquals { .. } |
        ParsedTeamFeedEventText::TakeTheMound { .. } |
        ParsedTeamFeedEventText::TakeThePlate { .. } |
        ParsedTeamFeedEventText::SwapPlaces { .. } |
        ParsedTeamFeedEventText::Recomposed { .. } |
        ParsedTeamFeedEventText::Modification { .. } |
        ParsedTeamFeedEventText::FallingStarOutcome { .. } |
        ParsedTeamFeedEventText::CorruptedByWither { .. } |
        ParsedTeamFeedEventText::Purified { .. } |
        ParsedTeamFeedEventText::NameChanged |
        ParsedTeamFeedEventText::PlayerMoved { .. } |
        ParsedTeamFeedEventText::PlayerRelegated { .. } |
        ParsedTeamFeedEventText::PlayerPositionsSwapped { .. } |
        ParsedTeamFeedEventText::PlayerContained { .. } |
        ParsedTeamFeedEventText::PlayerGrow { .. } |
        ParsedTeamFeedEventText::Callup { .. } |
        ParsedTeamFeedEventText::GreaterAugment { .. } |
        ParsedTeamFeedEventText::Released { .. } |
        ParsedTeamFeedEventText::Retirement { .. } |
        ParsedTeamFeedEventText::PlayerGrewInEfflorescence { .. } |
        ParsedTeamFeedEventText::PlayerEffloresce { .. } |
        ParsedTeamFeedEventText::DeliveryDiscarded { .. } |
        ParsedTeamFeedEventText::ConsumptionContestToPlayer { .. } |
        ParsedTeamFeedEventText::ConsumptionContestToTeam { .. } |
        ParsedTeamFeedEventText::PlayersSwapped { .. }  |
        ParsedTeamFeedEventText::PlayersPurified { .. } |
        ParsedTeamFeedEventText::ElectionAppliedLevelUps { .. } |
        ParsedTeamFeedEventText::Restyle { .. } |
        ParsedTeamFeedEventText::Augment { .. } |
        ParsedTeamFeedEventText::BulkImmunized { .. } => None,
    };

    (processed, game_outcome, ingest_logs.into_vec())
}
