// TODO Rename this file ingest_player_feed

use rayon::iter::ParallelIterator;
use std::fmt::{Display, Formatter};
use chrono::{DateTime, NaiveDateTime, Utc};
use hashbrown::{HashSet};
use itertools::Itertools;
use log::{debug, error, info, warn};
use mmolb_parsing::enums::{Attribute, Day};
use mmolb_parsing::feed_event::{FeedEvent, ParsedFeedEventText};
use rayon::iter::IntoParallelIterator;
use serde::Deserialize;
use mmoldb_db::{db, PgConnection};
use tokio_util::sync::CancellationToken;
use chron::ChronEntity;
use mmoldb_db::models::{NewPlayerAugment, NewPlayerFeedVersion, NewPlayerParadigmShift, NewPlayerRecomposition};
use mmoldb_db::taxa::Taxa;
use crate::ingest::{batch_by_entity, IngestFatalError};
use crate::ingest_players::day_to_db;

// I made this a constant because I'm constant-ly terrified of typoing
// it and introducing a difficult-to-find bug
const PLAYER_FEED_KIND: &'static str = "player_feed";
const CHRON_FETCH_PAGE_SIZE: usize = 1000;
const RAW_PLAYER_FEED_INSERT_BATCH_SIZE: usize = 1000;
const PROCESS_PLAYER_FEED_BATCH_SIZE: usize = 100000;

pub async fn ingest_player_feeds(pg_url: String, abort: CancellationToken) -> miette::Result<()> {
    crate::ingest::ingest(
        PLAYER_FEED_KIND,
        CHRON_FETCH_PAGE_SIZE,
        RAW_PLAYER_FEED_INSERT_BATCH_SIZE,
        PROCESS_PLAYER_FEED_BATCH_SIZE,
        pg_url,
        abort,
        db::get_player_feed_ingest_start_cursor,
        ingest_page_of_player_feeds,
    ).await
}

pub fn ingest_page_of_player_feeds(
    taxa: &Taxa,
    raw_player_feeds: Vec<ChronEntity<serde_json::Value>>,
    conn: &mut PgConnection,
    worker_id: usize,
) -> Result<(), IngestFatalError> {
    debug!(
        "Starting of {} player feeds on worker {worker_id}",
        raw_player_feeds.len()
    );
    let save_start = Utc::now();

    #[derive(Deserialize)]
    struct FeedContainer {
        feed: Vec<FeedEvent>,
    }

    let deserialize_start = Utc::now();
    // TODO Gracefully handle player feed deserialize failure
    let player_feeds = raw_player_feeds
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
        "Deserialized page of {} player feeds in {:.2} seconds on worker {}",
        player_feeds.len(), deserialize_duration, worker_id
    );

    let latest_time = player_feeds
        .last()
        .map(|version| version.valid_from)
        .unwrap_or(Utc::now());
    let time_ago = latest_time.signed_duration_since(Utc::now());
    let human_time_ago = chrono_humanize::HumanTime::from(time_ago);

    // Convert to Insertable type
    let new_player_feeds = player_feeds
        .iter()
        .map(|v| chron_player_feed_as_new(&taxa, &v.entity_id, v.valid_from, &v.data.feed, None))
        .collect_vec();

    // The handling of valid_until is entirely in the database layer, but its logic
    // requires that a given batch of players does not have the same player twice. We
    // provide that guarantee here.
    let new_player_feeds_len = new_player_feeds.len();
    for batch in batch_by_entity(new_player_feeds, |v| v.0.mmolb_player_id) {
        let to_insert = batch.len();
        info!(
            "Sent {} new player feed versions out of {} to the database.",
            to_insert,
            new_player_feeds_len,
        );

        let inserted = db::insert_player_feed_versions(conn, batch)?;

        info!(
            "Sent {} new player feed versions out of {} to the database. \
            {inserted} versions were actually inserted, the rest were duplicates. \
            Currently processing player feeds from {human_time_ago}.",
            to_insert,
            player_feeds.len(),
        );
    }

    let save_duration = (Utc::now() - save_start).as_seconds_f64();

    info!(
        "Ingested page of {} player feeds in {save_duration:.3} seconds.",
        player_feeds.len(),
    );

    Ok(())
}


fn process_paradigm_shift<'e>(
    changing_attribute: Attribute,
    value_attribute: Attribute,
    paradigm_shifts: &mut Vec<NewPlayerParadigmShift<'e>>,
    feed_event_index: i32,
    event: &FeedEvent,
    time: NaiveDateTime,
    player_id: &'e str,
    taxa: &Taxa,
) {
    if changing_attribute == Attribute::Priority {
        let (day_type, day, superstar_day) = day_to_db(&event.day, taxa);

        paradigm_shifts.push(NewPlayerParadigmShift {
            mmolb_player_id: player_id,
            feed_event_index,
            time,
            season: event.season as i32,
            day_type,
            day,
            superstar_day,
            attribute: taxa.attribute_id(value_attribute.into()),
        })
    } else {
        // TODO Expose player ingest_games errors on the site
        error!(
            "Encountered a SingleAttributeEquals feed event that changes an \
            attribute other than priority. Player {} feed event {} changes {}",
            player_id, feed_event_index, changing_attribute,
        );
    }
}

pub fn chron_player_feed_as_new<'a>(
    taxa: &Taxa,
    player_id: &'a str,
    valid_from: DateTime<Utc>,
    feed_items: &'a [FeedEvent],
    final_player_name: Option<&str>,
) -> (
    NewPlayerFeedVersion<'a>,
    Vec<NewPlayerAugment<'a>>,
    Vec<NewPlayerParadigmShift<'a>>,
    Vec<NewPlayerRecomposition<'a>>,
) {
    // TODO make this static, or at least global
    // Some feed events were accidentally reverted (by caching issues I think), and we want to
    // pretend they never happened
    let impermanent_feed_events = {
        let mut hashes = HashSet::new();
        #[rustfmt::skip]
        hashes.extend(vec![
            ("6805db0cac48194de3cd40dd", DateTime::parse_from_rfc3339("2025-07-14T12:32:16.183651+00:00").unwrap().naive_utc()),
            ("6840fa75ed58166c1895a7f3", DateTime::parse_from_rfc3339("2025-07-14T12:58:25.172157+00:00").unwrap().naive_utc()),
            ("6840fb13e63d9bb8728896d2", DateTime::parse_from_rfc3339("2025-07-14T11:56:08.156319+00:00").unwrap().naive_utc()),
            ("6840fe6508b7fc5e21e8a940", DateTime::parse_from_rfc3339("2025-07-14T15:04:09.335705+00:00").unwrap().naive_utc()),
            ("6841000988056169e0078792", DateTime::parse_from_rfc3339("2025-07-14T11:58:34.656639+00:00").unwrap().naive_utc()),
            ("684102aaf7b5d3bf791d67e8", DateTime::parse_from_rfc3339("2025-07-14T12:54:23.274555+00:00").unwrap().naive_utc()),
            ("684102dfec9dc637cfd0cad6", DateTime::parse_from_rfc3339("2025-07-14T12:01:51.299731+00:00").unwrap().naive_utc()),
            ("68412d1eed58166c1895ae66", DateTime::parse_from_rfc3339("2025-07-14T12:20:36.597014+00:00").unwrap().naive_utc()),
            ("68418c52554d8039701f1c93", DateTime::parse_from_rfc3339("2025-07-14T12:09:42.764129+00:00").unwrap().naive_utc()),
            ("6846cc4d4a488309816674ff", DateTime::parse_from_rfc3339("2025-07-14T12:10:19.571083+00:00").unwrap().naive_utc()),
            ("684727f5bb00de6f9bb79973", DateTime::parse_from_rfc3339("2025-07-14T12:14:14.710296+00:00").unwrap().naive_utc()),
            ("68505bc5341f7f2421020d05", DateTime::parse_from_rfc3339("2025-07-14T12:13:58.129103+00:00").unwrap().naive_utc()),
            ("6855b350f1d8f657407b231c", DateTime::parse_from_rfc3339("2025-07-14T15:02:55.607448+00:00").unwrap().naive_utc()),
            ("68564374acfab5652c3a6c44", DateTime::parse_from_rfc3339("2025-07-15T01:02:41.992667+00:00").unwrap().naive_utc()),
            ("685b740338c6569da104aa48", DateTime::parse_from_rfc3339("2025-07-14T12:47:59.895679+00:00").unwrap().naive_utc()),
            ("686355f4b254dfbaab3014b0", DateTime::parse_from_rfc3339("2025-07-14T11:51:34.711389+00:00").unwrap().naive_utc()),
            ("68655942f27aa83a88fa64e0", DateTime::parse_from_rfc3339("2025-07-14T10:36:47.308576+00:00").unwrap().naive_utc()),
        ]);
        hashes
    };

    struct PlayerFullName<'a>(Option<&'a str>);
    impl<'a> PlayerFullName<'a> {
        pub fn check_or_set_name(&mut self, name: &'a str) {
            match &self.0 {
                None => {
                    info!("Setting previously-unknown player name to {name}");
                    self.0 = Some(name);
                }
                Some(known_name) => {
                    // Multiple Stanleys Demir were generated during the s2
                    // Falling Stars event and then Danny manually renamed them
                    if name != *known_name && name != "Stanley Demir" {
                        warn!(
                            "Player name from augment '{}' doesn't match this feed's known player \
                            name '{}'",
                            name, known_name,
                        );
                    }
                }
            }
        }

        pub fn set_name(&mut self, new_name: &'a str) {
            self.0 = Some(new_name);
        }
    }

    impl Display for PlayerFullName<'_> {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            match self.0 {
                None => { write!(f, "unknown player name") }
                Some(name) => { write!(f, "'{name}'") }
            }
        }
    }

    let mut player_full_name = PlayerFullName(final_player_name);

    let player_feed_version = NewPlayerFeedVersion {
        mmolb_player_id: player_id,
        valid_from: valid_from.naive_utc(),
        valid_until: None,
        num_entries: feed_items.len() as i32,
    };

    // Current plan is to regenerate all feed-dependent tables every
    // time a player is ingested, and use a database function to handle
    // the many duplicates that will create.
    let mut augments = Vec::new();
    let mut paradigm_shifts = Vec::new();
    let mut recompositions = Vec::new();

    for (index, event) in feed_items.iter().enumerate() {
        let feed_event_index = index as i32;
        let time = event.timestamp.naive_utc();

        if impermanent_feed_events.contains(&(player_id, time)) {
            info!(
                "Skipping feed event \"{}\" because it was reverted later",
                event.text,
            );
            if index + 1 != feed_items.len() {
                warn!(
                    "This non-permanent event is not the last event in the feed ({} of {}).",
                    index + 1,
                    feed_items.len(),
                );
            }
            continue;
        }

        match mmolb_parsing::feed_event::parse_feed_event(event) {
            ParsedFeedEventText::ParseError { error, text } => {
                // TODO Expose player ingest_games errors on the site
                error!(
                    "Error {error} parsing {text} from {} ({})'s feed",
                    player_full_name, player_id,
                );
            }
            ParsedFeedEventText::GameResult { .. } => {
                // We don't (yet) have a use for this event
            }
            ParsedFeedEventText::Delivery { .. } => {
                // We don't (yet) use this event, but feed events have a timestamp so it
                // could be used to backdate when players got their item. Although it
                // doesn't really matter because player items can't (yet) change during
                // a game, so we can backdate any player items to the beginning of any
                // game they were observed doing. Also this doesn't apply to item changes
                // that team owners make using the inventory, so there's not much point.
            }
            ParsedFeedEventText::Shipment { .. } => {
                // See comment on Delivery
            }
            ParsedFeedEventText::SpecialDelivery { .. } => {
                // See comment on Delivery
            }
            ParsedFeedEventText::AttributeChanges { changes } => {
                let (day_type, day, superstar_day) = day_to_db(&event.day, taxa);

                for change in changes {
                    player_full_name.check_or_set_name(&change.player_name);
                    augments.push(NewPlayerAugment {
                        mmolb_player_id: player_id,
                        feed_event_index,
                        time,
                        season: event.season as i32,
                        day_type,
                        day,
                        superstar_day,
                        attribute: taxa.attribute_id(change.attribute.into()),
                        value: change.amount as i32,
                    })
                }
            }
            ParsedFeedEventText::SingleAttributeEquals {
                player_name,
                changing_attribute,
                value_attribute,
            } => {
                player_full_name.check_or_set_name(&player_name);
                // The handling of a non-priority SingleAttributeEquals will have to
                // be so different that it's not worth trying to implement before it
                // actually appears
                process_paradigm_shift(
                    changing_attribute,
                    value_attribute,
                    &mut paradigm_shifts,
                    feed_event_index,
                    event,
                    time,
                    player_id,
                    taxa,
                )
            }
            ParsedFeedEventText::MassAttributeEquals {
                value_attribute,
                changing_attribute,
                players,
            } => {
                if players.is_empty() {
                    // TODO Expose player ingest_games warnings on the site
                    warn!("MassAttributeEquals had 0 players");
                } else if let Some(((_, player_name),)) = players.iter().collect_tuple() {
                    player_full_name.check_or_set_name(player_name);
                    process_paradigm_shift(
                        changing_attribute,
                        value_attribute,
                        &mut paradigm_shifts,
                        feed_event_index,
                        event,
                        time,
                        player_id,
                        taxa,
                    );
                } else {
                    // TODO Expose player ingest_games warnings on the site
                    warn!("MassAttributeEquals on players shouldn't have more than one player");
                }
            }
            ParsedFeedEventText::S1Enchantment { .. } => {
                // This only affects items. Rationale is the same as Delivery.
            }
            ParsedFeedEventText::S2Enchantment { .. } => {
                // This only affects items. Rationale is the same as Delivery.
            }
            ParsedFeedEventText::TakeTheMound { .. } => {
                // We can use this to backdate certain position changes, but
                // the utility of doing that is limited for the same reason
                // as the utility of processing Delivery is limited.
            }
            ParsedFeedEventText::TakeThePlate { .. } => {
                // See comment on TakeTheMound
            }
            ParsedFeedEventText::SwapPlaces { .. } => {
                // See comment on TakeTheMound
            }
            ParsedFeedEventText::InfusedByFallingStar { .. } => {
                // We can use this to backdate certain mod changes, but
                // the utility of doing that is limited for the same reason
                // as the utility of processing Delivery is limited.
            }
            ParsedFeedEventText::InjuredByFallingStar { .. } => {
                // We don't (yet) have a use for this event
            }
            ParsedFeedEventText::Prosperous { .. } => {
                // This is entirely redundant with the information available
                // when parsing games. I suppose it might be used for
                // synchronization, but we have no need of that yet.
            }
            ParsedFeedEventText::Recomposed { new, previous } => {
                let (day_type, day, superstar_day) = day_to_db(&event.day, taxa);

                player_full_name.check_or_set_name(previous);
                player_full_name.set_name(new);
                recompositions.push(NewPlayerRecomposition {
                    mmolb_player_id: player_id,
                    feed_event_index,
                    time,
                    season: event.season as i32,
                    day_type,
                    day,
                    superstar_day,
                    player_name_before: previous,
                    player_name_after: new,
                });
            }
            ParsedFeedEventText::Modification { .. } => {
                // See comment on HitByFallingStar
            }
            ParsedFeedEventText::Retirement { .. } => {
                // This seems to include retirements of other players. Regardless,
                // we don't need it because after this the player's ID is no longer
                // used.
            }
            ParsedFeedEventText::Released { .. } => {
                // There shouldn't be anything to do about this. Unlike recomposition,
                // this player's ID is retired instead of being repurposed for the
                // new player.

                // There was a bug at the start of s3 where released players weren't actually
                // released.
                if index + 1 != feed_items.len()
                    && (event.season, &event.day) != (3, &Ok(Day::Day(1)))
                {
                    // TODO Expose player ingest_games warnings on the site
                    warn!(
                        "Released event wasn't the last event in the player's feed. {}/{}",
                        index + 1,
                        feed_items.len(),
                    );
                }
            }
        }
    }

    if let Some(name) = final_player_name {
        player_full_name.check_or_set_name(name);
    }

    (
        player_feed_version,
        augments,
        paradigm_shifts,
        recompositions,
    )
}