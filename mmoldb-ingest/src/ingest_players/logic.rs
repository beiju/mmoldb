use hashbrown::{HashMap, HashSet, hash_map::Entry};
use chrono::{DateTime, NaiveDateTime, ParseResult, Utc};
use futures::StreamExt;
use itertools::Itertools;
use log::{debug, error, info, warn};
use miette::Diagnostic;
use mmolb_parsing::enums::{Attribute, Day, FeedEventType, Handedness, Position};
use mmolb_parsing::feed_event::ParsedFeedEventText;
use mmolb_parsing::NotRecognized;
use mmolb_parsing::player::TalkCategory;
use thiserror::Error;
use time::error::Format;
use chron::ChronEntity;
use mmoldb_db::{db, Connection, PgConnection, QueryError, QueryResult};
use mmoldb_db::db::NameEmojiTooltip;
use mmoldb_db::models::{NewPlayerAugment, NewPlayerModificationVersion, NewPlayerParadigmShift, NewPlayerRecomposition, NewPlayerReport, NewPlayerVersion};
use mmoldb_db::taxa::{Taxa, TaxaDayType, TaxaSlot};
use rayon::prelude::*;

#[derive(Debug, Error, Diagnostic)]
pub enum IngestFatalError {
    #[error(transparent)]
    DeserializeError(#[from] serde_json::Error),

    #[error(transparent)]
    DbError(#[from] QueryError),
}

pub fn ingest_page_of_players(
    taxa: &Taxa,
    ingest_id: i64,
    page_index: usize,
    get_batch_to_process_duration: f64,
    raw_players: Vec<ChronEntity<serde_json::Value>>,
    conn: &mut PgConnection,
    worker_id: usize,
) -> Result<(), IngestFatalError> {
    debug!(
        "Starting ingest page of {} players on worker {worker_id}",
        raw_players.len()
    );
    let save_start = Utc::now();

    let deserialize_start = Utc::now();
    // TODO Gracefully handle player deserialize failure
    let players = raw_players
        .into_par_iter()
        .map(|game_json| {
            Ok::<ChronEntity<mmolb_parsing::player::Player>, serde_json::Error>(ChronEntity {
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
        "Deserialized page of {} players in {deserialize_duration:.2} seconds on worker {worker_id}",
        players.len()
    );

    let latest_time = players.last()
        .map(|version| version.valid_from)
        .unwrap_or(Utc::now());
    let time_ago = latest_time.signed_duration_since(Utc::now());
    let human_time_ago = chrono_humanize::HumanTime::from(time_ago);

    // Collect all modifications that appear in this batch so we can ensure they're all added
    let unique_modifications = players.iter()
        .flat_map(|version| {
            version.data.modifications.iter()
                .chain(version.data.lesser_boon.as_ref())
                .chain(version.data.greater_boon.as_ref())
                .map(|m| {
                    if !m.extra_fields.is_empty() {
                        warn!("Modification had extra fields that were not captured: {:?}", m.extra_fields);
                    }
                    (m.name.as_str(), m.emoji.as_str(), m.description.as_str())
                })
        })
        .unique()
        .collect_vec();

    let modifications = get_filled_modifications_map(conn, &unique_modifications)?;

    // Convert to Insertable type
    let mut new_players = players.iter()
        .map(|v| chron_player_as_new(&v, &taxa, &modifications))
        .collect_vec();

    // The handling of valid_until is entirely in the database layer, but its logic
    // requires that a given batch of players does not have the same player twice. We
    // provide that guarantee here.
    let mut this_batch = HashMap::new();
    while !new_players.is_empty() {
        // Pull out all players who don't yet appear
        let remaining_versions = new_players.into_iter()
            .flat_map(|version| {
                match this_batch.entry(version.0.mmolb_player_id) {
                    Entry::Occupied(_) => {
                        // Then retain this version for the next sub-batch
                        Some(version)
                    }
                    Entry::Vacant(entry) => {
                        // Then insert this version into the map and don't retain it
                        entry.insert(version);
                        None
                    }
                }
            })
            .collect_vec();

        let players_to_update = this_batch.into_iter()
            .map(|(_, version)| version)
            .collect_vec();

        let to_insert = players_to_update.len();
        info!(
            "Sent {} new player versions out of {} to the database. {} left of this batch.",
            to_insert, players.len(), remaining_versions.len(),
        );
        // let results = conn.transaction(|conn| {
        //     Ok::<_, QueryError>(db::insert_player_versions_with_retry(conn, &players_to_update))
        // })?;
        let results = db::insert_player_versions_with_retry(conn, &players_to_update);

        for (result, player) in results.iter().zip(&players_to_update) {
            if let Err(e) = result {
                let (version, _modification_version, _augment, _paradigm_shift, _recomposition, _report): &(NewPlayerVersion, _, _, _, _, _) = player;
                for recomp in _recomposition {
                    println!("Recomp {} {} {}", recomp.mmolb_player_id, recomp.feed_event_index, recomp.time);
                }
                error!(
                    "Error {e} ingesting player {} at {}",
                    version.mmolb_player_id, version.valid_from,
                );
            }
        }

        let inserted = results.into_iter().collect::<Result<Vec<_>, _>>()?.len();

        info!(
            "Sent {} new player versions out of {} to the database. {} left of this batch. \
            {inserted} versions were actually inserted, the rest were duplicates. \
            Currently processing players from {human_time_ago}.",
            to_insert, players.len(), remaining_versions.len(),
        );

        new_players = remaining_versions;
        this_batch = HashMap::new();
    }

    let save_duration = (Utc::now() - save_start).as_seconds_f64();

    info!(
        "Ingested page of {} players in {save_duration:.3} seconds. \
        Fetching them took {get_batch_to_process_duration:.3} seconds.",
        players.len(),
    );

    Ok(())
}

pub fn get_filled_modifications_map(
    conn: &mut PgConnection,
    modifications_to_ensure: &[(&str, &str, &str)],
) -> QueryResult<HashMap<NameEmojiTooltip, i64>> {
    // Put everything in a loop to handle insert conflicts with other
    // ingest threads
    Ok(loop {
        let mut modifications = db::get_modifications_table(conn)?;

        let modifications_to_add = modifications_to_ensure.iter()
            .filter(|key| !modifications.contains_key(*key))
            .collect_vec();

        if modifications_to_add.is_empty() {
            break modifications;
        }

        match db::insert_modifications(conn, modifications_to_add.as_slice())? {
            None => {
                // Indicates that we should try again
                warn!("Conflict inserting modifications; trying again");
                continue;
            }
            Some(new_values) => {
                modifications.extend(new_values);

                // For debugging only; remove once we're sure it works
                for m in modifications_to_ensure {
                    assert!(modifications.contains_key(m));
                }

                break modifications;
            }
        }

    })
}


fn day_to_db(day: &Result<Day, NotRecognized>, taxa: &Taxa) -> (Option<i64>, Option<i32>, Option<i32>) {
    match day {
        Ok(Day::Preseason) => {
            (Some(taxa.day_type_id(TaxaDayType::Preseason)), None, None)
        }
        Ok(Day::SuperstarBreak) => {
            (Some(taxa.day_type_id(TaxaDayType::SuperstarBreak)), None, None)
        }
        Ok(Day::PostseasonPreview) => {
            (Some(taxa.day_type_id(TaxaDayType::PostseasonPreview)), None, None)
        }
        Ok(Day::PostseasonRound(1)) => {
            (Some(taxa.day_type_id(TaxaDayType::PostseasonRound1)), None, None)
        }
        Ok(Day::PostseasonRound(2)) => {
            (Some(taxa.day_type_id(TaxaDayType::PostseasonRound2)), None, None)
        }
        Ok(Day::PostseasonRound(3)) => {
            (Some(taxa.day_type_id(TaxaDayType::PostseasonRound3)), None, None)
        }
        Ok(Day::PostseasonRound(other)) => {
            error!("Unexpected postseason day {other} (expected 1-3)");
            (None, None, None)
        }
        Ok(Day::Election) => {
            (Some(taxa.day_type_id(TaxaDayType::Election)), None, None)
        }
        Ok(Day::Holiday) => {
            (Some(taxa.day_type_id(TaxaDayType::Holiday)), None, None)
        }
        Ok(Day::Day(day)) => {
            (Some(taxa.day_type_id(TaxaDayType::RegularDay)), Some(*day as i32), None)
        }
        Ok(Day::SuperstarGame) => {
            (Some(taxa.day_type_id(TaxaDayType::SuperstarDay)), None, None)
        }
        Ok(Day::SuperstarDay(day)) => {
            (Some(taxa.day_type_id(TaxaDayType::SuperstarDay)), None, Some(*day as i32))
        }
        Ok(Day::Event) => {
            (Some(taxa.day_type_id(TaxaDayType::Event)), None, None)
        }
        Ok(Day::SpecialEvent) => {
            (Some(taxa.day_type_id(TaxaDayType::SpecialEvent)), None, None)
        }
        Err(err) => {
            error!("Unrecognized day {err}");
            (None, None, None)
        }
    }
}
fn chron_player_as_new<'a>(
    entity: &'a ChronEntity<mmolb_parsing::player::Player>, 
    taxa: &Taxa, 
    modifications: &HashMap<NameEmojiTooltip, i64>,
) -> (
    NewPlayerVersion<'a>,
    Vec<NewPlayerModificationVersion<'a>>,
    Vec<NewPlayerAugment<'a>>,
    Vec<NewPlayerParadigmShift<'a>>,
    Vec<NewPlayerRecomposition<'a>>,
    Vec<NewPlayerReport<'a>>,
) {
    // TODO make this static, or at least global
    // Some feed events were accidentally reverted (by caching issues I think), and we want to
    // pretend they never happened
    let impermanent_feed_events = {
        let mut hashes = HashSet::new();
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

    let (birthday_type, birthday_day, birthday_superstar_day) = day_to_db(&entity.data.birthday, taxa);

    let get_modification_id = |modification: &mmolb_parsing::player::Modification| {
        *modifications.get(&(modification.name.as_str(), modification.emoji.as_str(), modification.description.as_str()))
            .expect("All modifications should have been added to the modifications table")
    };

    let get_handedness_id = |handedness: &Result<Handedness, NotRecognized>| {
        match handedness {
            Ok(handedness) => Some(taxa.handedness_id((*handedness).into())),
            Err(err) => {
                error!("Player had unexpected batting handedness {err}");
                None
            }
        }
    };

    let modifications = entity.data.modifications.iter()
        .enumerate()
        .map(|(i, m)| NewPlayerModificationVersion {
            mmolb_player_id: &entity.entity_id,
            valid_from: entity.valid_from.naive_utc(),
            valid_until: None,
            modification_order: i as i32,
            modification_id: get_modification_id(m),
        })
        .collect_vec();

    let slot = match &entity.data.position {
        Ok(position) => {
            Some(taxa.slot_id(match position {
                Position::Pitcher => { TaxaSlot::Pitcher }
                Position::Catcher => { TaxaSlot::Catcher }
                Position::FirstBaseman => { TaxaSlot::FirstBase }
                Position::SecondBaseman => { TaxaSlot::SecondBase }
                Position::ThirdBaseman => { TaxaSlot::ThirdBase }
                Position::ShortStop => { TaxaSlot::Shortstop }
                Position::LeftField => { TaxaSlot::LeftField }
                Position::CenterField => { TaxaSlot::CenterField }
                Position::RightField => { TaxaSlot::RightField }
                Position::StartingPitcher => { TaxaSlot::StartingPitcher }
                Position::ReliefPitcher => { TaxaSlot::ReliefPitcher }
                Position::Closer => { TaxaSlot::Closer }
            }))
        }
        Err(err) => {
            error!("Player position not recognized: {err}");
            None
        }
    };

    let player = NewPlayerVersion {
        mmolb_player_id: &entity.entity_id,
        valid_from: entity.valid_from.naive_utc(),
        valid_until: None,
        first_name: &entity.data.first_name,
        last_name: &entity.data.last_name,
        batting_handedness: get_handedness_id(&entity.data.bats),
        pitching_handedness: get_handedness_id(&entity.data.throws),
        home: &entity.data.home,
        birthseason: entity.data.birthseason as i32,
        birthday_type,
        birthday_day,
        birthday_superstar_day,
        likes: &entity.data.likes,
        dislikes: &entity.data.dislikes,
        number: entity.data.number as i32,
        mmolb_team_id: entity.data.team_id.as_deref(),
        slot,
        durability: entity.data.durability,
        greater_boon: entity.data.greater_boon.as_ref().map(get_modification_id),
        lesser_boon: entity.data.lesser_boon.as_ref().map(get_modification_id),
    };

    struct PlayerFullName(String);
    impl PlayerFullName {
        pub fn check_name(&self, name: &str) {
            // Multiple Stanleys Demir were generated during the s2
            // Falling Stars event and then Danny manually renamed them
            if name != &self.0 && name != "Stanley Demir" {
                warn!(
                    "Player name from augment {} doesn't match player name from player object {}",
                    name, self.0,
                );
            }
        }

        pub fn set_name(&mut self, new_name: String) {
            self.0 = new_name;
        }
    }

    let mut player_full_name = PlayerFullName(format!("{} {}", player.first_name, player.last_name));

    // Current plan is to regenerate all feed-dependent tables every
    // time a player is ingested, and use a database function to handle
    // the many duplicates that will create.
    let mut augments = Vec::new();
    let mut paradigm_shifts = Vec::new();
    let mut recompositions = Vec::new();
    let mut reports = Vec::new();

    if let Ok(feed) = &entity.data.feed {
        // We process feed events in reverse so that we can keep the player's
        // expected name updated. Since we're starting at the player's current
        // state, we only know that the player's name has changed if we scan
        // backwards to find the event that changed it.
        // It is important that enumerate() be before rev() so the feed event
        // indices are correct.
        for (index, event) in feed.iter().enumerate().rev() {
            let feed_event_index = index as i32;
            // Wow this time stuff sucks
            let ts = match event.timestamp.format(&time::format_description::well_known::Rfc3339) {
                Ok(val) => val,
                Err(err) => {
                    // TODO Expose player ingest errors on the site
                    error!(
                        "Player {}'s {}th feed event `ts` failed to format as a date: {}",
                        entity.entity_id, feed_event_index, err,
                    );
                    // The behavior I've decided on for errors in feed event parsing is
                    // to skip the feed event
                    continue;
                }
            };
            let time = match DateTime::parse_from_rfc3339(&ts) {
                Ok(time_with_offset) => time_with_offset.naive_utc(),
                Err(err) => {
                    // TODO Expose player ingest errors on the site
                    error!(
                        "Player {}'s {}th feed event `ts` failed to parse as a date: {}",
                        entity.entity_id, feed_event_index, err,
                    );
                    // The behavior I've decided on for errors in feed event parsing is
                    // to skip the feed event
                    continue;
                }
            };

            if impermanent_feed_events.contains(&(entity.entity_id.as_str(), time)) {
                info!(
                    "Skipping feed event \"{}\" because it was reverted later",
                    event.text,
                );
                if index + 1 != feed.len() {
                    warn!(
                        "This non-permanent event is not the last event in the feed ({} of {}).",
                        index + 1, feed.len(),
                    );
                }
                continue;
            }

            match mmolb_parsing::feed_event::parse_feed_event(event) {
                ParsedFeedEventText::ParseError { error, text } => {
                    // TODO Expose player ingest errors on the site
                    error!(
                        "Error {error} parsing {text} from {} {} ({})'s feed",
                        entity.data.first_name, entity.data.last_name, entity.entity_id,
                    );
                }
                ParsedFeedEventText::GameResult { .. } => {
                    // We don't (yet) have a use for this event
                },
                ParsedFeedEventText::Delivery { .. } => {
                    // We don't (yet) use this event, but feed events have a timestamp so it
                    // could be used to backdate when players got their item. Although it
                    // doesn't really matter because player items can't (yet) change during
                    // a game, so we can backdate any player items to the beginning of any
                    // game they were observed doing. Also this doesn't apply to item changes
                    // that team owners make using the inventory, so there's not much point.
                },
                ParsedFeedEventText::Shipment { .. } => {
                    // See comment on Delivery
                },
                ParsedFeedEventText::SpecialDelivery { .. } => {
                    // See comment on Delivery
                },
                ParsedFeedEventText::AttributeChanges { changes } => {
                    for change in changes {
                        player_full_name.check_name(&change.player_name);
                        augments.push(NewPlayerAugment {
                            mmolb_player_id: &entity.entity_id,
                            feed_event_index,
                            time,
                            attribute: taxa.attribute_id(change.attribute.into()),
                            value: change.amount as i32,
                        })
                    }
                },
                ParsedFeedEventText::SingleAttributeEquals { player_name, changing_attribute, value_attribute } => {
                    player_full_name.check_name(&player_name);
                    // The handling of a non-priority SingleAttributeEquals will have to
                    // be so different that it's not worth trying to implement before it
                    // actually appears
                    process_paradigm_shift(
                        changing_attribute,
                        value_attribute,
                        &mut paradigm_shifts,
                        feed_event_index,
                        time,
                        entity,
                        taxa,
                    )
                },
                ParsedFeedEventText::MassAttributeEquals { value_attribute, changing_attribute, players } => {
                    if players.is_empty() {
                        // TODO Expose player ingest warnings on the site
                        warn!("MassAttributeEquals had 0 players");
                    } else if let Some(((_, player_name),)) = players.iter().collect_tuple() {
                        player_full_name.check_name(player_name);
                        process_paradigm_shift(
                            changing_attribute,
                            value_attribute,
                            &mut paradigm_shifts,
                            feed_event_index,
                            time,
                            entity,
                            taxa,
                        );
                    } else {
                        // TODO Expose player ingest warnings on the site
                        warn!("MassAttributeEquals on players shouldn't have more than one player");
                    }
                },
                ParsedFeedEventText::S1Enchantment { .. } => {
                    // This only affects items. Rationale is the same as Delivery.
                },
                ParsedFeedEventText::S2Enchantment { .. } => {
                    // This only affects items. Rationale is the same as Delivery.
                },
                ParsedFeedEventText::TakeTheMound { .. } => {
                    // We can use this to backdate certain position changes, but
                    // the utility of doing that is limited for the same reason
                    // as the utility of processing Delivery is limited.
                },
                ParsedFeedEventText::TakeThePlate { .. } => {
                    // See comment on TakeTheMound
                },
                ParsedFeedEventText::SwapPlaces { .. } => {
                    // See comment on TakeTheMound
                },
                ParsedFeedEventText::InfusedByFallingStar { .. } => {
                    // We can use this to backdate certain mod changes, but
                    // the utility of doing that is limited for the same reason
                    // as the utility of processing Delivery is limited.
                },
                ParsedFeedEventText::InjuredByFallingStar { .. } => {
                    // We don't (yet) have a use for this event
                },
                ParsedFeedEventText::Prosperous { .. } => {
                    // This is entirely redundant with the information available
                    // when parsing games. I suppose it might be used for
                    // synchronization, but we have no need of that yet.
                },
                ParsedFeedEventText::Recomposed { new, original } => {
                    player_full_name.check_name(new);
                    // Remember we're going backwards, so the player name
                    // goes from new to old
                    player_full_name.set_name(original.to_string());
                    recompositions.push(NewPlayerRecomposition {
                        mmolb_player_id: &entity.entity_id,
                        feed_event_index,
                        time,
                    });
                },
                ParsedFeedEventText::Modification { .. } => {
                    // See comment on HitByFallingStar
                },
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
                    if index + 1 != feed.len() && (event.season, &event.day) != (3, &Ok(Day::Day(1))) {
                        // TODO Expose player ingest warnings on the site
                        warn!(
                            "Released event wasn't the last event in the player's feed. {}/{}",
                            index + 1, feed.len(),
                        );
                    }
                }
            }
        }
    }

    if let Some(talk) = &entity.data.talk {
        if let Some(category) = &talk.batting {
            process_talk_category(category, entity, &mut reports, taxa);
        }
        if let Some(category) = &talk.pitching {
            process_talk_category(category, entity, &mut reports, taxa);
        }
        if let Some(category) = &talk.defense {
            process_talk_category(category, entity, &mut reports, taxa);
        }
        if let Some(category) = &talk.baserunning {
            process_talk_category(category, entity, &mut reports, taxa);
        }
    }

    (player, modifications, augments, paradigm_shifts, recompositions, reports)
}

fn process_paradigm_shift<'e>(
    changing_attribute: Attribute,
    value_attribute: Attribute,
    paradigm_shifts: &mut Vec<NewPlayerParadigmShift<'e>>,
    feed_event_index: i32,
    time: NaiveDateTime,
    entity: &'e ChronEntity<mmolb_parsing::player::Player>,
    taxa: &Taxa,
) {
    if changing_attribute == Attribute::Priority {
        paradigm_shifts.push(NewPlayerParadigmShift {
            mmolb_player_id: &entity.entity_id,
            feed_event_index,
            time,
            attribute: taxa.attribute_id(value_attribute.into()),
        })
    } else {
        // TODO Expose player ingest errors on the site
        error!(
            "Encountered a SingleAttributeEquals feed event that changes an \
            attribute other than priority. Player {} feed event {} changes {}",
            entity.entity_id, feed_event_index, changing_attribute,
        );
    }
}

fn process_talk_category<'e>(
    category: &TalkCategory,
    entity: &'e ChronEntity<mmolb_parsing::player::Player>,
    reports: &mut Vec<NewPlayerReport<'e>>,
    taxa: &Taxa,
) {
    let Ok(season) = category.season else {
        // TODO See if I can figure out a fallback for reports generated
        //   before `season` was added
        return;
    };

    let Ok(day) = &category.day else {
        // TODO See if I can figure out a fallback for reports generated
        //   before `day` was added
        return;
    };

    let (day_type, day, superstar_day) = day_to_db(day, taxa);

    for (attribute, stars) in &category.stars {
        reports.push(NewPlayerReport {
            mmolb_player_id: &entity.entity_id,
            season: season as i32,
            day_type,
            day,
            superstar_day,
            observed: entity.valid_from.naive_utc(),
            attribute: taxa.attribute_id((*attribute).into()),
            stars: *stars as i32,
        });
    }
}
