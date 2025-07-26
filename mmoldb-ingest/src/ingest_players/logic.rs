use hashbrown::{HashMap, hash_map::Entry};
use chrono::Utc;
use itertools::Itertools;
use log::{debug, error, info, warn};
use miette::Diagnostic;
use mmolb_parsing::enums::{Day, Handedness, Position};
use mmolb_parsing::NotRecognized;
use thiserror::Error;
use chron::ChronEntity;
use mmoldb_db::{db, PgConnection, QueryError, QueryResult};
use mmoldb_db::db::NameEmojiTooltip;
use mmoldb_db::models::{NewPlayerModificationVersion, NewPlayerVersion};
use mmoldb_db::taxa::{Taxa, TaxaDayType, TaxaSlot};

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
        .into_iter()
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
        let inserted = db::insert_player_versions(conn, players_to_update)?;
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

    info!("Ingested page of {} players in {save_duration:.3} seconds", players.len());

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

fn chron_player_as_new<'a>(
    entity: &'a ChronEntity<mmolb_parsing::player::Player>, 
    taxa: &Taxa, 
    modifications: &HashMap<NameEmojiTooltip, i64>,
) -> (NewPlayerVersion<'a>, Vec<NewPlayerModificationVersion<'a>>) {
    let (birthday_type, birthday_day, birthday_superstar_day) = match &entity.data.birthday {
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
            error!("Player was born on a unexpected postseason day {other} (expected 1-3)");
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
            error!("Player was born on an unrecognized day {err}");
            (None, None, None)
        }
    };

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

    // let x = match &entity.data.feed {
    //     Err(_) => Vec::new(),
    //     Ok(feed) => feed.iter().map(|event| {
    //         match event.event_type {
    //             Err(_) => { None }
    //             Ok(event_type) => match event_type {
    //                 FeedEventType::Game => {}
    //                 FeedEventType::Augment => {}
    //             }
    //         }
    //     })
    //         .collect_vec()
    // };

    (player, modifications)
}