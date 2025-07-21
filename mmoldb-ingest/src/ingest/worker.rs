use crate::ingest::sim::{self, Game, SimStartupError};
use crate::ingest::{IngestFatalError, IngestStats, check_round_trip};
use chron::{ChronEntity};
use chrono::Utc;
use itertools::{Itertools, izip};
use log::{debug, error, info};
use miette::Context;
use mmoldb_db::{db, IngestLog, PgConnection};
use mmoldb_db::db::{CompletedGameForDb, GameForDb, Timings};
use mmoldb_db::taxa::Taxa;

pub trait GameExt {
    /// Returns true for any game which will never be updated. This includes all
    /// finished games and a set of games from s0d60 that the sim lost track of
    /// and will never be finished.
    fn is_terminal(&self) -> bool;

    /// True for any game in the "Completed" state
    fn is_completed(&self) -> bool;
}

impl GameExt for mmolb_parsing::Game {
    fn is_terminal(&self) -> bool {
        // There are some games from season 0 that aren't completed and never
        // will be.
        self.season == 0 || self.is_completed()
    }

    fn is_completed(&self) -> bool {
        self.state == "Complete"
    }
}

pub fn ingest_page_of_games(
    taxa: &Taxa,
    ingest_id: i64,
    page_index: usize,
    get_batch_to_process_duration: f64,
    all_games_json: Vec<ChronEntity<serde_json::Value>>,
    conn: &mut PgConnection,
    worker_id: usize,
) -> Result<IngestStats, IngestFatalError> {
    debug!("Starting ingest page of {} games on worker {worker_id}", all_games_json.len());
    let save_start = Utc::now();
    let deserialize_games_start = Utc::now();
    // TODO Turn game deserialize failure into a GameForDb::FatalError case
    //   instead of propagating it
    let all_games = all_games_json.into_iter()
        .map(|game_json| Ok::<_, serde_json::Error>(ChronEntity {
            kind: game_json.kind,
            entity_id: game_json.entity_id,
            valid_from: game_json.valid_from,
            valid_until: game_json.valid_until,
            data: serde_json::from_value(game_json.data)?,
        }))
        .collect::<Result<Vec<_>, _>>()?;
    let deserialize_games_duration = (Utc::now() - deserialize_games_start).as_seconds_f64();
    debug!("Deserialized page of {} games on worker {worker_id}", all_games.len());

    let filter_finished_games_start = Utc::now();
    let filter_finished_games_duration =
        (Utc::now() - filter_finished_games_start).as_seconds_f64();

    let parse_and_sim_start = Utc::now();
    let games_for_db = all_games
        .iter()
        .map(prepare_game_for_db)
        .collect::<Result<Vec<_>, _>>()?;
    debug!("Prepared {} games on worker {worker_id}", games_for_db.len());

    assert_eq!(
        games_for_db.len(), all_games.len(),
        "Ingest logic requires all games to have an entry in the db, otherwise it will keep trying \
        to ingest them forever.",
    );

    // Kind of confusing, but "skipped" games still have entries in
    // data.games. They just don't have anything in data.events.
    let mut num_ongoing_games_skipped = 0;
    let mut num_bugged_games_skipped = 0;
    let mut num_games_imported = 0;
    let mut num_games_with_fatal_errors = 0;
    for game in &games_for_db {
        match game {
            GameForDb::Ongoing { .. } => { num_ongoing_games_skipped += 1; }
            GameForDb::ForeverIncomplete { .. } => { num_bugged_games_skipped += 1; }
            GameForDb::Completed { .. } => { num_games_imported += 1; }
            GameForDb::FatalError { .. } => { num_games_with_fatal_errors += 1; }
        }
    }
    assert_eq!(num_ongoing_games_skipped + num_bugged_games_skipped + num_games_imported + num_games_with_fatal_errors, games_for_db.len());
    info!(
        "Ingesting {num_games_imported} games, skipping {num_games_with_fatal_errors} games \
        due to fatal errors, ignoring {num_ongoing_games_skipped} games in progress, and skipping \
        {num_bugged_games_skipped} bugged games on worker {worker_id}.",
    );
    let parse_and_sim_duration = (Utc::now() - parse_and_sim_start).as_seconds_f64();

    let db_insert_start = Utc::now();
    let db_insert_timings = db::insert_games(conn, taxa, ingest_id, &games_for_db)?;
    debug!("Inserted {} games on worker {worker_id}", games_for_db.len());
    let db_insert_duration = (Utc::now() - db_insert_start).as_seconds_f64();

    // Immediately turn around and fetch all the games we just inserted,
    // so we can verify that they round-trip correctly.
    // This step, and all the following verification steps, could be
    // skipped. However, my profiling shows that it's negligible
    // cost so I haven't added the capability.
    let db_fetch_for_check_start = Utc::now();
    let mmolb_game_ids = games_for_db
        .iter()
        .filter_map(|game| match game {
            GameForDb::Completed { game, .. } => Some(game.id),
            _ => None,
        })
        .collect_vec();
    debug!("Checking {} games on worker {worker_id}", mmolb_game_ids.len());

    let (ingested_games, events_for_game_timings) =
        db::events_for_games(conn, taxa, &mmolb_game_ids)?;
    assert_eq!(mmolb_game_ids.len(), ingested_games.len());
    debug!("Fetched {} games on worker {worker_id}", ingested_games.len());
    let db_fetch_for_check_duration = (Utc::now() - db_fetch_for_check_start).as_seconds_f64();

    let check_round_trip_start = Utc::now();
    let additional_logs = games_for_db.iter()
        .filter_map(|game| match game {
            GameForDb::Completed { game, .. } => Some(game),
            _ => None,
        })
        .zip(&ingested_games)
        .filter_map(|(game, (game_id, inserted_events))| {
            let detail_events = &game.events;
            let mut extra_ingest_logs = IngestLogs::new();
            if inserted_events.len() != detail_events.len() {
                error!(
                    "Number of events read from the db ({}) does not match number of events written to \
                    the db ({})",
                    inserted_events.len(),
                    detail_events.len(),
                );
            }
            for (reconstructed_detail, original_detail) in izip!(inserted_events, detail_events) {
                let index = original_detail.game_event_index;
                let fair_ball_index = original_detail.fair_ball_event_index;

                if let Some(index) = fair_ball_index {
                    check_round_trip::check_round_trip(
                        index,
                        &mut extra_ingest_logs,
                        true,
                        &game.parsed_game[index],
                        &original_detail,
                        reconstructed_detail,
                    );
                }

                check_round_trip::check_round_trip(
                    index,
                    &mut extra_ingest_logs,
                    false,
                    &game.parsed_game[index],
                    &original_detail,
                    reconstructed_detail,
                );
            }
            let extra_ingest_logs = extra_ingest_logs.into_vec();
            if extra_ingest_logs.is_empty() {
                None
            } else {
                Some((*game_id, extra_ingest_logs))
            }
        })
        .collect_vec();
    debug!("Collected logs for {} games on worker {worker_id}", additional_logs.len());
    let check_round_trip_duration = (Utc::now() - check_round_trip_start).as_seconds_f64();

    let insert_extra_logs_start = Utc::now();
    if !additional_logs.is_empty() {
        db::insert_additional_ingest_logs(conn, &additional_logs)?;
        debug!("Inserted logs for {} games on worker {worker_id}", additional_logs.len());
    } else {
        debug!("No need to insert additional logs on worker {worker_id}");
    }
    let insert_extra_logs_duration = (Utc::now() - insert_extra_logs_start).as_seconds_f64();
    let save_duration = (Utc::now() - save_start).as_seconds_f64();

    db::insert_timings(
        conn,
        ingest_id,
        page_index,
        Timings {
            get_batch_to_process_duration,
            deserialize_games_duration,
            filter_finished_games_duration,
            parse_and_sim_duration,
            db_insert_duration,
            db_insert_timings,
            db_fetch_for_check_duration,
            events_for_game_timings,
            check_round_trip_duration,
            insert_extra_logs_duration,
            save_duration,
        },
    )?;
    debug!("Saved ingest timings on worker {worker_id}");

    Ok::<_, IngestFatalError>(IngestStats {
        num_ongoing_games_skipped,
        num_bugged_games_skipped,
        num_games_with_fatal_errors,
        num_games_imported,
    })
}

fn diagnostic_to_string(err: miette::Report) -> String {
    let handler =
        miette::GraphicalReportHandler::new_themed(miette::GraphicalTheme::unicode_nocolor());

    let mut error_message = String::new();
    handler
        .render_report(&mut error_message, err.as_ref())
        .expect("Formatting into a String buffer can't fail");

    error_message
}

fn prepare_game_for_db(
    entity: &ChronEntity<mmolb_parsing::Game>,
) -> Result<GameForDb, IngestFatalError> {
    Ok(if !entity.data.is_terminal() {
        GameForDb::Ongoing {
            game_id: &entity.entity_id,
            from_version: entity.valid_from,
            raw_game: &entity.data,
        }
    } else if entity.data.is_completed() {
        let game_result = prepare_completed_game_for_db(entity)
            .wrap_err("Error constructing the initial state. This entire game will be skipped.");
        match game_result {
            Ok(game) => GameForDb::Completed {
                game,
                from_version: entity.valid_from,
            },
            Err(err) => GameForDb::FatalError {
                game_id: &entity.entity_id,
                from_version: entity.valid_from,
                raw_game: &entity.data,
                error_message: diagnostic_to_string(err),
            },
        }
    } else {
        GameForDb::ForeverIncomplete {
            game_id: &entity.entity_id,
            from_version: entity.valid_from,
            raw_game: &entity.data,
        }
    })
}

fn prepare_completed_game_for_db(
    entity: &ChronEntity<mmolb_parsing::Game>,
) -> Result<CompletedGameForDb, SimStartupError> {
    let parsed_game = mmolb_parsing::process_game(&entity.data, &entity.entity_id);

    // I'm adding enumeration to parsed, then stripping it out for
    // the iterator fed to Game::new, on purpose. I need the
    // counting to count every event, but I don't need the count
    // inside Game::new.
    let mut parsed = parsed_game.iter().zip(&entity.data.event_log).enumerate();

    let (mut game, mut all_logs) = {
        let mut parsed_for_game = (&mut parsed).map(|(_, (parsed, _))| parsed);

        Game::new(&entity.entity_id, &entity.data, &mut parsed_for_game)?
    };

    let stadium_name = game.stadium_name;

    let detail_events = parsed
        .map(|(game_event_index, (parsed, raw))| {
            // Sim has a different IngestLogs... this made sense at the time
            let mut ingest_logs = sim::IngestLogs::new(game_event_index as i32);

            let unparsed = parsed
                .clone()
                .unparse(&entity.data, Some(game_event_index as _));
            if unparsed != raw.message {
                ingest_logs.error(format!(
                    "Round-trip of raw event through ParsedEvent produced a mismatch:\n\
                     Original: <pre>{:?}</pre>\n\
                     Through EventDetail: <pre>{:?}</pre>",
                    raw.message, unparsed,
                ));
            }

            let event_result = game
                .next(game_event_index, &parsed, &raw, &mut ingest_logs)
                .wrap_err(
                    "Error processing game event. This event will be skipped, and further \
                    errors are likely if this error left the game in an incorrect state.",
                );
            let event = match event_result {
                Ok(result) => result,
                Err(err) => {
                    ingest_logs.critical(diagnostic_to_string(err));
                    None
                }
            };

            all_logs.push(ingest_logs.into_vec());

            event
        })
        .collect_vec();

    // Take the None values out of detail_events
    let events = detail_events
        .into_iter()
        .filter_map(|event| event)
        .collect_vec();

    Ok(CompletedGameForDb {
        id: &entity.entity_id,
        raw_game: &entity.data,
        events,
        logs: all_logs,
        parsed_game,
        stadium_name,
    })
}

// A utility to more conveniently build a Vec<IngestLog>
pub struct IngestLogs {
    logs: Vec<IngestLog>,
}

impl IngestLogs {
    pub fn new() -> Self {
        Self { logs: Vec::new() }
    }

    #[allow(dead_code)]
    pub fn critical(&mut self, game_event_index: usize, s: impl Into<String>) {
        self.logs.push(IngestLog {
            game_event_index: game_event_index as i32,
            log_level: 0,
            log_text: s.into(),
        });
    }

    pub fn error(&mut self, game_event_index: usize, s: impl Into<String>) {
        self.logs.push(IngestLog {
            game_event_index: game_event_index as i32,
            log_level: 1,
            log_text: s.into(),
        });
    }

    #[allow(dead_code)]
    pub fn warn(&mut self, game_event_index: usize, s: impl Into<String>) {
        self.logs.push(IngestLog {
            game_event_index: game_event_index as i32,
            log_level: 2,
            log_text: s.into(),
        });
    }

    #[allow(dead_code)]
    pub fn info(&mut self, game_event_index: usize, s: impl Into<String>) {
        self.logs.push(IngestLog {
            game_event_index: game_event_index as i32,
            log_level: 3,
            log_text: s.into(),
        });
    }

    #[allow(dead_code)]
    pub fn debug(&mut self, game_event_index: usize, s: impl Into<String>) {
        self.logs.push(IngestLog {
            game_event_index: game_event_index as i32,
            log_level: 4,
            log_text: s.into(),
        });
    }

    #[allow(dead_code)]
    pub fn trace(&mut self, game_event_index: usize, s: impl Into<String>) {
        self.logs.push(IngestLog {
            game_event_index: game_event_index as i32,
            log_level: 5,
            log_text: s.into(),
        });
    }

    pub fn into_vec(self) -> Vec<IngestLog> {
        self.logs
    }
}
