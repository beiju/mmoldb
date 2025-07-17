use crate::db::{CompletedGameForDb, GameForDb, Taxa, Timings};
use crate::ingest::sim::{self, Game, SimStartupError};
use crate::ingest::{IngestConfig, IngestFatalError, IngestLog, IngestStats, check_round_trip};
use crate::{Db, db};
use chron::{ChronEntities, ChronEntity};
use chrono::Utc;
use diesel::PgConnection;
use itertools::{Itertools, izip};
use log::{error, info};
use miette::Context;
use rocket::tokio;
use rocket_sync_db_pools::ConnectionPool;

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


pub(super) struct IngestWorker {
    pool: ConnectionPool<Db, PgConnection>,
    taxa: Taxa,
    config: IngestConfig,
    ingest_id: i64,
}

pub(super) struct IngestPageOfGamesOutput {
    pub worker: IngestWorker,
    pub stats: IngestStats,
    pub next_page: Option<String>,
}

type IngestPageOfGamesResult = Result<IngestPageOfGamesOutput, IngestFatalError>;

pub(super) struct IngestWorkerInProgress(tokio::task::JoinHandle<IngestPageOfGamesResult>);

impl IngestWorker {
    pub fn new(
        pool: ConnectionPool<Db, PgConnection>,
        taxa: Taxa,
        config: IngestConfig,
        ingest_id: i64,
    ) -> Self {
        Self {
            pool,
            taxa,
            config,
            ingest_id,
        }
    }

    pub fn ingest_page_of_games(
        self,
        page_index: usize,
        fetch_duration: f64,
        page: ChronEntities<mmolb_parsing::Game>,
    ) -> IngestWorkerInProgress {
        // tokio::spawn makes it start making progress before it's awaited
        IngestWorkerInProgress(tokio::spawn(self.ingest_page_internal(
            page_index,
            fetch_duration,
            page,
        )))
    }

    async fn ingest_page_internal<'t>(
        mut self,
        page_index: usize,
        fetch_duration: f64,
        mut page: ChronEntities<mmolb_parsing::Game>,
    ) -> IngestPageOfGamesResult {
        page.items.sort_by(|a, b| a.entity_id.cmp(&b.entity_id));

        let conn = self
            .pool
            .get()
            .await
            .ok_or(IngestFatalError::CouldNotGetConnection)?;

        conn.run(move |conn| {
            let stats =
                self.ingest_page_with_connection(page_index, fetch_duration, page.items, conn)?;
            Ok(IngestPageOfGamesOutput {
                worker: self,
                stats,
                next_page: page.next_page,
            })
        })
        .await
    }

    fn ingest_page_with_connection(
        &mut self,
        page_index: usize,
        fetch_duration: f64,
        all_games: Vec<ChronEntity<mmolb_parsing::Game>>,
        conn: &mut PgConnection,
    ) -> Result<IngestStats, IngestFatalError> {
        let save_start = Utc::now();
        let filter_finished_games_start = Utc::now();
        let all_games_len = all_games.len();
        let games_for_ingest = self.filter_out_finished_games(conn, all_games)?;
        let filter_finished_games_duration =
            (Utc::now() - filter_finished_games_start).as_seconds_f64();

        let parse_and_sim_start = Utc::now();
        let games_for_db = games_for_ingest
            .iter()
            .filter_map(prepare_game_for_db)
            .collect::<Result<Vec<_>, _>>()?;

        let num_ongoing_games_skipped = games_for_db
            .iter()
            .filter(|g| match g {
                GameForDb::Incomplete { .. } => true,
                _ => false,
            })
            .count();
        let num_games_with_fatal_errors = games_for_db
            .iter()
            .filter(|g| match g {
                GameForDb::FatalError { .. } => true,
                _ => false,
            })
            .count();
        let num_already_ingested_games_skipped = all_games_len - games_for_ingest.len();
        let num_terminal_incomplete_games_skipped = games_for_ingest.len() - games_for_db.len();
        let num_games_imported = games_for_db.len() - num_ongoing_games_skipped - num_games_with_fatal_errors;
        info!(
            "Ingesting {num_games_imported} games, skipping {num_games_with_fatal_errors} games \
            due to fatal errors, ignoring {num_already_ingested_games_skipped} already-ingested \
            games, ignoring {num_ongoing_games_skipped} games in progress, and skipping \
            {num_terminal_incomplete_games_skipped} terminal incomplete games.",
        );
        let parse_and_sim_duration = (Utc::now() - parse_and_sim_start).as_seconds_f64();

        let db_insert_start = Utc::now();
        let db_insert_timings = db::insert_games(conn, &self.taxa, self.ingest_id, &games_for_db)?;
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
                GameForDb::Completed(game) => Some(game.id),
                _ => None,
            })
            .collect_vec();

        let (ingested_games, events_for_game_timings) =
            db::events_for_games(conn, &self.taxa, &mmolb_game_ids)?;
        assert_eq!(mmolb_game_ids.len(), ingested_games.len());
        let db_fetch_for_check_duration = (Utc::now() - db_fetch_for_check_start).as_seconds_f64();

        let check_round_trip_start = Utc::now();
        let additional_logs = games_for_db.iter()
            .filter_map(|game| match game {
                GameForDb::Completed(game) => Some(game),
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
        let check_round_trip_duration = (Utc::now() - check_round_trip_start).as_seconds_f64();

        let insert_extra_logs_start = Utc::now();
        if !additional_logs.is_empty() {
            db::insert_additional_ingest_logs(conn, &additional_logs)?;
        }
        let insert_extra_logs_duration = (Utc::now() - insert_extra_logs_start).as_seconds_f64();
        let save_duration = (Utc::now() - save_start).as_seconds_f64();

        db::insert_timings(
            conn,
            self.ingest_id,
            page_index,
            Timings {
                fetch_duration,
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

        Ok::<_, IngestFatalError>(IngestStats {
            num_ongoing_games_skipped,
            num_terminal_incomplete_games_skipped,
            num_already_ingested_games_skipped,
            num_games_with_fatal_errors,
            num_games_imported,
        })
    }

    fn filter_out_finished_games(
        &mut self,
        conn: &mut PgConnection,
        all_games: Vec<ChronEntity<mmolb_parsing::Game>>,
    ) -> Result<Vec<ChronEntity<mmolb_parsing::Game>>, IngestFatalError> {
        Ok(if !self.config.reimport_all_games {
            let all_game_ids = all_games.iter().map(|e| e.entity_id.as_str()).collect_vec();
            // Remove any games which are fully imported
            let mut is_finished = db::is_finished(conn, &all_game_ids)?.into_iter().peekable();
            all_games
                .into_iter()
                .filter_map(|game| {
                    if let Some((_, finished)) =
                        is_finished.next_if(|(id, _)| id == &game.entity_id)
                    {
                        if finished {
                            // Game is finished, don't import it again
                            None
                        } else {
                            // Game is not finished, do import it again
                            Some(game)
                        }
                    } else {
                        // Game is not in the db, import it for the first time
                        Some(game)
                    }
                })
                .collect()
        } else {
            all_games
        })
    }
}

impl IngestWorkerInProgress {
    pub fn is_ready(&self) -> bool {
        self.0.is_finished()
    }

    pub async fn into_future(self) -> IngestPageOfGamesResult {
        self.0.await?
    }
}

fn diagnostic_to_string(err: miette::Report) -> String {
    let handler = miette::GraphicalReportHandler::new_themed(miette::GraphicalTheme::unicode_nocolor());

    let mut error_message = String::new();
    handler.render_report(&mut error_message, err.as_ref())
        .expect("Formatting into a String buffer can't fail");

    error_message
}

fn prepare_game_for_db(
    entity: &ChronEntity<mmolb_parsing::Game>,
) -> Option<Result<GameForDb, IngestFatalError>> {
    Some(Ok(if !entity.data.is_terminal() {
        GameForDb::Incomplete {
            game_id: &entity.entity_id,
            raw_game: &entity.data,
        }
    } else if entity.data.is_completed() {
        let game_result = prepare_completed_game_for_db(entity)
            .wrap_err("Error constructing the initial state. This entire game will be skipped.");
        match game_result {
            Ok(game) => GameForDb::Completed(game),
            Err(err) => {
                GameForDb::FatalError {
                    game_id: &entity.entity_id,
                    raw_game: &entity.data,
                    error_message: diagnostic_to_string(err),
                }
            }
        }
    } else {
        return None;
    }))
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

            let event_result = game.next(game_event_index, &parsed, &raw, &mut ingest_logs)
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
