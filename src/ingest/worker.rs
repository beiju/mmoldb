use crate::db::{CompletedGameForDb, GameForDb, RowToEventError, Taxa, Timings};
use crate::ingest::sim::{self, Game, SimFatalError};
use crate::ingest::{ChronEntities, ChronEntity, EventDetail, GameExt, IngestConfig, IngestFatalError, IngestLog, IngestStats};
use crate::{Db, db};
use chrono::Utc;
use diesel::PgConnection;
use itertools::{Itertools, izip};
use log::{error, info, warn};
use mmolb_parsing::ParsedEventMessage;
use rocket::tokio;
use rocket_sync_db_pools::ConnectionPool;

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
                GameForDb::Completed { .. } => false,
            })
            .count();
        let num_already_ingested_games_skipped = all_games_len - games_for_ingest.len();
        let num_terminal_incomplete_games_skipped = games_for_ingest.len() - games_for_db.len();
        let num_games_imported = games_for_db.len() - num_ongoing_games_skipped;
        info!(
            "Ingesting {num_games_imported} games, ignoring {num_already_ingested_games_skipped} \
            already-ingested games, ignoring {num_ongoing_games_skipped} games in progress, and \
            skipping {num_terminal_incomplete_games_skipped} terminal incomplete games.",
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
                GameForDb::Incomplete { .. } => None,
                GameForDb::Completed(game) => Some(game.id),
            })
            .collect_vec();

        let (ingested_games, events_for_game_timings) =
            db::events_for_games(conn, &self.taxa, &mmolb_game_ids)?;
        assert_eq!(mmolb_game_ids.len(), ingested_games.len());
        let db_fetch_for_check_duration = (Utc::now() - db_fetch_for_check_start).as_seconds_f64();

        let check_round_trip_start = Utc::now();
        let additional_logs = games_for_db.iter()
            .filter_map(|game| match game {
                GameForDb::Incomplete { .. } => { None }
                GameForDb::Completed(game) => { Some(game) }
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
                        check_round_trip(
                            index,
                            &mut extra_ingest_logs,
                            true,
                            &game.parsed_game[index],
                            &original_detail,
                            reconstructed_detail,
                        );
                    }

                    check_round_trip(
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

fn prepare_game_for_db(
    entity: &ChronEntity<mmolb_parsing::Game>,
) -> Option<Result<GameForDb, IngestFatalError>> {
    Some(Ok(if !entity.data.is_terminal() {
        GameForDb::Incomplete {
            game_id: &entity.entity_id,
            raw_game: &entity.data,
        }
    } else if entity.data.is_completed() {
        match prepare_completed_game_for_db(entity) {
            Ok(game) => GameForDb::Completed(game),
            Err(err) => {
                // TODO Surface this error on the games with issues page
                let description = format!(
                    "{} {} @ {} {} s{}d{}",
                    entity.data.away_team_emoji,
                    entity.data.away_team_name,
                    entity.data.home_team_emoji,
                    entity.data.home_team_name,
                    entity.data.season,
                    entity.data.day,
                );
                warn!("Sim fatal error importing {description}: {err}. This game will be skipped.");
                GameForDb::Incomplete {
                    game_id: &entity.entity_id,
                    raw_game: &entity.data,
                }
            }
        }
    } else {
        return None;
    }))
}

fn prepare_completed_game_for_db(
    entity: &ChronEntity<mmolb_parsing::Game>,
) -> Result<CompletedGameForDb, SimFatalError> {
    let parsed_game = mmolb_parsing::process_game(&entity.data);

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

            let unparsed = parsed.clone().unparse();
            if unparsed != raw.message {
                ingest_logs.error(format!(
                    "Round-trip of raw event through ParsedEvent produced a mismatch:\n\
                     Original: <pre>{:?}</pre>\n\
                     Through EventDetail: <pre>{:?}</pre>",
                    raw.message, unparsed,
                ));
            }

            let event = match game.next(game_event_index, &parsed, &raw, &mut ingest_logs) {
                Ok(result) => result,
                Err(e) => {
                    ingest_logs.critical(e.to_string());
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

fn log_if_error<'g, E: std::fmt::Display>(
    ingest_logs: &mut IngestLogs,
    index: usize,
    to_parsed_result: Result<ParsedEventMessage<&'g str>, E>,
    log_prefix: &str,
) -> Option<ParsedEventMessage<&'g str>> {
    match to_parsed_result {
        Ok(to_contact_result) => Some(to_contact_result),
        Err(err) => {
            ingest_logs.error(index, format!("{log_prefix}: {err}"));
            None
        }
    }
}

// The particular combination of &str and String type arguments is
// dictated by the caller
fn check_round_trip(
    index: usize,
    ingest_logs: &mut IngestLogs,
    is_contact_event: bool,
    parsed: &ParsedEventMessage<&str>,
    original_detail: &EventDetail<&str>,
    reconstructed_detail: &Result<EventDetail<String>, RowToEventError>,
) {
    let Some(parsed_through_detail) = (if is_contact_event {
        log_if_error(
            ingest_logs,
            index,
            original_detail.to_parsed_contact(),
            "Attempt to round-trip contact event through ParsedEventMessage -> EventDetail -> \
            ParsedEventMessage failed at the EventDetail -> ParsedEventMessage step with error",
        )
    } else {
        log_if_error(
            ingest_logs,
            index,
            original_detail.to_parsed(),
            "Attempt to round-trip event through ParsedEventMessage -> EventDetail -> \
            ParsedEventMessage failed at the EventDetail -> ParsedEventMessage step with error",
        )
    }) else {
        return;
    };

    if parsed != &parsed_through_detail {
        ingest_logs.error(
            index,
            format!(
                "Round-trip of {} through EventDetail produced a mismatch:\n\
                 Original: <pre>{:?}</pre>\
                 Through EventDetail: <pre>{:?}</pre>",
                if is_contact_event {
                    "contact event"
                } else {
                    "event"
                },
                parsed,
                parsed_through_detail,
            ),
        );
    }

    let reconstructed_detail = match reconstructed_detail {
        Ok(reconstructed_detail) => reconstructed_detail,
        Err(err) => {
            ingest_logs.error(
                index,
                format!(
                    "Attempt to round-trip {} ParsedEventMessage -> EventDetail -> database \
                    -> EventDetail -> ParsedEventMessage failed at the database -> EventDetail \
                    step with error: {err}",
                    if is_contact_event {
                        "contact event"
                    } else {
                        "event"
                    }
                ),
            );
            return;
        }
    };

    let Some(parsed_through_db) = (if is_contact_event {
        log_if_error(
            ingest_logs,
            index,
            reconstructed_detail.to_parsed_contact(),
            "Attempt to round-trip contact event through ParsedEventMessage -> EventDetail -> \
            database -> EventDetail -> ParsedEventMessage failed at the EventDetail -> \
            ParsedEventMessage step with error",
        )
    } else {
        log_if_error(
            ingest_logs,
            index,
            reconstructed_detail.to_parsed(),
            "Attempt to round-trip event through ParsedEventMessage -> EventDetail -> database \
            -> EventDetail -> ParsedEventMessage failed at the EventDetail -> ParsedEventMessage \
            step with error",
        )
    }) else {
        return;
    };

    if parsed != &parsed_through_db {
        ingest_logs.error(
            index,
            format!(
                "Round-trip of {} through database produced a mismatch:\n\
                 Original: <pre>{:?}</pre>\n\
                 Through database: <pre>{:?}</pre>",
                if is_contact_event {
                    "contact event"
                } else {
                    "event"
                },
                parsed,
                parsed_through_db,
            ),
        );
    }
}
