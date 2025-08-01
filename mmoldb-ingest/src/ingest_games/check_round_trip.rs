use crate::db::RowToEventError;
use crate::ingest_games::worker::IngestLogs;
use itertools::{EitherOrBoth, Itertools};
use mmolb_parsing::ParsedEventMessage;
use mmolb_parsing::enums::Place;
use mmolb_parsing::parsed_event::PlacedPlayer;
use mmoldb_db::EventDetail;
use strum::IntoDiscriminant;

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

// When generating our events we intentionally add more detail to `Place`s than
// was originally there. This causes round-trip errors if it's not specifically
// address it. This function specifically addresses it by mutating the
// reconstructed event's places to match the original's, where they're
// compatible.
fn downgrade_parsed_places_to_match(
    game_event_index: usize,
    ours: &mut ParsedEventMessage<&str>,
    original: &ParsedEventMessage<&str>,
    ingest_logs: &mut IngestLogs,
) {
    // TODO The bodies of the non-empty members of this are nearly
    //   identical. Refactor that logic out into a function somehow.
    match ours {
        ParsedEventMessage::BatterToBase { fielder, .. } => {
            if let ParsedEventMessage::BatterToBase {
                fielder: original_fielder,
                ..
            } = original
            {
                downgrade_place_to_match(
                    game_event_index,
                    fielder,
                    original_fielder,
                    ingest_logs,
                    "BatterToBase fielder",
                );
            } else {
                ingest_logs.warn(
                    game_event_index,
                    format!(
                        "Not downgrading parsed Places because the event types don't match \
                    (reconstructed is {:?} and original is {:?})",
                        ours.discriminant(),
                        original.discriminant(),
                    ),
                );
            }
        }
        ParsedEventMessage::CaughtOut { caught_by, .. } => {
            if let ParsedEventMessage::CaughtOut {
                caught_by: original_caught_by,
                ..
            } = original
            {
                downgrade_place_to_match(
                    game_event_index,
                    caught_by,
                    original_caught_by,
                    ingest_logs,
                    "CaughtOut caught_by",
                );
            } else {
                ingest_logs.warn(
                    game_event_index,
                    format!(
                        "Not downgrading parsed Places because the event types don't match \
                    (reconstructed is {:?} and original is {:?})",
                        ours.discriminant(),
                        original.discriminant(),
                    ),
                );
            }
        }
        ParsedEventMessage::GroundedOut { fielders, .. } => {
            if let ParsedEventMessage::GroundedOut {
                fielders: original_fielders,
                ..
            } = original
            {
                downgrade_places_to_match(
                    game_event_index,
                    fielders,
                    original_fielders,
                    ingest_logs,
                    "GroundedOut fielders",
                );
            } else {
                ingest_logs.warn(
                    game_event_index,
                    format!(
                        "Not downgrading parsed Places because the event types don't match \
                    (reconstructed is {:?} and original is {:?})",
                        ours.discriminant(),
                        original.discriminant(),
                    ),
                );
            }
        }
        ParsedEventMessage::ForceOut { fielders, .. } => {
            if let ParsedEventMessage::ForceOut {
                fielders: original_fielders,
                ..
            } = original
            {
                downgrade_places_to_match(
                    game_event_index,
                    fielders,
                    original_fielders,
                    ingest_logs,
                    "ForceOut fielders",
                );
            } else {
                ingest_logs.warn(
                    game_event_index,
                    format!(
                        "Not downgrading parsed Places because the event types don't match \
                    (reconstructed is {:?} and original is {:?})",
                        ours.discriminant(),
                        original.discriminant(),
                    ),
                );
            }
        }
        ParsedEventMessage::ReachOnFieldersChoice { fielders, .. } => {
            if let ParsedEventMessage::ReachOnFieldersChoice {
                fielders: original_fielders,
                ..
            } = original
            {
                downgrade_places_to_match(
                    game_event_index,
                    fielders,
                    original_fielders,
                    ingest_logs,
                    "ReachOnFieldersChoice fielders",
                );
            } else {
                ingest_logs.warn(
                    game_event_index,
                    format!(
                        "Not downgrading parsed Places because the event types don't match \
                    (reconstructed is {:?} and original is {:?})",
                        ours.discriminant(),
                        original.discriminant(),
                    ),
                );
            }
        }
        _ => {}
    }
}

fn downgrade_places_to_match(
    game_event_index: usize,
    ours: &mut [PlacedPlayer<&str>],
    original: &[PlacedPlayer<&str>],
    ingest_logs: &mut IngestLogs,
    log_loc: &str,
) {
    ours.into_iter()
        .zip_longest(original)
        .enumerate()
        .for_each(|(i, pair)| match pair {
            EitherOrBoth::Both(ours, original) => {
                let loc = format!("{log_loc}[{i}]");
                downgrade_place_to_match(game_event_index, ours, original, ingest_logs, &loc);
            }
            EitherOrBoth::Left(ours) => {
                ingest_logs.warn(
                    game_event_index,
                    format!(
                        "Not downgrading parsed Place for {} because the reconstructed event's \
                    {log_loc} item at index {i} had no corresponding item in the original",
                        ours.name,
                    ),
                );
            }
            EitherOrBoth::Right(original) => {
                ingest_logs.warn(game_event_index, format!(
                    "Not downgrading parsed Place for {} because the original event's {log_loc} \
                    item at index {i} had no corresponding item in the reconstruction",
                    original.name,
                ));
            }
        });
}

fn downgrade_place_to_match(
    game_event_index: usize,
    ours: &mut PlacedPlayer<&str>,
    original: &PlacedPlayer<&str>,
    ingest_logs: &mut IngestLogs,
    log_loc: &str,
) {
    if ours.name != original.name {
        ingest_logs.warn(
            game_event_index,
            format!(
                "Not downgrading parsed Place at {log_loc} because the reconstructed player's name \
            ({}) didn't match the original's ({})",
                ours.name, original.name,
            ),
        );
    } else {
        match original.place {
            Place::Pitcher => {
                // Remember, any player (even the dh? maybe not them) can pitch
                ours.place = Place::Pitcher;
            }
            Place::StartingPitcher(None) => {
                downgrade_place_to_starting_pitcher(
                    game_event_index,
                    &mut ours.place,
                    ingest_logs,
                    log_loc,
                );
            }
            Place::ReliefPitcher(None) => {
                downgrade_place_to_relief_pitcher(
                    game_event_index,
                    &mut ours.place,
                    ingest_logs,
                    log_loc,
                );
            }
            _ => {
                // Everything else doesn't need downgrading
            }
        }
    }
}

fn downgrade_place_to_starting_pitcher(
    game_event_index: usize,
    ours: &mut Place,
    ingest_logs: &mut IngestLogs,
    log_loc: &str,
) {
    match ours {
        Place::StartingPitcher(_) => *ours = Place::StartingPitcher(None),
        _ => {
            ingest_logs.error(
                game_event_index,
                format!(
                    "Can't \"downgrade\" {ours:?} into Place::StartingPitcher(None) at {log_loc}",
                ),
            );
        }
    }
}

fn downgrade_place_to_relief_pitcher(
    game_event_index: usize,
    ours: &mut Place,
    ingest_logs: &mut IngestLogs,
    log_loc: &str,
) {
    match ours {
        Place::ReliefPitcher(_) => *ours = Place::ReliefPitcher(None),
        _ => {
            ingest_logs.error(
                game_event_index,
                format!(
                    "Can't \"downgrade\" {ours:?} into Place::ReliefPitcher(None) at {log_loc}",
                ),
            );
        }
    }
}

// The particular combination of &str and String type arguments is
// dictated by the caller
pub fn check_round_trip(
    index: usize,
    ingest_logs: &mut IngestLogs,
    is_contact_event: bool,
    parsed: &ParsedEventMessage<&str>,
    original_detail: &EventDetail<&str>,
    reconstructed_detail: &Result<EventDetail<String>, RowToEventError>,
) {
    let Some(mut parsed_through_detail) = (if is_contact_event {
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

    downgrade_parsed_places_to_match(index, &mut parsed_through_detail, parsed, ingest_logs);

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

    let Some(mut parsed_through_db) = (if is_contact_event {
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

    downgrade_parsed_places_to_match(index, &mut parsed_through_db, parsed, ingest_logs);

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
