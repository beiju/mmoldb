use super::pages::*;
use crate::{Db, StatsCache};
use crate::web::error::AppError;
use itertools::Itertools;
use mmoldb_db::db;
use mmoldb_db::models::DbPlayerVersion;
use mmoldb_db::taxa::{AsInsertable, Taxa, TaxaDayType, TaxaEventType};
use rocket::{State, get, uri};
use rocket_dyn_templates::{Template, context};
use serde::Serialize;
use std::collections::HashMap;
use log::error;
use rocket_db_pools::Connection;
use rocket_db_pools::deadpool_redis::redis::AsyncCommands;
use mmoldb_db::db::Outcome;

#[derive(Serialize)]
pub struct PlayerContext<'r, 't> {
    home: &'r str,
    first_name: &'r str,
    last_name: &'r str,
    birthday: String,
    batting_handedness: Option<&'t str>,
    pitching_handedness: Option<&'t str>,
    likes: &'r str,
    dislikes: &'r str,
    durability: f64,
    slot: Option<&'r str>,
}

impl<'r, 't> PlayerContext<'r, 't> {
    fn from_db(raw: &'r DbPlayerVersion, taxa: &'t Taxa) -> PlayerContext<'r, 't> {
        let birthday_day = match raw.birthday_type {
            None => "Error storing player's birthday".to_string(),
            Some(birthday_type) => match taxa.day_type_from_id(birthday_type) {
                TaxaDayType::Preseason => "Preseason".to_string(),
                TaxaDayType::RegularDay => match raw.birthday_day {
                    None => "Unknown regular day".to_string(),
                    Some(day) => format!("Day {day}"),
                },
                TaxaDayType::SuperstarBreak => "Superstar Break".to_string(),
                TaxaDayType::SuperstarGame => "Superstar Game".to_string(),
                TaxaDayType::SuperstarDay => match raw.birthday_superstar_day {
                    None => "Unknown superstar day".to_string(),
                    Some(day) => format!("Superstar Day {day}"),
                },
                TaxaDayType::PostseasonPreview => "Postseason Preview".to_string(),
                TaxaDayType::PostseasonRound1 => "Postseason Round 1".to_string(),
                TaxaDayType::PostseasonRound2 => "Postseason Round 2".to_string(),
                TaxaDayType::PostseasonRound3 => "Postseason Round 3".to_string(),
                TaxaDayType::Election => "Election".to_string(),
                TaxaDayType::Holiday => "Holiday".to_string(),
                TaxaDayType::Event => "Event".to_string(),
                TaxaDayType::SpecialEvent => "Special Event".to_string(),
            },
        };

        Self {
            home: &raw.home,
            first_name: &raw.first_name,
            last_name: &raw.last_name,
            birthday: format!("Season {} {}", raw.birthseason, birthday_day),
            batting_handedness: raw
                .batting_handedness
                .map(|h| taxa.handedness_from_id(h).as_insertable().name),
            pitching_handedness: raw
                .pitching_handedness
                .map(|h| taxa.handedness_from_id(h).as_insertable().name),
            likes: &raw.likes,
            dislikes: &raw.dislikes,
            durability: raw.durability,
            slot: raw.slot.map(|id| taxa.slot_from_id(id).as_insertable().display_name),
        }
    }
}

#[derive(Serialize)]
struct OutcomeStats {
    outcome: &'static str,
    player_count: i64,
    everyone_count: Option<i64>,
}

#[derive(Serialize)]
struct OutcomesSummary {
    outcomes: Vec<OutcomeStats>,
    player_total: i64,
    everyone_total: i64,
}

fn outcomes(outcomes: Option<Vec<Outcome>>, taxa: &Taxa) -> OutcomesSummary {
    let outcomes = outcomes
        .unwrap_or(Vec::new())
        .into_iter()
        .filter_map(|outcome| {
            let et = taxa.event_type_from_id(outcome.event_type);
            if let Some(et) = et {
                let et_info = et.as_insertable();
                if !et_info.ends_plate_appearance {
                    return None;
                }
                Some(OutcomeStats {
                    outcome: et_info.display_name,
                    player_count: outcome.count,
                    everyone_count: None,
                })
            } else {
                Some(OutcomeStats {
                    outcome: "Unrecognized event type",
                    player_count: outcome.count,
                    everyone_count: None,
                })
            }
        })
        .collect_vec();
    let player_total = outcomes
        .iter()
        .map(|stats| stats.player_count)
        .sum();
    let everyone_total = outcomes
        .iter()
        .filter_map(|stats| stats.everyone_count)
        .sum();

    OutcomesSummary {
        outcomes,
        player_total,
        everyone_total,
    }
}

type SeasonStats = HashMap<String, i64>;
type Stats = HashMap<Option<i64>, SeasonStats>;

// TODO Add `at` support, which requires figuring out chrono deserialize from rocket
#[get("/player/<player_id>?<season>")]
pub async fn player(
    player_id: String,
    season: Option<i32>,
    db: Db,
    // mut stats_cache: Connection<StatsCache>,
    taxa: &State<Taxa>,
) -> Result<Template, AppError> {
    let taxa_for_db = (*taxa).clone();
    let player_all = db.run(move |conn|
        db::player_all(conn, &player_id, &taxa_for_db, season)
    ).await?;

    // let combo_stats: Option<Stats> = stats_cache.get("breakdowns").await
    //     .map_err(|err| {
    //         error!("Failed to get breakdowns: {}", err);
    //     })
    //     .ok();

    let raw_clone = player_all.player.clone();
    let player = PlayerContext::from_db(&raw_clone, &taxa);

    let total_events = player_all.pitch_types
        .as_ref()
        .map(|pitches| pitches.iter().map(|info| info.count).sum::<i64>());

    let total_pitches = player_all.pitch_types.as_ref().map(|pitches| {
        pitches
            .iter()
            .map(|info| {
                if info.pitch_type.is_some() {
                    info.count
                } else {
                    0
                }
            })
            .sum::<i64>()
    });

    let balk_id = taxa.event_type_id(TaxaEventType::Balk);
    let total_balks = player_all.pitch_types.as_ref().map(|pitches| {
        pitches
            .iter()
            .map(|info| {
                if info.event_type == balk_id {
                    info.count
                } else {
                    0
                }
            })
            .sum::<i64>()
    });
    let unexpected_non_pitch_events =
        total_events.unwrap_or(0) - total_pitches.unwrap_or(0) - total_balks.unwrap_or(0);

    let pitch_types = player_all.pitch_types.as_ref().map(|pitches| {
        let chunks = pitches
            .iter()
            .filter_map(|info| {
                info.pitch_type.map(|ty| {
                    (
                        taxa.pitch_type_from_id(ty),
                        info.min_speed,
                        info.max_speed,
                        info.count,
                    )
                })
            })
            .chunk_by(|(ty, _, _, _)| *ty);
        chunks
            .into_iter()
            .map(|(ty, chunk)| {
                chunk.into_iter().reduce(
                    |(_, acc_min_speed, acc_max_speed, acc_count),
                     (_, min_speed, max_speed, count)| {
                        (
                            ty,
                            [acc_min_speed, min_speed]
                                .into_iter()
                                .flatten()
                                .reduce(f64::min),
                            [acc_max_speed, max_speed]
                                .into_iter()
                                .flatten()
                                .reduce(f64::max),
                            acc_count + count,
                        )
                    },
                )
            })
            .collect_vec()
    });

    let pitching_outcomes = outcomes(player_all.pitching_outcomes, taxa);
    let fielding_outcomes = outcomes(player_all.fielding_outcomes, taxa);
    let batting_outcomes = outcomes(player_all.batting_outcomes, taxa);

    Ok(Template::render(
        "player",
        context! {
            index_url: uri!(index_page()),
            season,
            player,
            player_raw: player_all.player,
            total_events,
            total_pitches,
            total_balks,
            unexpected_non_pitch_events,
            pitch_types,
            pitching_outcomes,
            fielding_outcomes,
            batting_outcomes,
        },
    ))
}
