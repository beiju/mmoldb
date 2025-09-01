use super::pages::*;
use crate::Db;
use crate::web::error::AppError;
use itertools::Itertools;
use mmoldb_db::db;
use mmoldb_db::models::DbPlayerVersion;
use mmoldb_db::taxa::{AsInsertable, Taxa, TaxaDayType, TaxaEventType};
use rocket::{State, get, uri};
use rocket_dyn_templates::{Template, context};
use serde::Serialize;
use bigdecimal::{BigDecimal, ToPrimitive};

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

// TODO Add `at` support, which requires figuring out chrono deserialize from rocket
#[get("/player/<player_id>")]
pub async fn player(player_id: String, db: Db, taxa: &State<Taxa>) -> Result<Template, AppError> {
    let taxa_for_db = (*taxa).clone();
    let (player_raw, pitches, batting_outcomes, fielding_outcomes) = db.run(move |conn|
        db::player_all(conn, &player_id, &taxa_for_db)
    ).await?;

    let raw_clone = player_raw.clone();
    let player = PlayerContext::from_db(&player_raw, &taxa);

    let total_events = pitches
        .as_ref()
        .map(|pitches| pitches.iter().map(|info| info.count).sum::<i64>());

    let total_pitches = pitches.as_ref().map(|pitches| {
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
    let total_balks = pitches.as_ref().map(|pitches| {
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

    let pitch_types = pitches.as_ref().map(|pitches| {
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

    let batting_outcomes = batting_outcomes
        .unwrap_or(Vec::new())
        .into_iter()
        .filter_map(|(et_id, all_count, player_count)| {
            let player_count = player_count.map_or(0, |count| count.to_i64().unwrap());
            let et = taxa.event_type_from_id(et_id);
            if let Some(et) = et {
                let et_info = et.as_insertable();
                if !et_info.ends_plate_appearance {
                    return None;
                }
                Some((et_info.display_name, all_count, player_count))
            } else {
                Some(("Unrecognized event type", all_count, player_count))
            }
        })
        .collect_vec();
    let total_batting_outcomes = batting_outcomes
        .iter()
        .map(|(_, _, count)| *count)
        .sum::<i64>();
    let total_batting_outcomes_all = batting_outcomes
        .iter()
        .map(|(_, count_all, _)| *count_all)
        .sum::<i64>();

    let fielding_outcomes = fielding_outcomes
        .unwrap_or(Vec::new())
        .into_iter()
        .map(|(et_id, all_count, player_count)| {
            let player_count = player_count.map_or(0, |count| count.to_i64().unwrap());
            let et = taxa.event_type_from_id(et_id);
            if let Some(et) = et {
                let et_info = et.as_insertable();
                (et_info.display_name, all_count, player_count)
            } else {
                ("Unrecognized event type", all_count, player_count)
            }
        })
        .collect_vec();
    let total_fielding_outcomes = fielding_outcomes
        .iter()
        .map(|(_, _, count)| *count)
        .sum::<i64>();
    let total_fielding_outcomes_all = fielding_outcomes
        .iter()
        .map(|(_, count_all, _)| *count_all)
        .sum::<i64>();

    Ok(Template::render(
        "player",
        context! {
            index_url: uri!(index_page()),
            player,
            player_raw: raw_clone,
            total_events,
            total_pitches,
            total_balks,
            unexpected_non_pitch_events,
            pitch_types,
            total_batting_outcomes,
            total_batting_outcomes_all,
            batting_outcomes,
            total_fielding_outcomes,
            total_fielding_outcomes_all,
            fielding_outcomes,
        },
    ))
}
