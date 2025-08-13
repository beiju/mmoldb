use super::pages::*;
use crate::Db;
use crate::web::error::AppError;
use include_dir::{Dir, include_dir};
use itertools::Itertools;
use miette::Diagnostic;
use mmoldb_db::db;
use rocket::{get, uri, form::FromForm, State};
use rocket_dyn_templates::{Template, context};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use chrono::NaiveDateTime;
use thiserror::Error;
use rocket::time::PrimitiveDateTime;
use mmoldb_db::models::DbPlayerVersion;
use mmoldb_db::taxa::{AsInsertable, Taxa, TaxaDayType, TaxaEventType};

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
}

impl<'r, 't> PlayerContext<'r, 't> {
    fn from_db(raw: &'r DbPlayerVersion, taxa: &'t Taxa) -> PlayerContext<'r, 't> {
        let birthday_day = match raw.birthday_type {
            None => { "Error storing player's birthday".to_string() }
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
            }
        };

        Self {
            home: &raw.home,
            first_name: &raw.first_name,
            last_name: &raw.last_name,
            birthday: format!("Season {} {}", raw.birthseason, birthday_day),
            batting_handedness: raw.batting_handedness.map(|h| taxa.handedness_from_id(h).as_insertable().name),
            pitching_handedness: raw.pitching_handedness.map(|h| taxa.handedness_from_id(h).as_insertable().name),
            likes: &raw.likes,
            dislikes: &raw.dislikes,
            durability: raw.durability,
        }
    }
}


// TODO Add `at` support, which requires figuring out chrono deserialize from rocket
#[get("/player/<player_id>")]
pub async fn player(player_id: String, db: Db, taxa: &State<Taxa>) -> Result<Template, AppError> {
    let (player_raw, pitches) = db.run(move |conn| {
        db::player_all(conn, &player_id)
    }).await?;

    let raw_clone = player_raw.clone();
    let player = PlayerContext::from_db(&player_raw, &taxa);

    let total_events = pitches.as_ref().map(|pitches| pitches.iter()
        .map(|info| info.count)
        .sum::<i64>()
    );

    let total_pitches = pitches.as_ref().map(|pitches| pitches.iter()
        .map(|info| if info.pitch_type.is_some() { info.count } else { 0 })
        .sum::<i64>()
    );

    let balk_id = taxa.event_type_id(TaxaEventType::Balk);
    let total_balks = pitches.as_ref().map(|pitches| pitches.iter()
        .map(|info| if info.event_type == balk_id { info.count } else { 0 })
        .sum::<i64>()
    );
    let unexpected_non_pitch_events = total_events.unwrap_or(0) - total_pitches.unwrap_or(0) - total_balks.unwrap_or(0);

    let pitch_types = pitches.as_ref().map(|pitches| {
        let chunks = pitches.iter()
            .filter_map(|info| info.pitch_type.map(|ty| (taxa.pitch_type_from_id(ty), info.min_speed, info.max_speed, info.count)))
            .chunk_by(|(ty, _, _, _)| *ty);
        chunks
            .into_iter()
            .map(|(ty, chunk)| {
                chunk
                    .into_iter()
                    .reduce(|(_, acc_min_speed, acc_max_speed, acc_count), (_, min_speed, max_speed, count)| (
                        ty,
                        [acc_min_speed, min_speed].into_iter().flatten().reduce(f64::min),
                        [acc_max_speed, max_speed].into_iter().flatten().reduce(f64::max),
                        acc_count + count,
                    ))
            })
            .collect_vec()
    });

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
        },
    ))
}