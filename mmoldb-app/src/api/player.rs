use chrono::{DateTime, Utc};
use itertools::Itertools;
use log::warn;
use rocket::serde::json::Json;
use rocket::{get, State};
use rocket::serde::Serialize;
use mmoldb_db::taxa::{Taxa, TaxaDayType, TaxaHandedness, TaxaSlot};
use hashbrown::HashMap;
use crate::api::error::ApiError;
use crate::Db;

#[derive(Clone, Serialize)]
pub struct ApiModification {
    pub name: String,
    pub emoji: String,
    pub description: String,
}

#[derive(Clone, Serialize)]
pub struct ApiPlayerVersion {
    pub id: String,
    pub valid_from: DateTime<Utc>,
    pub valid_until: Option<DateTime<Utc>>,
    pub first_name: String,
    pub last_name: String,
    pub batting_handedness: Option<TaxaHandedness>,
    pub pitching_handedness: Option<TaxaHandedness>,
    pub home: String,
    pub birthseason: i32,
    pub birthday_type: Option<TaxaDayType>,
    pub birthday_day: Option<i32>,
    pub birthday_superstar_day: Option<i32>,
    pub likes: String,
    pub dislikes: String,
    pub number: i32,
    pub mmolb_team_id: Option<String>,
    pub slot: Option<TaxaSlot>,
    pub durability: f64,
    pub greater_boon: Option<ApiModification>,
    pub lesser_boon: Option<ApiModification>,
    pub modifications: Vec<Option<ApiModification>>,
}


#[derive(Serialize)]
pub struct ApiPlayerVersions<'a> {
    pub player_id: &'a str,
    pub versions: Vec<ApiPlayerVersion>,
}

struct NextChangeTime(Option<DateTime<Utc>>);

impl NextChangeTime {
    pub fn new() -> Self {
        Self(None)
    }

    pub fn with_change(&mut self, time: DateTime<Utc>) {
        if let Some(t) = &mut self.0 {
            if (time < *t) {
                *t = time;
            }
        } else {
            self.0 = Some(time);
        }
    }

    pub fn with_possible_change(&mut self, time: Option<DateTime<Utc>>) {
        if let Some(time) = time {
            self.with_change(time);
        }
    }

    pub fn into_inner(self) -> Option<DateTime<Utc>> {
        self.0
    }
}

#[get("/player_versions/<player_id>")]
pub async fn player_versions<'a>(player_id: &'a str, db: Db, taxa: &State<Taxa>) -> Result<Json<ApiPlayerVersions<'a>>, ApiError> {
    let mmolb_player_id = player_id.to_string();
    let (player_versions, player_modification_versions, modifications) = db.run(move |conn| {
        let players = mmoldb_db::db::get_player_versions(conn, &mmolb_player_id)?;
        let player_modifications = mmoldb_db::db::get_player_modification_versions(conn, &mmolb_player_id)?;

        let mod_ids = player_modifications.iter()
            .map(|pm| pm.modification_id)
            .chain(
                players.iter()
                    .map(|pm| pm.greater_boon)
                    .flatten()
            )
            .chain(
                players.iter()
                    .map(|pm| pm.lesser_boon)
                    .flatten()
            )
            .collect_vec();
        let modifications = mmoldb_db::db::get_modifications(conn, &mod_ids)?;

        Ok::<_, ApiError>((players, player_modifications, modifications))
    }).await.expect("TODO Error handling");

    let modifications_table: HashMap<_, _> = modifications.into_iter()
        .map(|m| (m.id, ApiModification {
            name: m.name,
            emoji: m.emoji,
            description: m.description,
        }))
        .collect();

    let mut next_player_version = player_versions.into_iter().peekable();
    let mut next_player_modification_version = player_modification_versions.into_iter().peekable();

    let mut versions = Vec::new();
    let mut active_player = None;
    let mut modifications: Vec<Option<ApiModification>> = Vec::new();
    loop {
        let mut next_change_time = NextChangeTime::new();
        next_change_time.with_possible_change(next_player_version.peek().map(|v| v.valid_from.and_utc()));
        next_change_time.with_possible_change(next_player_modification_version.peek().map(|v| v.valid_from.and_utc()));

        let Some(time) = next_change_time.into_inner() else {
            break;
        };

        // TODO Emit new version for any closed modifications

        while let Some(player) = next_player_version.next_if(|p| p.valid_from.and_utc() == time) {
            active_player = Some(player);
        }

        let Some(player) = &mut active_player else {
            warn!("A player child table entry became valid before the first player entry");
            continue;
        };

        modifications.resize(player.num_modifications as usize, None);
        while let Some(modif) = next_player_modification_version.next_if(|p| p.valid_from.and_utc() == time) {
            let modif_index = modif.modification_index as usize;

            if let Some(elem) = modifications.get_mut(modif_index) {
                if let Some(api_mod) = modifications_table.get(&modif.modification_id) {
                    *elem = Some(api_mod.clone());
                } else {
                    warn!("Unrecognized modification id {}", modif.modification_id);
                    *elem = None;
                }
            } else {
                warn!(
                    "player_modifications table had more modifications than player \
                    num_modifications indicated"
                );
            }
        }

        // Fill active_modifications according to
        versions.push(ApiPlayerVersion {
            id: player.mmolb_player_id.clone(),
            valid_from: player.valid_from.and_utc(),
            valid_until: player.valid_until.map(|dt| dt.and_utc()),
            first_name: player.first_name.clone(),
            last_name: player.last_name.clone(),
            batting_handedness: player.batting_handedness.map(|h| taxa.handedness_from_id(h)),
            pitching_handedness: player.pitching_handedness.map(|h| taxa.handedness_from_id(h)),
            home: player.home.clone(),
            birthseason: player.birthseason,
            birthday_type: player.birthday_type.map(|d| taxa.day_type_from_id(d)),
            birthday_day: player.birthday_day,
            birthday_superstar_day: player.birthday_superstar_day,
            likes: player.likes.clone(),
            dislikes: player.dislikes.clone(),
            number: player.number,
            mmolb_team_id: player.mmolb_team_id.clone(),
            slot: player.slot.map(|s| taxa.slot_from_id(s)),
            durability: player.durability,
            greater_boon: player.greater_boon.and_then(|m| {
                if let Some(api_mod) = modifications_table.get(&m) {
                    Some(api_mod.clone())
                } else {
                    warn!("Unrecognized greater boon id {}", m);
                    None
                }
            }),
            lesser_boon: player.lesser_boon.and_then(|m| {
                if let Some(api_mod) = modifications_table.get(&m) {
                    Some(api_mod.clone())
                } else {
                    warn!("Unrecognized lesser boon id {}", m);
                    None
                }
            }),
            modifications: modifications.clone(),
        });
    }

    Ok(Json(ApiPlayerVersions {
        player_id,
        versions,
    }))
}
