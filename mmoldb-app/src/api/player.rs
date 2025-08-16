use chrono::{DateTime, Utc};
use rocket::serde::json::Json;
use rocket::{get, State};
use rocket::serde::Serialize;
use mmoldb_db::taxa::{Taxa, TaxaDayType, TaxaHandedness, TaxaSlot};
use crate::Db;

#[derive(Serialize)]
struct ApiPlayerVersion {
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
    // TODO
    // pub greater_boon: Option<i64>,
    // pub lesser_boon: Option<i64>,
}


#[derive(Serialize)]
struct ApiPlayerVersions<'a> {
    player_id: &'a str,
    versions: Vec<ApiPlayerVersion>,
}

#[get("/player_versions/<player_id>")]
pub async fn player_versions<'a>(player_id: &'a str, db: Db, taxa: &State<Taxa>) -> Json<ApiPlayerVersions<'a>> {
    let mmolb_player_id = player_id.to_string();
    let player_versions = db.run(move |conn| {
        mmoldb_db::db::get_player_versions(conn, &mmolb_player_id)
    }).await.expect("TODO Error handling");

    Json(ApiPlayerVersions {
        player_id,
        versions: player_versions.into_iter()
            .map(|v| ApiPlayerVersion {
                id: v.mmolb_player_id,
                valid_from: v.valid_from.and_utc(),
                valid_until: v.valid_until.map(|dt| dt.and_utc()),
                first_name: v.first_name,
                last_name: v.last_name,
                batting_handedness: v.batting_handedness.map(|h| taxa.handedness_from_id(h)),
                pitching_handedness: v.pitching_handedness.map(|h| taxa.handedness_from_id(h)),
                home: v.home,
                birthseason: v.birthseason,
                birthday_type: v.birthday_type.map(|d| taxa.day_type_from_id(d)),
                birthday_day: v.birthday_day,
                birthday_superstar_day: v.birthday_superstar_day,
                likes: v.likes,
                dislikes: v.dislikes,
                number: v.number,
                mmolb_team_id: v.mmolb_team_id,
                slot: v.slot.map(|s| taxa.slot_from_id(s)),
                durability: v.durability,
            })
            .collect(),
    })
}
