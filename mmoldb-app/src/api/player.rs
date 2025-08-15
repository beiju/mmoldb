use rocket::serde::json::Json;
use rocket::get;
use rocket::serde::Serialize;

#[derive(Serialize)]
struct PlayerVersionsFull {

}

#[get("/player_versions/<player_id>")]
pub async fn player_versions(player_id: &str) -> Json<PlayerVersionsFull> {
    Json(PlayerVersionsFull {})
}
