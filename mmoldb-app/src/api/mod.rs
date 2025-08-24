mod error;
mod player;

#[rocket::get("/")]
pub async fn index() -> &'static str {
    "This is the MMOLDB API. Documentation is possibly forthcoming."
}

pub fn routes() -> Vec<rocket::Route> {
    rocket::routes![index, player::player_versions,]
}
