mod error;
mod pages;
mod utility_contexts;

pub fn routes() -> Vec<rocket::Route> {
    rocket::routes![
        pages::index_page,
        pages::games_page,
        pages::games_with_issues_page,
        pages::debug_no_games_page,
        pages::ingest_page,
        pages::game_page,
        pages::debug_always_error_page,
        pages::debug_memory_profile,
    ]
}
