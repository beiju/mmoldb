mod error;
mod pages;
mod utility_contexts;
mod docs_pages;

pub fn routes() -> Vec<rocket::Route> {
    rocket::routes![
        pages::index_page,
        pages::status_page,
        docs_pages::docs_page,
        docs_pages::docs_schema_page,
        docs_pages::docs_debug_page,
        pages::games_page,
        pages::paginated_games_page,
        pages::games_with_issues_page,
        pages::paginated_games_with_issues_page,
        pages::debug_no_games_page,
        pages::ingest_page,
        pages::paginated_ingest_page,
        pages::game_page,
        pages::debug_always_error_page,
    ]
}
