use diesel::sql_types::Json;
use rocket::{get, uri};
use rocket_dyn_templates::{context, Template};
use crate::{db, Db};
use crate::web::error::AppError;
use super::pages::*;

#[get("/docs")]
pub async fn docs_page() -> Template {
    Template::render(
        "docs",
        context! {
            index_url: uri!(index_page()),
            status_url: uri!(status_page()),
            docs_url: uri!(docs_page()),
        },
    )
}

#[get("/docs/debug")]
pub async fn docs_debug_page(db: Db) -> Result<Template, AppError> {
    let data = db.run(|conn| {
        db::docs_debug(conn)
    }).await?;

    Ok(Template::render(
        "json_renderer",
        context! {
                data: data,
            },
    ))
}
