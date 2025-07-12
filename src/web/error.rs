use log::error;
use miette::Diagnostic;
use rocket::http::Status;
use rocket::response::Responder;
use rocket::{Request, Response, uri};
use rocket_dyn_templates::{Template, context};
use thiserror::Error;
use crate::web::docs_pages::DocsError;
use crate::web::pages::rocket_uri_macro_index_page;

// TODO This should probably be some miette bullshit
#[derive(Debug, Error, Diagnostic)]
pub enum AppError {
    #[error("This URL produces a test error")]
    TestError,

    #[error(transparent)]
    DbError(#[from] diesel::result::Error),

    #[error(transparent)]
    DbMetaQueryError(#[from] crate::db::DbMetaQueryError),

    #[error(transparent)]
    DocsError(#[from] DocsError),
}

impl<'r, 'o: 'r> Responder<'r, 'o> for AppError {
    fn respond_to(self, req: &'r Request<'_>) -> rocket::response::Result<'o> {
        error!("{:#?}", self);

        let is_debug = req.rocket().config().profile == "debug";

        let rendered = Template::show(
            req.rocket(),
            "error",
            context! {
                index_url: uri!(index_page()),
                error_text: format!("{:}", self),
                error_debug: if is_debug { Some(format!("{:?}", self)) } else { None },
            },
        )
        .unwrap();

        Response::build()
            .status(Status::InternalServerError)
            .header(rocket::http::ContentType::HTML)
            .sized_body(rendered.len(), std::io::Cursor::new(rendered))
            .ok()
    }
}
