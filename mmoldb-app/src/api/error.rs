use log::error;
use miette::Diagnostic;
use rocket::http::Status;
use rocket::response::Responder;
use rocket::{Request, Response};
use thiserror::Error;

#[derive(Debug, Error, Diagnostic)]
pub enum ApiError {
    #[error(transparent)]
    DbError(#[from] diesel::result::Error),
}

impl<'r, 'o: 'r> Responder<'r, 'o> for ApiError {
    fn respond_to(self, _: &'r Request<'_>) -> rocket::response::Result<'o> {
        let rendered = self.to_string();

        Response::build()
            .status(Status::InternalServerError)
            .header(rocket::http::ContentType::JSON)
            .sized_body(rendered.len(), std::io::Cursor::new(rendered))
            .ok()
    }
}
