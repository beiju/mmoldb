use crate::chron::{Chron, ChronEntity};
use chrono::NaiveDateTime;
use diesel::{Connection, PgConnection};
use futures::{pin_mut, StreamExt, TryStreamExt};
use log::info;
use miette::IntoDiagnostic;

mod chron;
mod db2;
mod shared;

mod data_schema;

const CHRON_FETCH_PAGE_SIZE: usize = 1000;

#[tokio::main]
async fn main() -> Result<(), Box<dyn miette::Diagnostic>> {
    env_logger::init();

    let url = shared::postgres_url_from_environment();

    ingest_games(&url).await
}

async fn ingest_games(url: &str) -> Result<(), Box<dyn miette::Diagnostic>> {
    let mut conn = PgConnection::establish(url).into_diagnostic()?;

    let start_date = db2::get_latest_entity_valid_from(&mut conn, "game")
        .into_diagnostic()?
        .as_ref()
        .map(NaiveDateTime::and_utc);

    info!("Fetch will start at {:?}", start_date);

    let chron = Chron::new(false, "", CHRON_FETCH_PAGE_SIZE).into_diagnostic()?;

    let stream = chron
        .entities("game", start_date)
        .try_chunks(CHRON_FETCH_PAGE_SIZE);
    pin_mut!(stream);

    while let Some(chunk) = stream.next().await {
        let (chunk, maybe_err): (Vec<ChronEntity<serde_json::Value>>, _) = match chunk {
            Ok(chunk) => (chunk, None),
            Err(err) => (err.0, Some(err.1)),
        };
        info!("Saving {} games", chunk.len());
        let inserted = db2::insert_entities(&mut conn, chunk).into_diagnostic()?;
        info!("Saved {} games", inserted);

        if let Some(err) = maybe_err {
            Err(err)?;
        }
    }

    Ok(())
}
