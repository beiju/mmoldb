mod db;
mod ingest;
mod models;
mod parsing_extensions;
mod web;

use crate::ingest::{IngestFairing, IngestTask};
use num_format::{Locale, ToFormattedString};
use rocket::fairing::AdHoc;
use rocket::figment::map;
use rocket::{figment, launch, Build, Rocket};
use rocket_dyn_templates::Template;
use rocket_dyn_templates::tera::Value;
use rocket_sync_db_pools::database;
use rocket_sync_db_pools::diesel::{prelude::*, PgConnection};
use serde::Deserialize;
use std::collections::HashMap;
#[database("mmoldb")]
struct Db(PgConnection);

struct NumFormat;

impl rocket_dyn_templates::tera::Filter for NumFormat {
    fn filter(
        &self,
        value: &Value,
        _args: &HashMap<String, Value>,
    ) -> rocket_dyn_templates::tera::Result<Value> {
        if let Value::Number(num) = value {
            if let Some(n) = num.as_i64() {
                return Ok(n.to_formatted_string(&Locale::en).into());
            }
        }

        Ok(value.clone())
    }
}

async fn run_migrations(rocket: Rocket<Build>) -> Rocket<Build> {
    use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};

    const MIGRATIONS: EmbeddedMigrations = embed_migrations!("../migrations");
    let config: rocket_sync_db_pools::Config = rocket
        .figment()
        .extract_inner("databases.mmoldb")
        .expect("mmoldb database connection information was not found in Rocket.toml");

    tokio::task::spawn_blocking(move || {
        PgConnection::establish(&config.url)
            .expect("Failed to connect to mmoldb database during migrations")
            .run_pending_migrations(MIGRATIONS)
            .expect("Failed to apply migrations");
    })
    .await
    .expect("Error joining migrations task");

    rocket
}

fn get_figment_with_constructed_db_url() -> figment::Figment {
    let url = mmoldb_db::postgres_url_from_environment();
    rocket::Config::figment().merge(("databases", map!["mmoldb" => map!["url" => url]]))
}

#[launch]
fn rocket() -> _ {
    rocket::custom(get_figment_with_constructed_db_url())
        .mount("/", web::routes())
        .mount("/static", rocket::fs::FileServer::from("./static"))
        .manage(IngestTask::new())
        .attach(Template::custom(|engines| {
            engines.tera.register_filter("num_format", NumFormat);
        }))
        .attach(Db::fairing())
        .attach(AdHoc::on_ignite("Migrations", run_migrations))
        .attach(IngestFairing::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    pub async fn get_db() -> Db {
        let config = get_figment_with_constructed_db_url()
            .merge(("port", openport::pick_random_unused_port()))
            .merge(("databases", map!["mmoldb" => map!["pool_size" => 3]]));

        let rocket = rocket::custom(config)
            .attach(Db::fairing())
            .ignite()
            .await
            .expect("Rocket failed to ignite");

        Db::get_one(&rocket).await
            .expect("Failed to get a database connection")
    }

    #[tokio::test]
    async fn connect_to_db() {
        let db = get_db().await;

        db.run(|_| ()).await;
    }
}