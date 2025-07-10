mod db;
#[rustfmt::skip] // This is a generated file
mod data_schema;
#[rustfmt::skip] // This is a generated file
mod taxa_schema;
#[rustfmt::skip] // This is a generated file
mod info_schema;
mod ingest;
mod models;
mod parsing_extensions;
mod web;
mod meta_schema;

use crate::ingest::{IngestFairing, IngestTask};
use num_format::{Locale, ToFormattedString};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use rocket::fairing::AdHoc;
use rocket::figment::map;
use rocket::{Build, Rocket, figment, launch};
use rocket_dyn_templates::Template;
use rocket_dyn_templates::tera::Value;
use rocket_sync_db_pools::database;
use rocket_sync_db_pools::diesel::{PgConnection, prelude::*};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;

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

    const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");
    let config: rocket_sync_db_pools::Config = rocket
        .figment()
        .extract_inner("databases.mmoldb")
        .expect("mmoldb database connection information was not found in Rocket.toml");

    rocket::tokio::task::spawn_blocking(move || {
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
    #[derive(Debug, PartialEq, Deserialize)]
    struct PostgresConfig {
        user: String,
        password: Option<String>,
        password_file: Option<PathBuf>,
        db: String,
    }
    let provider = figment::providers::Env::prefixed("POSTGRES_");
    let postgres_config: PostgresConfig = figment::Figment::from(provider)
        .extract()
        .expect("Postgres configuration environment variable(s) missing or invalid");

    let password = if let Some(password) = postgres_config.password {
        password
    } else if let Some(password_file) = postgres_config.password_file {
        std::fs::read_to_string(password_file).expect("Failed to read postgres password file")
    } else {
        panic!("One of POSTGRES_PASSWORD or POSTGRES_PASSWORD_FILE must be provided");
    };

    // Postgres (or something else in my Postgres pipeline) will _truncate_ the
    // password at the first newline. I don't want to mimic that behavior,
    // because it could lead to people using vastly less secure passwords than
    // they intended to. I can trim a trailing newline without losing (a
    // meaningful amount of) entropy, which I will do because the trailing
    // newline convention is so strong the user might not even realize they
    // have one. But if there are any other newlines in the string, I just exit
    // with an error.
    let password = if let Some(pw) = password.strip_suffix("\n") {
        pw
    } else {
        &password
    };

    if password.contains("\n") {
        // Print this error in the most direct way to maximize the chances that the
        // user can figure out what's going on
        eprintln!(
            "Postgres admin password contains a non-terminal newline. This password will be \
            insecurely truncated. Please try again with a password that does not contain non-\
            terminal newlines."
        );
        // Also panic with the same message
        panic!(
            "Postgres admin password contains a non-terminal newline. This password will be \
            insecurely truncated. Please try again with a password that does not contain non-\
            terminal newlines."
        );
    }

    // Must percent encode password.
    // The return type of utf8_percent_encode implements Display so we can skip the to_string call
    // and provide it directly to the format!().
    let password = utf8_percent_encode(&password, NON_ALPHANUMERIC);

    let url = format!(
        "postgres://{}:{}@db/{}",
        postgres_config.user, password, postgres_config.db
    );
    rocket::Config::figment().merge(("databases", map!["mmoldb" => map!["url" => url]]))
}

#[launch]
fn rocket() -> _ {
    rocket::custom(get_figment_with_constructed_db_url())
        .mount("/", web::routes())
        .mount("/static", rocket::fs::FileServer::from("static"))
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