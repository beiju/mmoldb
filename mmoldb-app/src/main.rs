mod api;
mod web;

use num_format::{Locale, ToFormattedString};
use rocket::fairing::AdHoc;
use rocket::figment::map;
use rocket::{Build, Rocket, figment, launch};
use rocket_dyn_templates::Template;
use rocket_dyn_templates::tera::Value;
use rocket_sync_db_pools::database as sync_database;
use rocket_sync_db_pools::diesel::{PgConnection};
use std::collections::HashMap;

#[sync_database("mmoldb")]
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
    let taxa = tokio::task::spawn_blocking(move || {
        mmoldb_db::run_migrations()
    })
        .await
        .expect("Error joining migrations task")
        .expect("Error running migrations");

    rocket.manage(taxa)
}

fn get_figment_with_constructed_db_url() -> figment::Figment {
    let url = mmoldb_db::postgres_url_from_environment();
    rocket::Config::figment().merge(("databases", map!["mmoldb" => map!["url" => url]]))
}

#[launch]
fn rocket() -> _ {
    let cors = rocket_cors::CorsOptions::default()
        .to_cors()
        .expect("CORS specification should be valid");
    rocket::custom(get_figment_with_constructed_db_url())
        .attach(cors)
        .mount("/", web::routes())
        .mount("/api", api::routes())
        .mount("/static", rocket::fs::FileServer::from("./static"))
        .attach(Template::custom(|engines| {
            engines.tera.register_filter("num_format", NumFormat);
        }))
        .attach(Db::fairing())
        .attach(AdHoc::on_ignite("Migrations", run_migrations))
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

        Db::get_one(&rocket)
            .await
            .expect("Failed to get a database connection")
    }

    #[tokio::test]
    async fn connect_to_db() {
        let db = get_db().await;

        db.run(|_| ()).await;
    }
}
