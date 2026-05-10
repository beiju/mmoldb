pub mod db;
mod migrations;
pub mod models;
mod parsing_extensions;
mod pool;
mod schema;
pub mod taxa;
mod url;

pub mod async_db;
mod event_detail;

pub(crate) use schema::*;

pub use db::DbMetaQueryError;
pub use event_detail::*;
pub use migrations::*;
pub use parsing_extensions::*;
pub use pool::*;
pub use url::*;

pub use diesel::{
    Connection, PgConnection, QueryResult, result::ConnectionError, result::Error as QueryError,
};
pub use diesel_async::{AsyncConnection, AsyncPgConnection};

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use one_au::OneAu;
    use crate::models::{NewTeamPlayerVersion, NewTeamVersion, NewVersionProcessed};
    use super::*;

    #[test]
    fn teams_works() {
        let url = postgres_url_from_environment();
        let mut conn = PgConnection::establish(&url)
            .expect("postgres_url_from_environment should return a valid connection string");

        let entity_id = "dummy-team-entity-id";
        let mut valid_from = Utc::now();

        conn.test_transaction(|mut conn| {
            // 1. Insert a reference record
            let processed = NewVersionProcessed {
                kind: "team",
                entity_id,
                valid_from: valid_from.naive_utc(),
                skipped: false,
                fatal_error: false,
            };
            let mut team_version = NewTeamVersion::default();
            team_version.valid_from = valid_from.naive_utc();
            team_version.num_players = 1; // Don't close out the team player version we're inserting
            let mut team_player_version = NewTeamPlayerVersion::default();
            team_player_version.valid_from = valid_from.naive_utc();

            let mut reference_team = (
                processed,
                Some(team_version),
                vec![team_player_version],
                Vec::new(), // Ignore ingest logs
            );
            let (total, inserted) = db::insert_team_versions_all(&mut conn, vec![&reference_team])?;
            assert_eq!(total, 3, "Should have provided 3 records");
            assert_eq!(inserted, 3, "Should have inserted both provided records");

            // 2. Insert same version with new date, expect only the processed row to be added
            valid_from += chrono::Duration::seconds(1);
            reference_team.0.valid_from = valid_from.naive_utc();
            reference_team.1.as_mut().unwrap().valid_from = valid_from.naive_utc();
            reference_team.2.first_mut().unwrap().valid_from = valid_from.naive_utc();
            let (total, inserted) = db::insert_team_versions_all(&mut conn, vec![&reference_team])?;
            assert_eq!(total, 3, "Should have provided 3 records");
            assert_eq!(inserted, 1, "Should have only inserted `processed`, others should be duplicates");

            // 3. Iterate through fields, insert the record with a modified version of that field, expect 1 row added
            for field in <NewTeamVersion as OneAu>::fields() {
                match field {
                    <NewTeamVersion as OneAu>::Field::mmolb_team_id |
                    <NewTeamVersion as OneAu>::Field::valid_from |
                    <NewTeamVersion as OneAu>::Field::valid_until => { continue; }
                    _ => {}
                }

                valid_from += chrono::Duration::seconds(1);
                reference_team.0.valid_from = valid_from.naive_utc();
                reference_team.1.as_mut().unwrap().valid_from = valid_from.naive_utc();
                reference_team.2.first_mut().unwrap().valid_from = valid_from.naive_utc();

                *reference_team.1.as_mut().unwrap() = reference_team.1.clone().unwrap().au(field);

                let (total, inserted) = db::insert_team_versions_all(&mut conn, vec![&reference_team])?;
                assert_eq!(total, 3, "After modifying {:?}, should have provided 3 records", field);
                assert_eq!(inserted, 2, "After modifying {:?}, should have inserted `processed` and `team_version`", field);
            }

            Ok::<_, diesel::result::Error>(())
        })
    }
}