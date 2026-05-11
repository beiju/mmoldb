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
    use crate::models::{NewPlayerEquipmentEffectVersion, NewPlayerEquipmentVersion, NewPlayerModificationVersion, NewPlayerPitchCategoryBonusVersion, NewPlayerPitchTypeBonusVersion, NewPlayerPitchTypeVersion, NewPlayerReportAttributeVersion, NewPlayerReportVersion, NewPlayerVersion, NewTeamPlayerVersion, NewTeamVersion, NewVersionProcessed};
    use super::*;

    fn team_increment_valid_from(team: &mut db::NewTeamVersionExt) {
        let new_date = team.0.valid_from + chrono::Duration::seconds(1);
        team.0.valid_from = new_date;
        if let Some(team_version) = team.1.as_mut() {
            team_version.valid_from = new_date;
        }
        for team_player_version in &mut team.2 {
            team_player_version.valid_from = new_date;
        }
        assert!(team.3.is_empty());
    }

    #[test]
    fn teams_duplicate_detection() {
        let url = postgres_url_from_environment();
        let mut conn = PgConnection::establish(&url)
            .expect("postgres_url_from_environment should return a valid connection string");

        let valid_from = Utc::now();

        conn.test_transaction(|mut conn| {
            // 1. Insert a reference record
            let processed = NewVersionProcessed {
                kind: "team",
                entity_id: "",
                valid_from: valid_from.naive_utc(),
                skipped: false,
                fatal_error: false,
            };
            let mut team_version = NewTeamVersion::default();
            team_version.valid_from = valid_from.naive_utc();
            team_version.num_players = 1; // Don't close out the team player version we're inserting
            let mut team_player_version = NewTeamPlayerVersion::default();
            team_player_version.valid_from = valid_from.naive_utc();

            let mut team = (
                processed,
                Some(team_version),
                vec![team_player_version],
                Vec::new(), // Ignore ingest logs
            );
            let (total, inserted) = db::insert_team_versions_all(&mut conn, vec![&team])?;
            assert_eq!(total, 3, "We provided 3 total records");
            assert_eq!(inserted, 3, "Should have inserted all provided records");

            // 2. Insert same version with new date, expect only the processed row to be added
            team_increment_valid_from(&mut team);
            let (total, inserted) = db::insert_team_versions_all(&mut conn, vec![&team])?;
            assert_eq!(total, 3, "We provided 3 total records");
            assert_eq!(inserted, 1, "Should have only inserted `processed`, others should be duplicates");

            // 3. Iterate through fields, insert the record with a modified version of that field, expect 1 row added
            for field in <NewTeamVersion as OneAu>::fields() {
                match field {
                    <NewTeamVersion as OneAu>::Field::mmolb_team_id |
                    <NewTeamVersion as OneAu>::Field::valid_from |
                    <NewTeamVersion as OneAu>::Field::valid_until => { continue; }
                    _ => {}
                }

                *team.1.as_mut().unwrap() = team.1.clone().unwrap().au(field);

                team_increment_valid_from(&mut team);
                let (total, inserted) = db::insert_team_versions_all(&mut conn, vec![&team])?;
                assert_eq!(total, 3, "After modifying NewTeamVersion::{:?}, we provided 3 total records", field);
                assert_eq!(inserted, 2, "After modifying NewTeamVersion::{:?}, should have inserted `processed` and `team_version`", field);
            }

            team_player_version_duplicate_detection(&mut conn, &mut team)?;

            Ok::<_, diesel::result::Error>(())
        })
    }

    fn team_player_version_duplicate_detection(conn: &mut PgConnection, team: &mut db::NewTeamVersionExt) -> Result<(), diesel::result::Error> {
        // 4. Insert a team version that closes out the one (1) team player version
        // Don't insert a team player version, insert_team_versions_all makes no
        // ordering guarantees so it might either be inserted and immediately closed
        // out or it might be inserted after the previous version is closed out
        let team_player_version = team.2.pop().unwrap();
        team.1.as_mut().unwrap().num_players = 0;
        team_increment_valid_from(team);
        let (total, inserted) = db::insert_team_versions_all(conn, vec![&*team])?;
        assert_eq!(total, 2, "We provided 2 total records");
        assert_eq!(inserted, 2, "Should have inserted `processed` and `team_version`");

        // 5. Re-insert the same team player version, which should be inserted even
        // though it's identical to the previous version because the previous version
        // was closed out
        team.2.push(team_player_version);
        team.1.as_mut().unwrap().num_players = 1;
        team_increment_valid_from(team);
        let (total, inserted) = db::insert_team_versions_all(conn, vec![&*team])?;
        assert_eq!(total, 3, "We provided 3 total records");
        assert_eq!(inserted, 3, "Should have inserted all 3 records");

        // 6. Iterate through team player version fields, insert the record with a modified
        // version of that field, expect 1 row added
        for field in <NewTeamPlayerVersion as OneAu>::fields() {
            // Ignore fields that are part of identification and versioning
            match field {
                <NewTeamPlayerVersion as OneAu>::Field::mmolb_team_id |
                <NewTeamPlayerVersion as OneAu>::Field::team_player_index |
                <NewTeamPlayerVersion as OneAu>::Field::valid_from |
                <NewTeamPlayerVersion as OneAu>::Field::valid_until => { continue; }
                _ => {}
            }

            *team.2.first_mut().unwrap() = team.2.first().unwrap().clone().au(field);

            team_increment_valid_from(team);
            let (total, inserted) = db::insert_team_versions_all(conn, vec![&*team])?;
            assert_eq!(total, 3, "After modifying NewTeamPlayerVersion::{:?}, we provided 3 total records", field);
            assert_eq!(inserted, 2, "After modifying NewTeamPlayerVersion::{:?}, should have inserted `processed` and `team_player_version`", field);
        }

        Ok(())
    }

    fn player_increment_valid_from(player: &mut db::NewPlayerVersionExt) {
        let new_date = player.0.valid_from + chrono::Duration::seconds(1);
        player.0.valid_from = new_date;
        if let Some(player_version) = player.1.as_mut() {
            player_version.valid_from = new_date;
        }
        for player_modification_version in &mut player.2 {
            player_modification_version.valid_from = new_date;
        }
        for player_report_version in &mut player.3 {
            player_report_version.0.valid_from = new_date;
            for player_report_attribute_version in &mut player_report_version.1 {
                player_report_attribute_version.valid_from = new_date;
            }
        }
        for player_equipment_version in &mut player.4 {
            player_equipment_version.0.valid_from = new_date;
            for player_equipment_effect_version in &mut player_equipment_version.1 {
                player_equipment_effect_version.valid_from = new_date;
            }
        }
        for player_pitch_type_version in &mut player.5 {
            player_pitch_type_version.valid_from = new_date;
        }
        for player_pitch_type_bonus_version in &mut player.6 {
            player_pitch_type_bonus_version.valid_from = new_date;
        }
        for player_pitch_category_bonus_version in &mut player.7 {
            player_pitch_category_bonus_version.valid_from = new_date;
        }
        assert!(player.8.is_empty());
    }

    #[test]
    fn players_duplicate_detection() {
        // TODO: This is way, way, way too copy-pasted. Needs to be much more generic
        let url = postgres_url_from_environment();
        let mut conn = PgConnection::establish(&url)
            .expect("postgres_url_from_environment should return a valid connection string");

        let valid_from = Utc::now();

        conn.test_transaction(|mut conn| {
            // 0. Get a valid modification ID, otherwise the database will shout at us about a foreign key constraint
            let modification_id = db::insert_modifications(conn, &[&("", "", "")])?
                .expect("DB should not already have an empty modification")
                .first()
                .expect("insert_modifications should return a vec with the same length as the slice it was given, or None")
                .1;

            // 1. Insert a reference record
            let processed = NewVersionProcessed {
                kind: "player",
                entity_id: "",
                valid_from: valid_from.naive_utc(),
                skipped: false,
                fatal_error: false,
            };
            let mut player_version = NewPlayerVersion::default();
            player_version.valid_from = valid_from.naive_utc();
            player_version.num_modifications = 1; // Don't close out the player modification version we're inserting
            player_version.included_report_categories = vec![0]; // Don't close out the player report version we're inserting
            player_version.occupied_equipment_slots = vec![""]; // Don't close out the player equipment version we're inserting
            player_version.num_pitch_types = 1; // Don't close out the player pitch type version we're inserting
            player_version.included_pitch_type_bonuses = vec![1]; // Don't close out the player pitch type bonus version we're inserting
            player_version.included_pitch_category_bonuses = vec![1]; // Don't close out the player pitch category bonus version we're inserting
            let mut player_modification_version = NewPlayerModificationVersion::default();
            player_modification_version.valid_from = valid_from.naive_utc();
            player_modification_version.modification_id = modification_id; // Need a valid modification ID to satisfy the foreign key constraint
            let mut player_report_version = NewPlayerReportVersion::default();
            player_report_version.valid_from = valid_from.naive_utc();
            player_report_version.included_attributes = vec![2]; // Don't close out the player report attribute version we're inserting
            let mut player_report_attribute_version = NewPlayerReportAttributeVersion::default();
            player_report_attribute_version.valid_from = valid_from.naive_utc();
            player_report_attribute_version.attribute = 2;  // Need a valid attribute id (this comes from hard-coded taxa)
            let mut player_equipment_version = NewPlayerEquipmentVersion::default();
            player_equipment_version.valid_from = valid_from.naive_utc();
            player_equipment_version.num_effects = 1; // Don't close out the player equipment effect version we're inserting
            let mut player_equipment_effect_version = NewPlayerEquipmentEffectVersion::default();
            player_equipment_effect_version.valid_from = valid_from.naive_utc();
            player_equipment_effect_version.attribute = 2;  // Need a valid attribute id (this comes from hard-coded taxa)
            player_equipment_effect_version.effect_type = 1;  // Need a valid effect type id (this comes from hard-coded taxa)
            let mut player_pitch_type_version = NewPlayerPitchTypeVersion::default();
            player_pitch_type_version.valid_from = valid_from.naive_utc();
            let mut player_pitch_type_bonus_version = NewPlayerPitchTypeBonusVersion::default();
            player_pitch_type_bonus_version.valid_from = valid_from.naive_utc();
            player_pitch_type_bonus_version.pitch_type = 1;  // Need a valid pitch type id (this comes from hard-coded taxa)
            let mut player_pitch_category_bonus_version = NewPlayerPitchCategoryBonusVersion::default();
            player_pitch_category_bonus_version.valid_from = valid_from.naive_utc();
            player_pitch_category_bonus_version.pitch_category = 1;  // Need a valid pitch category id (this comes from hard-coded taxa)

            let mut player = (
                processed,
                Some(player_version),
                vec![player_modification_version],
                vec![(player_report_version, vec![player_report_attribute_version])],
                vec![(player_equipment_version, vec![player_equipment_effect_version])],
                vec![player_pitch_type_version],
                vec![player_pitch_type_bonus_version],
                vec![player_pitch_category_bonus_version],
                Vec::new(), // Ignore ingest logs
            );
            let (total, inserted) = db::insert_player_versions_all(&mut conn, vec![&player])?;
            assert_eq!(total, 10, "We provided 10 total records");
            assert_eq!(inserted, 10, "Should have inserted all provided records");

            // 2. Insert same version with new date, expect only the processed row to be added
            player_increment_valid_from(&mut player);
            let (total, inserted) = db::insert_player_versions_all(&mut conn, vec![&player])?;
            assert_eq!(total, 10, "We provided 10 total records");
            assert_eq!(inserted, 1, "Should have only inserted `processed`, others should be duplicates");

            // 3. Iterate through fields, insert the record with a modified version of that field, expect 1 row added
            for field in <NewPlayerVersion as OneAu>::fields() {
                // Ignore fields that are part of identification and versioning
                match field {
                    <NewPlayerVersion as OneAu>::Field::mmolb_player_id |
                    <NewPlayerVersion as OneAu>::Field::valid_from |
                    <NewPlayerVersion as OneAu>::Field::valid_until => { continue; }
                    _ => {}
                }

                *player.1.as_mut().unwrap() = player.1.clone().unwrap().au(field);

                player_increment_valid_from(&mut player);
                let (total, inserted) = db::insert_player_versions_all(&mut conn, vec![&player])?;
                assert_eq!(total, 10, "After modifying NewPlayerVersion::{:?}, we provided 10 total records", field);
                assert_eq!(inserted, 2, "After modifying NewPlayerVersion::{:?}, should have inserted `processed` and `player_version`", field);
            }

            // TODO all the child tables (to the tune of all the single ladies)
            player_modification_version_duplicate_detection(&mut conn, &mut player)?;
            player_report_version_duplicate_detection(&mut conn, &mut player)?;
            player_equipment_version_duplicate_detection(&mut conn, &mut player)?;
            player_pitch_type_version_duplicate_detection(&mut conn, &mut player)?;
            player_pitch_type_bonus_version_duplicate_detection(&mut conn, &mut player)?;
            player_pitch_category_bonus_version_duplicate_detection(&mut conn, &mut player)?;

            Ok::<_, diesel::result::Error>(())
        })
    }

    fn player_modification_version_duplicate_detection(conn: &mut PgConnection, player: &mut db::NewPlayerVersionExt) -> Result<(), diesel::result::Error> {
        // 4. Insert a player version that closes out the one (1) player modification
        // version. Don't insert a player modification version, insert_player_versions_all
        // makes no ordering guarantees so it might either be inserted and immediately closed
        // out or it might be inserted after the previous version is closed out
        let player_modification_version = player.2.pop().unwrap();
        player.1.as_mut().unwrap().num_modifications = 0;
        player_increment_valid_from(player);
        let (total, inserted) = db::insert_player_versions_all(conn, vec![&*player])?;
        assert_eq!(total, 9, "We provided 9 total records");
        assert_eq!(inserted, 2, "Should have inserted `processed` and `player_version`");

        // 5. Re-insert the same player modification version, which should be inserted even
        // though it's identical to the previous version because the previous version
        // was closed out
        player.2.push(player_modification_version);
        player.1.as_mut().unwrap().num_modifications = 1;
        player_increment_valid_from(player);
        let (total, inserted) = db::insert_player_versions_all(conn, vec![&*player])?;

        assert_eq!(total, 10, "We provided 10 total records");
        // If this assert fails, it may be because the previous step failed to close out the
        // child version, or because this step incorrectly detected the parent version as a
        // duplicate, or because the child's duplicate detection is incorrectly identifying a
        // closed-out version as a duplicate
        assert_eq!(inserted, 3, "Should have inserted `processed`, `player_version`, and `player_modification_version`");

        // TODO Test close-out functionality for num_greater_boons and num_lesser_boons too

        // We need a second valid modification id
        let another_modification_id = db::insert_modifications(conn, &[&("2", "2", "2")])?
            .expect("DB should not already have an empty modification")
            .first()
            .expect("insert_modifications should return a vec with the same length as the slice it was given, or None")
            .1;


        // 6. Iterate through player modification version fields, insert the record with a modified
        // version of that field, expect 1 row added
        for field in <NewPlayerModificationVersion as OneAu>::fields() {
            // Ignore fields that are part of identification and versioning
            match field {
                <NewPlayerModificationVersion as OneAu>::Field::mmolb_player_id |
                <NewPlayerModificationVersion as OneAu>::Field::modification_type |
                <NewPlayerModificationVersion as OneAu>::Field::modification_index |
                <NewPlayerModificationVersion as OneAu>::Field::valid_from |
                <NewPlayerModificationVersion as OneAu>::Field::valid_until => { continue; }
                _ => {}
            }

            // Special handling for modification ID because it has a foreign key constraint
            if let <NewPlayerModificationVersion as OneAu>::Field::modification_id = field {
                player.2.first_mut().unwrap().modification_id = another_modification_id;
            } else {
                *player.2.first_mut().unwrap() = player.2.first().unwrap().clone().au(field);
            }

            player_increment_valid_from(player);
            let (total, inserted) = db::insert_player_versions_all(conn, vec![&*player])?;
            assert_eq!(total, 10, "After modifying NewPlayerModificationVersion::{:?}, we provided 10 total records", field);
            assert_eq!(inserted, 2, "After modifying NewPlayerModificationVersion::{:?}, should have inserted `processed` and `player_modification_version`", field);
        }

        Ok(())
    }

    fn player_report_version_duplicate_detection(conn: &mut PgConnection, player: &mut db::NewPlayerVersionExt) -> Result<(), diesel::result::Error> {
        // 4. Insert a player version that closes out the one (1) player report
        // version. Don't insert a player report version, insert_player_versions_all
        // makes no ordering guarantees so it might either be inserted and immediately closed
        // out or it might be inserted after the previous version is closed out
        let player_report_version = player.3.pop().unwrap();
        player.1.as_mut().unwrap().included_report_categories = vec![];
        player_increment_valid_from(player);
        let (total, inserted) = db::insert_player_versions_all(conn, vec![&*player])?;
        assert_eq!(total, 8, "We provided 8 total records");
        assert_eq!(inserted, 2, "Should have inserted `processed` and `player_version`");

        // 5. Re-insert the same player report version, which should be inserted even
        // though it's identical to the previous version because the previous version
        // was closed out
        player.3.push(player_report_version);
        player.1.as_mut().unwrap().included_report_categories = vec![0];
        player_increment_valid_from(player);
        let (total, inserted) = db::insert_player_versions_all(conn, vec![&*player])?;

        assert_eq!(total, 10, "We provided 10 total records");
        assert_eq!(inserted, 4, "Should have inserted `processed`, `player_version`, `player_report_version`, and `player_report_attribute_version`");

        // 6. Iterate through player report version fields, insert the record with a modified
        // version of that field, expect 1 row added
        for field in <NewPlayerReportVersion as OneAu>::fields() {
            // Ignore fields that are part of identification and versioning
            match field {
                <NewPlayerReportVersion as OneAu>::Field::mmolb_player_id |
                <NewPlayerReportVersion as OneAu>::Field::category |
                <NewPlayerReportVersion as OneAu>::Field::valid_from |
                <NewPlayerReportVersion as OneAu>::Field::valid_until => { continue; }
                _ => {}
            }

            player.3.first_mut().unwrap().0 = player.3.first().unwrap().0.clone().au(field);

            player_increment_valid_from(player);
            let (total, inserted) = db::insert_player_versions_all(conn, vec![&*player])?;
            assert_eq!(total, 10, "After modifying NewPlayerReportVersion::{:?}, we provided 10 total records", field);
            assert_eq!(inserted, 2, "After modifying NewPlayerReportVersion::{:?}, should have inserted `processed` and `player_report_version`", field);
        }

        player_report_attribute_version_duplicate_detection(conn, player)?;

        Ok(())
    }

    fn player_report_attribute_version_duplicate_detection(conn: &mut PgConnection, player: &mut db::NewPlayerVersionExt) -> Result<(), diesel::result::Error> {
        // 4. Insert a player version that closes out the one (1) player report attribute
        // version. Don't insert a player report version, insert_player_versions_all
        // makes no ordering guarantees so it might either be inserted and immediately closed
        // out or it might be inserted after the previous version is closed out
        let player_report_attribute_version = player.3.first_mut().unwrap().1.pop().unwrap();
        player.3.first_mut().unwrap().0.included_attributes = vec![];
        player_increment_valid_from(player);
        let (total, inserted) = db::insert_player_versions_all(conn, vec![&*player])?;
        assert_eq!(total, 9, "We provided 9 total records");
        assert_eq!(inserted, 2, "Should have inserted `processed` and `player_report_version`");

        // 5. Re-insert the same player report attribute version, which should be inserted even
        // though it's identical to the previous version because the previous version
        // was closed out
        player.3.first_mut().unwrap().1.push(player_report_attribute_version);
        player.3.first_mut().unwrap().0.included_attributes = vec![2];
        player_increment_valid_from(player);
        let (total, inserted) = db::insert_player_versions_all(conn, vec![&*player])?;

        assert_eq!(total, 10, "We provided 10 total records");
        assert_eq!(inserted, 3, "Should have inserted `processed`, `player_report_version`, and `player_report_attribute_version`");

        // 6. Iterate through player report attribute version fields, insert the record with a modified
        // version of that field, expect 1 row added
        for field in <NewPlayerReportAttributeVersion as OneAu>::fields() {
            // Ignore fields that are part of identification and versioning
            match field {
                <NewPlayerReportAttributeVersion as OneAu>::Field::mmolb_player_id |
                <NewPlayerReportAttributeVersion as OneAu>::Field::category |
                <NewPlayerReportAttributeVersion as OneAu>::Field::attribute |
                <NewPlayerReportAttributeVersion as OneAu>::Field::valid_from |
                <NewPlayerReportAttributeVersion as OneAu>::Field::valid_until => { continue; }
                _ => {}
            }

            *player.3.first_mut().unwrap().1.first_mut().unwrap() = player.3.first().unwrap().1.first().unwrap().clone().au(field);

            player_increment_valid_from(player);
            let (total, inserted) = db::insert_player_versions_all(conn, vec![&*player])?;
            assert_eq!(total, 10, "After modifying NewPlayerReportAttributeVersion::{:?}, we provided 10 total records", field);
            assert_eq!(inserted, 2, "After modifying NewPlayerReportAttributeVersion::{:?}, should have inserted `processed` and `player_report_attribute_version`", field);
        }

        Ok(())
    }

    fn player_equipment_version_duplicate_detection(conn: &mut PgConnection, player: &mut db::NewPlayerVersionExt) -> Result<(), diesel::result::Error> {
        // 4. Insert a player version that closes out the one (1) player equipment
        // version. Don't insert a player equipment version, insert_player_versions_all
        // makes no ordering guarantees so it might either be inserted and immediately closed
        // out or it might be inserted after the previous version is closed out
        let player_equipment_version = player.4.pop().unwrap();
        player.1.as_mut().unwrap().occupied_equipment_slots = vec![];
        player_increment_valid_from(player);
        let (total, inserted) = db::insert_player_versions_all(conn, vec![&*player])?;
        assert_eq!(total, 8, "We provided 8 total records");
        assert_eq!(inserted, 2, "Should have inserted `processed` and `player_version`");

        // 5. Re-insert the same player equipment version, which should be inserted even
        // though it's identical to the previous version because the previous version
        // was closed out
        player.4.push(player_equipment_version);
        player.1.as_mut().unwrap().occupied_equipment_slots = vec![""];
        player_increment_valid_from(player);
        let (total, inserted) = db::insert_player_versions_all(conn, vec![&*player])?;

        assert_eq!(total, 10, "We provided 10 total records");
        assert_eq!(inserted, 4, "Should have inserted `processed`, `player_version`, `player_equipment_version`, and `player_equipment_effect_version`");

        // 6. Iterate through player equipment version fields, insert the record with a modified
        // version of that field, expect 1 row added
        for field in <NewPlayerEquipmentVersion as OneAu>::fields() {
            // Ignore fields that are part of identification and versioning
            match field {
                <NewPlayerEquipmentVersion as OneAu>::Field::mmolb_player_id |
                <NewPlayerEquipmentVersion as OneAu>::Field::equipment_slot |
                <NewPlayerEquipmentVersion as OneAu>::Field::valid_from |
                <NewPlayerEquipmentVersion as OneAu>::Field::valid_until => { continue; }
                _ => {}
            }

            player.4.first_mut().unwrap().0 = player.4.first().unwrap().0.clone().au(field);

            player_increment_valid_from(player);
            let (total, inserted) = db::insert_player_versions_all(conn, vec![&*player])?;
            assert_eq!(total, 10, "After modifying NewPlayerEquipmentVersion::{:?}, we provided 10 total records", field);
            assert_eq!(inserted, 2, "After modifying NewPlayerEquipmentVersion::{:?}, should have inserted `processed` and `player_equipment_version`", field);
        }

        player_equipment_effect_version_duplicate_detection(conn, player)?;

        Ok(())
    }

    fn player_equipment_effect_version_duplicate_detection(conn: &mut PgConnection, player: &mut db::NewPlayerVersionExt) -> Result<(), diesel::result::Error> {
        // 4. Insert a player version that closes out the one (1) player equipment effect
        // version. Don't insert a player equipment effect version, insert_player_versions_all
        // makes no ordering guarantees so it might either be inserted and immediately closed
        // out or it might be inserted after the previous version is closed out
        let player_equipment_effect_version = player.4.first_mut().unwrap().1.pop().unwrap();
        player.4.first_mut().unwrap().0.num_effects = 0;
        player_increment_valid_from(player);
        let (total, inserted) = db::insert_player_versions_all(conn, vec![&*player])?;
        assert_eq!(total, 9, "We provided 9 total records");
        assert_eq!(inserted, 2, "Should have inserted `processed` and `player_equipment_version`");

        // 5. Re-insert the same player equipment effect version, which should be inserted even
        // though it's identical to the previous version because the previous version
        // was closed out
        player.4.first_mut().unwrap().1.push(player_equipment_effect_version);
        player.4.first_mut().unwrap().0.num_effects = 1;
        player_increment_valid_from(player);
        let (total, inserted) = db::insert_player_versions_all(conn, vec![&*player])?;

        assert_eq!(total, 10, "We provided 10 total records");
        assert_eq!(inserted, 3, "Should have inserted `processed`, `player_equipment_version`, and `player_equipment_effect_version`");

        // 6. Iterate through player equipment effect version fields, insert the record with a modified
        // version of that field, expect 1 row added
        for field in <NewPlayerEquipmentEffectVersion as OneAu>::fields() {
            // Ignore fields that are part of identification and versioning
            match field {
                <NewPlayerEquipmentEffectVersion as OneAu>::Field::mmolb_player_id |
                <NewPlayerEquipmentEffectVersion as OneAu>::Field::equipment_slot |
                <NewPlayerEquipmentEffectVersion as OneAu>::Field::effect_index |
                <NewPlayerEquipmentEffectVersion as OneAu>::Field::valid_from |
                <NewPlayerEquipmentEffectVersion as OneAu>::Field::valid_until => { continue; }
                _ => {}
            }

            *player.4.first_mut().unwrap().1.first_mut().unwrap() = player.4.first().unwrap().1.first().unwrap().clone().au(field);

            player_increment_valid_from(player);
            let (total, inserted) = db::insert_player_versions_all(conn, vec![&*player])?;
            assert_eq!(total, 10, "After modifying NewPlayerEquipmentEffectVersion::{:?}, we provided 10 total records", field);
            assert_eq!(inserted, 2, "After modifying NewPlayerEquipmentEffectVersion::{:?}, should have inserted `processed` and `player_equipment_effect_version`", field);
        }

        Ok(())
    }

    fn player_pitch_type_version_duplicate_detection(conn: &mut PgConnection, player: &mut db::NewPlayerVersionExt) -> Result<(), diesel::result::Error> {
        // 4. Insert a player version that closes out the one (1) player pitch type
        // version. Don't insert a player pitch type version, insert_player_versions_all
        // makes no ordering guarantees so it might either be inserted and immediately closed
        // out or it might be inserted after the previous version is closed out
        let player_pitch_type_version = player.5.pop().unwrap();
        player.1.as_mut().unwrap().num_pitch_types = 0;
        player_increment_valid_from(player);
        let (total, inserted) = db::insert_player_versions_all(conn, vec![&*player])?;
        assert_eq!(total, 9, "We provided 9 total records");
        assert_eq!(inserted, 2, "Should have inserted `processed` and `player_version`");

        // 5. Re-insert the same player pitch type version, which should be inserted even
        // though it's identical to the previous version because the previous version
        // was closed out
        player.5.push(player_pitch_type_version);
        player.1.as_mut().unwrap().num_pitch_types = 1;
        player_increment_valid_from(player);
        let (total, inserted) = db::insert_player_versions_all(conn, vec![&*player])?;

        assert_eq!(total, 10, "We provided 10 total records");
        assert_eq!(inserted, 3, "Should have inserted `processed`, `player_version`, and `player_pitch_type_version`");

        // 6. Iterate through player pitch type version fields, insert the record with a modified
        // version of that field, expect 1 row added
        for field in <NewPlayerPitchTypeVersion as OneAu>::fields() {
            // Ignore fields that are part of identification and versioning
            match field {
                <NewPlayerPitchTypeVersion as OneAu>::Field::mmolb_player_id |
                <NewPlayerPitchTypeVersion as OneAu>::Field::pitch_type_index |
                <NewPlayerPitchTypeVersion as OneAu>::Field::valid_from |
                <NewPlayerPitchTypeVersion as OneAu>::Field::valid_until => { continue; }
                _ => {}
            }

            *player.5.first_mut().unwrap() = player.5.first().unwrap().clone().au(field);

            player_increment_valid_from(player);
            let (total, inserted) = db::insert_player_versions_all(conn, vec![&*player])?;
            assert_eq!(total, 10, "After modifying NewPlayerPitchTypeVersion::{:?}, we provided 10 total records", field);
            assert_eq!(inserted, 2, "After modifying NewPlayerPitchTypeVersion::{:?}, should have inserted `processed` and `player_pitch_type_version`", field);
        }

        Ok(())
    }

    fn player_pitch_type_bonus_version_duplicate_detection(conn: &mut PgConnection, player: &mut db::NewPlayerVersionExt) -> Result<(), diesel::result::Error> {
        // 4. Insert a player version that closes out the one (1) player pitch type bonus
        // version. Don't insert a player pitch type bonus version, insert_player_versions_all
        // makes no ordering guarantees so it might either be inserted and immediately closed
        // out or it might be inserted after the previous version is closed out
        let player_pitch_type_bonus_version = player.6.pop().unwrap();
        player.1.as_mut().unwrap().included_pitch_type_bonuses = vec![];
        player_increment_valid_from(player);
        let (total, inserted) = db::insert_player_versions_all(conn, vec![&*player])?;
        assert_eq!(total, 9, "We provided 9 total records");
        assert_eq!(inserted, 2, "Should have inserted `processed` and `player_version`");

        // 5. Re-insert the same player pitch type bonus version, which should be inserted even
        // though it's identical to the previous version because the previous version
        // was closed out
        player.6.push(player_pitch_type_bonus_version);
        player.1.as_mut().unwrap().included_pitch_type_bonuses = vec![1];
        player_increment_valid_from(player);
        let (total, inserted) = db::insert_player_versions_all(conn, vec![&*player])?;

        assert_eq!(total, 10, "We provided 10 total records");
        assert_eq!(inserted, 3, "Should have inserted `processed`, `player_version`, and `player_pitch_type_bonus_version`");

        // 6. Iterate through player pitch type bonus version fields, insert the record with a modified
        // version of that field, expect 1 row added
        for field in <NewPlayerPitchTypeBonusVersion as OneAu>::fields() {
            // Ignore fields that are part of identification and versioning
            match field {
                <NewPlayerPitchTypeBonusVersion as OneAu>::Field::mmolb_player_id |
                <NewPlayerPitchTypeBonusVersion as OneAu>::Field::pitch_type |
                <NewPlayerPitchTypeBonusVersion as OneAu>::Field::valid_from |
                <NewPlayerPitchTypeBonusVersion as OneAu>::Field::valid_until => { continue; }
                _ => {}
            }

            *player.6.first_mut().unwrap() = player.6.first().unwrap().clone().au(field);

            player_increment_valid_from(player);
            let (total, inserted) = db::insert_player_versions_all(conn, vec![&*player])?;
            assert_eq!(total, 10, "After modifying NewPlayerPitchTypeBonusVersion::{:?}, we provided 10 total records", field);
            assert_eq!(inserted, 2, "After modifying NewPlayerPitchTypeBonusVersion::{:?}, should have inserted `processed` and `player_pitch_type_bonus_version`", field);
        }

        Ok(())
    }

    fn player_pitch_category_bonus_version_duplicate_detection(conn: &mut PgConnection, player: &mut db::NewPlayerVersionExt) -> Result<(), diesel::result::Error> {
        // 4. Insert a player version that closes out the one (1) player pitch category bonus
        // version. Don't insert a player pitch category bonus version, insert_player_versions_all
        // makes no ordering guarantees so it might either be inserted and immediately closed
        // out or it might be inserted after the previous version is closed out
        let player_pitch_category_bonus_version = player.7.pop().unwrap();
        player.1.as_mut().unwrap().included_pitch_category_bonuses = vec![];
        player_increment_valid_from(player);
        let (total, inserted) = db::insert_player_versions_all(conn, vec![&*player])?;
        assert_eq!(total, 9, "We provided 9 total records");
        assert_eq!(inserted, 2, "Should have inserted `processed` and `player_version`");

        // 5. Re-insert the same player pitch category bonus version, which should be inserted even
        // though it's identical to the previous version because the previous version
        // was closed out
        player.7.push(player_pitch_category_bonus_version);
        player.1.as_mut().unwrap().included_pitch_category_bonuses = vec![1];
        player_increment_valid_from(player);
        let (total, inserted) = db::insert_player_versions_all(conn, vec![&*player])?;

        assert_eq!(total, 10, "We provided 10 total records");
        assert_eq!(inserted, 3, "Should have inserted `processed`, `player_version`, and `player_pitch_category_bonus_version`");

        // 6. Iterate through player pitch category bonus version fields, insert the record with a modified
        // version of that field, expect 1 row added
        for field in <NewPlayerPitchCategoryBonusVersion as OneAu>::fields() {
            // Ignore fields that are part of identification and versioning
            match field {
                <NewPlayerPitchCategoryBonusVersion as OneAu>::Field::mmolb_player_id |
                <NewPlayerPitchCategoryBonusVersion as OneAu>::Field::pitch_category |
                <NewPlayerPitchCategoryBonusVersion as OneAu>::Field::valid_from |
                <NewPlayerPitchCategoryBonusVersion as OneAu>::Field::valid_until => { continue; }
                _ => {}
            }

            *player.7.first_mut().unwrap() = player.7.first().unwrap().clone().au(field);

            player_increment_valid_from(player);
            let (total, inserted) = db::insert_player_versions_all(conn, vec![&*player])?;
            assert_eq!(total, 10, "After modifying NewPlayerPitchCategoryBonusVersion::{:?}, we provided 10 total records", field);
            assert_eq!(inserted, 2, "After modifying NewPlayerPitchCategoryBonusVersion::{:?}, should have inserted `processed` and `player_pitch_category_bonus_version`", field);
        }

        Ok(())
    }
}