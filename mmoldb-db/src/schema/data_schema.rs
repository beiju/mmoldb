// @generated automatically by Diesel CLI.

pub mod data {
    diesel::table! {
        data.aurora_photos (id) {
            id -> Int8,
            event_id -> Int8,
            is_listed_first -> Bool,
            team_emoji -> Text,
            player_slot -> Int8,
            player_name -> Text,
        }
    }

    diesel::table! {
        data.ejections (id) {
            id -> Int8,
            event_id -> Int8,
            team_emoji -> Text,
            team_name -> Text,
            ejected_player_name -> Text,
            ejected_player_slot -> Int8,
            violation_type -> Text,
            reason -> Text,
            replacement_player_name -> Text,
            replacement_player_slot -> Nullable<Int8>,
        }
    }

    diesel::table! {
        data.entities (kind, entity_id) {
            kind -> Text,
            entity_id -> Text,
            valid_from -> Timestamp,
            data -> Jsonb,
        }
    }

    diesel::table! {
        data.event_baserunners (id) {
            id -> Int8,
            event_id -> Int8,
            baserunner_name -> Text,
            base_before -> Nullable<Int8>,
            base_after -> Int8,
            is_out -> Bool,
            base_description_format -> Nullable<Int8>,
            steal -> Bool,
            source_event_index -> Nullable<Int4>,
            is_earned -> Bool,
        }
    }

    diesel::table! {
        data.event_fielders (id) {
            id -> Int8,
            event_id -> Int8,
            fielder_name -> Text,
            fielder_slot -> Int8,
            play_order -> Int4,
        }
    }

    diesel::table! {
        data.events (id) {
            id -> Int8,
            game_id -> Int8,
            game_event_index -> Int4,
            fair_ball_event_index -> Nullable<Int4>,
            inning -> Int4,
            top_of_inning -> Bool,
            event_type -> Int8,
            hit_base -> Nullable<Int8>,
            fair_ball_type -> Nullable<Int8>,
            fair_ball_direction -> Nullable<Int8>,
            fielding_error_type -> Nullable<Int8>,
            pitch_type -> Nullable<Int8>,
            pitch_speed -> Nullable<Float8>,
            pitch_zone -> Nullable<Int4>,
            described_as_sacrifice -> Nullable<Bool>,
            is_toasty -> Nullable<Bool>,
            balls_before -> Int4,
            strikes_before -> Int4,
            outs_before -> Int4,
            outs_after -> Int4,
            away_team_score_before -> Int4,
            away_team_score_after -> Int4,
            home_team_score_before -> Int4,
            home_team_score_after -> Int4,
            pitcher_name -> Text,
            pitcher_count -> Int4,
            batter_name -> Text,
            batter_count -> Int4,
            batter_subcount -> Int4,
            errors_before -> Int4,
            errors_after -> Int4,
            cheer -> Nullable<Text>,
        }
    }

    diesel::table! {
        data.games (id) {
            id -> Int8,
            ingest -> Int8,
            mmolb_game_id -> Text,
            weather -> Int8,
            season -> Int4,
            day -> Nullable<Int4>,
            superstar_day -> Nullable<Int4>,
            away_team_emoji -> Text,
            away_team_name -> Text,
            away_team_mmolb_id -> Text,
            away_team_final_score -> Nullable<Int4>,
            home_team_emoji -> Text,
            home_team_name -> Text,
            home_team_mmolb_id -> Text,
            home_team_final_score -> Nullable<Int4>,
            is_ongoing -> Bool,
            from_version -> Timestamp,
            stadium_name -> Nullable<Text>,
            home_team_earned_coins -> Nullable<Int4>,
            away_team_earned_coins -> Nullable<Int4>,
            home_team_photo_contest_top_scorer -> Nullable<Text>,
            home_team_photo_contest_score -> Nullable<Int4>,
            away_team_photo_contest_top_scorer -> Nullable<Text>,
            away_team_photo_contest_score -> Nullable<Int4>,
        }
    }

    diesel::table! {
        data.modifications (id) {
            id -> Int8,
            name -> Text,
            emoji -> Text,
            description -> Text,
        }
    }

    diesel::table! {
        data.player_attribute_augments (id) {
            id -> Int8,
            mmolb_player_id -> Text,
            feed_event_index -> Int4,
            time -> Timestamp,
            attribute -> Int8,
            value -> Int4,
            season -> Int4,
            day_type -> Nullable<Int8>,
            day -> Nullable<Int4>,
            superstar_day -> Nullable<Int4>,
        }
    }

    diesel::table! {
        data.player_equipment_effect_versions (id) {
            id -> Int8,
            mmolb_player_id -> Text,
            equipment_slot -> Text,
            effect_index -> Int4,
            valid_from -> Timestamp,
            valid_until -> Nullable<Timestamp>,
            duplicates -> Int4,
            attribute -> Int8,
            effect_type -> Int8,
            value -> Float8,
        }
    }

    diesel::table! {
        data.player_equipment_versions (id) {
            id -> Int8,
            mmolb_player_id -> Text,
            equipment_slot -> Text,
            valid_from -> Timestamp,
            valid_until -> Nullable<Timestamp>,
            duplicates -> Int4,
            emoji -> Text,
            name -> Text,
            special_type -> Nullable<Text>,
            description -> Nullable<Text>,
            rare_name -> Nullable<Text>,
            cost -> Nullable<Int4>,
            prefixes -> Array<Nullable<Text>>,
            suffixes -> Array<Nullable<Text>>,
            rarity -> Nullable<Text>,
            num_effects -> Int4,
        }
    }

    diesel::table! {
        data.player_feed_versions (id) {
            id -> Int8,
            mmolb_player_id -> Text,
            valid_from -> Timestamp,
            valid_until -> Nullable<Timestamp>,
            duplicates -> Int4,
            num_entries -> Int4,
        }
    }

    diesel::table! {
        data.player_modification_versions (id) {
            id -> Int8,
            mmolb_player_id -> Text,
            modification_index -> Int4,
            valid_from -> Timestamp,
            valid_until -> Nullable<Timestamp>,
            duplicates -> Int4,
            modification_id -> Int8,
        }
    }

    diesel::table! {
        data.player_paradigm_shifts (id) {
            id -> Int8,
            mmolb_player_id -> Text,
            feed_event_index -> Int4,
            time -> Timestamp,
            attribute -> Int8,
            season -> Int4,
            day_type -> Nullable<Int8>,
            day -> Nullable<Int4>,
            superstar_day -> Nullable<Int4>,
        }
    }

    diesel::table! {
        data.player_recompositions (id) {
            id -> Int8,
            mmolb_player_id -> Text,
            feed_event_index -> Int4,
            time -> Timestamp,
            season -> Int4,
            day_type -> Nullable<Int8>,
            day -> Nullable<Int4>,
            superstar_day -> Nullable<Int4>,
            player_name_before -> Text,
            player_name_after -> Text,
            inferred_event_index -> Nullable<Int4>,
            reverts_recomposition -> Nullable<Timestamp>,
        }
    }

    diesel::table! {
        data.player_report_attribute_versions (id) {
            id -> Int8,
            mmolb_player_id -> Text,
            category -> Int8,
            attribute -> Int8,
            valid_from -> Timestamp,
            valid_until -> Nullable<Timestamp>,
            stars -> Int4,
        }
    }

    diesel::table! {
        data.player_report_versions (id) {
            id -> Int8,
            mmolb_player_id -> Text,
            category -> Int8,
            valid_from -> Timestamp,
            valid_until -> Nullable<Timestamp>,
            season -> Nullable<Int4>,
            day_type -> Nullable<Int8>,
            day -> Nullable<Int4>,
            superstar_day -> Nullable<Int4>,
            quote -> Text,
            included_attributes -> Array<Nullable<Int8>>,
        }
    }

    diesel::table! {
        data.player_versions (id) {
            id -> Int8,
            mmolb_player_id -> Text,
            valid_from -> Timestamp,
            valid_until -> Nullable<Timestamp>,
            duplicates -> Int4,
            first_name -> Text,
            last_name -> Text,
            batting_handedness -> Nullable<Int8>,
            pitching_handedness -> Nullable<Int8>,
            home -> Text,
            birthseason -> Int4,
            birthday_type -> Nullable<Int8>,
            birthday_day -> Nullable<Int4>,
            birthday_superstar_day -> Nullable<Int4>,
            likes -> Text,
            dislikes -> Text,
            number -> Int4,
            mmolb_team_id -> Nullable<Text>,
            slot -> Nullable<Int8>,
            durability -> Float8,
            greater_boon -> Nullable<Int8>,
            lesser_boon -> Nullable<Int8>,
            num_modifications -> Int4,
            occupied_equipment_slots -> Array<Nullable<Text>>,
            included_report_categories -> Array<Nullable<Int8>>,
        }
    }

    diesel::table! {
        data.team_player_versions (id) {
            id -> Int8,
            mmolb_team_id -> Text,
            team_player_index -> Int4,
            valid_from -> Timestamp,
            valid_until -> Nullable<Timestamp>,
            duplicates -> Int4,
            first_name -> Text,
            last_name -> Text,
            number -> Int4,
            slot -> Nullable<Int8>,
            mmolb_player_id -> Text,
        }
    }

    diesel::table! {
        data.team_versions (id) {
            id -> Int8,
            mmolb_team_id -> Text,
            valid_from -> Timestamp,
            valid_until -> Nullable<Timestamp>,
            duplicates -> Int4,
            name -> Text,
            emoji -> Text,
            color -> Text,
            location -> Text,
            full_location -> Text,
            abbreviation -> Text,
            motto -> Nullable<Text>,
            active -> Bool,
            eligible -> Nullable<Bool>,
            augments -> Int4,
            championships -> Int4,
            motes_used -> Nullable<Int4>,
            mmolb_league_id -> Nullable<Text>,
            ballpark_name -> Nullable<Text>,
            ballpark_word_1 -> Nullable<Text>,
            ballpark_word_2 -> Nullable<Text>,
            ballpark_suffix -> Nullable<Text>,
            ballpark_use_city -> Nullable<Bool>,
            num_players -> Int4,
        }
    }

    diesel::table! {
        data.versions (kind, entity_id, valid_from) {
            kind -> Text,
            entity_id -> Text,
            valid_from -> Timestamp,
            valid_to -> Nullable<Timestamp>,
            data -> Jsonb,
        }
    }

    diesel::table! {
        data.weather (id) {
            id -> Int8,
            name -> Text,
            emoji -> Text,
            tooltip -> Text,
        }
    }

    diesel::joinable!(aurora_photos -> events (event_id));
    diesel::joinable!(ejections -> events (event_id));
    diesel::joinable!(event_baserunners -> events (event_id));
    diesel::joinable!(event_fielders -> events (event_id));
    diesel::joinable!(events -> games (game_id));
    diesel::joinable!(games -> weather (weather));
    diesel::joinable!(player_modification_versions -> modifications (modification_id));

    diesel::allow_tables_to_appear_in_same_query!(
        aurora_photos,
        ejections,
        entities,
        event_baserunners,
        event_fielders,
        events,
        games,
        modifications,
        player_attribute_augments,
        player_equipment_effect_versions,
        player_equipment_versions,
        player_feed_versions,
        player_modification_versions,
        player_paradigm_shifts,
        player_recompositions,
        player_report_attribute_versions,
        player_report_versions,
        player_versions,
        team_player_versions,
        team_versions,
        versions,
        weather,
    );
}
