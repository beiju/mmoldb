// @generated automatically by Diesel CLI.

pub mod data {
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
        data.player_modification_versions (id) {
            id -> Int8,
            mmolb_player_id -> Text,
            modification_order -> Int4,
            valid_from -> Timestamp,
            valid_until -> Nullable<Timestamp>,
            duplicates -> Int4,
            modification_id -> Int8,
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
            batting_handedness -> Int8,
            pitching_handedness -> Int8,
            home -> Text,
            birthseason -> Int4,
            birthday_type -> Int8,
            birthday_day -> Nullable<Int4>,
            birthday_superstar_day -> Nullable<Int4>,
            likes -> Text,
            dislikes -> Text,
            number -> Int4,
            mmolb_team_id -> Nullable<Text>,
            slot -> Int8,
            durability -> Float8,
            greater_boon -> Nullable<Int8>,
            lesser_boon -> Nullable<Int8>,
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

    diesel::joinable!(event_baserunners -> events (event_id));
    diesel::joinable!(event_fielders -> events (event_id));
    diesel::joinable!(events -> games (game_id));
    diesel::joinable!(games -> weather (weather));
    diesel::joinable!(player_modification_versions -> modifications (modification_id));

    diesel::allow_tables_to_appear_in_same_query!(
        entities,
        event_baserunners,
        event_fielders,
        events,
        games,
        modifications,
        player_modification_versions,
        player_versions,
        versions,
        weather,
    );
}
