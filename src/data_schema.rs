// @generated automatically by Diesel CLI.

pub mod data {
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
            is_finished -> Bool,
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

    diesel::allow_tables_to_appear_in_same_query!(
        event_baserunners,
        event_fielders,
        events,
        games,
        versions,
        weather,
    );
}
