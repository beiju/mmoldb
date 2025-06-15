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
            fielder_position -> Int8,
            play_order -> Int4,
            perfect_catch -> Nullable<Bool>,
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
            hit_type -> Nullable<Int8>,
            fair_ball_type -> Nullable<Int8>,
            fair_ball_direction -> Nullable<Int8>,
            fielding_error_type -> Nullable<Int8>,
            pitch_type -> Nullable<Int8>,
            pitch_speed -> Nullable<Float8>,
            described_as_sacrifice -> Nullable<Bool>,
            count_balls -> Int4,
            count_strikes -> Int4,
            outs_before -> Int4,
            outs_after -> Int4,
            batter_name -> Text,
            pitcher_name -> Text,
        }
    }

    diesel::table! {
        data.games (id) {
            id -> Int8,
            ingest -> Int8,
            mmolb_game_id -> Text,
            season -> Int4,
            day -> Int4,
            away_team_emoji -> Text,
            away_team_name -> Text,
            away_team_id -> Text,
            home_team_emoji -> Text,
            home_team_name -> Text,
            home_team_id -> Text,
        }
    }

    diesel::table! {
        data.ingests (id) {
            id -> Int8,
            started_at -> Timestamp,
            finished_at -> Nullable<Timestamp>,
            aborted_at -> Nullable<Timestamp>,
            latest_completed_season -> Nullable<Int4>,
        }
    }

    diesel::joinable!(event_baserunners -> events (event_id));
    diesel::joinable!(event_fielders -> events (event_id));
    diesel::joinable!(events -> games (game_id));
    diesel::joinable!(games -> ingests (ingest));

    diesel::allow_tables_to_appear_in_same_query!(
        event_baserunners,
        event_fielders,
        events,
        games,
        ingests,
    );
}
