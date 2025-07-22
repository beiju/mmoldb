// @generated automatically by Diesel CLI.

pub mod taxa {
    diesel::table! {
        taxa.base (id) {
            id -> Int8,
            name -> Text,
            bases_achieved -> Int4,
        }
    }

    diesel::table! {
        taxa.base_description_format (id) {
            id -> Int8,
            name -> Text,
        }
    }

    diesel::table! {
        taxa.day_type (id) {
            id -> Int8,
            name -> Text,
            display_name -> Text,
        }
    }

    diesel::table! {
        taxa.event_type (id) {
            id -> Int8,
            name -> Text,
            display_name -> Text,
            ends_plate_appearance -> Bool,
            is_in_play -> Bool,
            is_hit -> Bool,
            is_ball -> Bool,
            is_strike -> Bool,
            is_strikeout -> Bool,
            is_basic_strike -> Bool,
            is_foul -> Bool,
            is_foul_tip -> Bool,
            batter_swung -> Bool,
            is_error -> Bool,
        }
    }

    diesel::table! {
        taxa.fair_ball_type (id) {
            id -> Int8,
            name -> Text,
            display_name -> Text,
        }
    }

    diesel::table! {
        taxa.fielder_location (id) {
            id -> Int8,
            name -> Text,
            display_name -> Text,
            abbreviation -> Text,
            area -> Text,
        }
    }

    diesel::table! {
        taxa.fielding_error_type (id) {
            id -> Int8,
            name -> Text,
        }
    }

    diesel::table! {
        taxa.handedness (id) {
            id -> Int8,
            name -> Text,
        }
    }

    diesel::table! {
        taxa.leagues (id) {
            id -> Int8,
            name -> Text,
            color -> Text,
            emoji -> Text,
            league_type -> Text,
            parent_team_id -> Text,
            mmolb_league_id -> Text,
        }
    }

    diesel::table! {
        taxa.pitch_type (id) {
            id -> Int8,
            name -> Text,
            display_name -> Text,
            abbreviation -> Text,
        }
    }

    diesel::table! {
        taxa.slot (id) {
            id -> Int8,
            name -> Text,
            display_name -> Text,
            abbreviation -> Text,
            role -> Text,
            pitcher_type -> Nullable<Text>,
            slot_number -> Nullable<Int4>,
            location -> Nullable<Int8>,
        }
    }

    diesel::joinable!(slot -> fielder_location (location));

    diesel::allow_tables_to_appear_in_same_query!(
        base,
        base_description_format,
        day_type,
        event_type,
        fair_ball_type,
        fielder_location,
        fielding_error_type,
        handedness,
        leagues,
        pitch_type,
        slot,
    );
}
