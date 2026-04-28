// @generated automatically by Diesel CLI.

pub mod info {
    diesel::table! {
        info.event_ingest_log (id) {
            id -> Int8,
            game_id -> Int8,
            game_event_index -> Nullable<Int4>,
            log_index -> Int4,
            log_level -> Int4,
            log_text -> Text,
        }
    }

    diesel::table! {
        info.version_ingest_log (id) {
            id -> Int8,
            kind -> Text,
            entity_id -> Text,
            valid_from -> Timestamp,
            log_index -> Int4,
            log_level -> Int4,
            log_text -> Text,
        }
    }

    diesel::allow_tables_to_appear_in_same_query!(event_ingest_log, version_ingest_log,);
}
