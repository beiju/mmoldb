// Written by hand in the style of diesel-cli generated files, because diesel cli doesn't
// support views
pub mod meta {
    diesel::table! {
        information_schema.schemata (catalog_name, schema_name) {
            catalog_name -> Nullable<Text>,
            schema_name -> Nullable<Text>,
            schema_owner -> Nullable<Text>,
            default_character_set_catalog -> Nullable<Text>,
            default_character_set_schema -> Nullable<Text>,
            default_character_set_name -> Nullable<Text>,
            sql_path -> Nullable<Text>,
        }
    }

    // diesel::joinable!(ingest_timings -> ingests (ingest_id));
    //
    // diesel::allow_tables_to_appear_in_same_query!(
    //     event_ingest_log,
    //     ingest_timings,
    //     ingests,
    //     raw_events,
    // );
}
