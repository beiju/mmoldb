// Written by hand in the style of diesel-cli generated files, because diesel cli doesn't
// support views
pub mod meta {
    diesel::table! {
        information_schema.schemata (catalog_name, schema_name) {
            catalog_name -> Nullable<Text>,
            schema_name -> Nullable<Text>,
        }
    }

    diesel::table! {
        information_schema.tables (table_catalog, table_schema, table_name) {
            table_catalog -> Nullable<Text>,
            table_schema -> Nullable<Text>,
            table_name -> Nullable<Text>,
            table_type -> Nullable<Text>,
        }
    }

    diesel::table! {
        information_schema.columns (table_catalog, table_schema, table_name, column_name) {
            table_catalog -> Nullable<Text>,
            table_schema -> Nullable<Text>,
            table_name -> Nullable<Text>,
            column_name -> Nullable<Text>,
            ordinal_position -> Nullable<Integer>,
            column_default -> Nullable<Text>,
            #[sql_name = "is_nullable"]
            column_is_nullable -> Nullable<Text>,
            data_type -> Nullable<Text>,
            character_maximum_length -> Nullable<Integer>,
            character_octet_length -> Nullable<Integer>,
            numeric_precision -> Nullable<Integer>,
            numeric_precision_radix -> Nullable<Integer>,
            numeric_scale -> Nullable<Integer>,
            datetime_precision -> Nullable<Integer>,
            interval_type -> Nullable<Text>,
            interval_precision -> Nullable<Integer>,
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
