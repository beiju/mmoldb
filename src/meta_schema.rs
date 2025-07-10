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

    diesel::table! {
        information_schema.tables (table_catalog, table_schema, table_name) {
            table_catalog -> Nullable<Text>,
            table_schema -> Nullable<Text>,
            table_name -> Nullable<Text>,
            table_type -> Nullable<Text>,
            self_referencing_column_name -> Nullable<Text>,
            reference_generation -> Nullable<Text>,
            user_defined_type_catalog -> Nullable<Text>,
            user_defined_type_schema -> Nullable<Text>,
            user_defined_type_name -> Nullable<Text>,
            is_insertable_into -> Nullable<Text>,
            is_typed -> Nullable<Text>,
            commit_action -> Nullable<Text>,
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
            character_set_catalog -> Nullable<Text>,
            character_set_schema -> Nullable<Text>,
            character_set_name -> Nullable<Text>,
            collation_catalog -> Nullable<Text>,
            collation_schema -> Nullable<Text>,
            collation_name -> Nullable<Text>,
            domain_catalog -> Nullable<Text>,
            domain_schema -> Nullable<Text>,
            domain_name -> Nullable<Text>,
            udt_catalog -> Nullable<Text>,
            udt_schema -> Nullable<Text>,
            udt_name -> Nullable<Text>,
            scope_catalog -> Nullable<Text>,
            scope_schema -> Nullable<Text>,
            scope_name -> Nullable<Text>,
            maximum_cardinality -> Nullable<Integer>,
            dtd_identifier -> Nullable<Text>,
            is_self_referencing -> Nullable<Text>,
            is_identity -> Nullable<Text>,
            identity_generation -> Nullable<Text>,
            identity_start -> Nullable<Text>,
            identity_increment -> Nullable<Text>,
            identity_maximum -> Nullable<Text>,
            identity_minimum -> Nullable<Text>,
            identity_cycle -> Nullable<Text>,
            is_generated -> Nullable<Text>,
            generation_expression -> Nullable<Text>,
            is_updatable -> Nullable<Text>,
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
