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
    // Adjust module paths as needed for your crate layout
    diesel::table! {
        information_schema.table_constraints (constraint_catalog, constraint_schema, constraint_name) {
            constraint_catalog -> Text,
            constraint_schema -> Text,
            constraint_name -> Text,
            table_catalog -> Text,
            table_schema -> Text,
            table_name -> Text,
            constraint_type -> Text,
        }
    }

    diesel::table! {
        information_schema.key_column_usage (constraint_catalog, constraint_schema, constraint_name, ordinal_position) {
            constraint_catalog -> Text,
            constraint_schema -> Text,
            constraint_name -> Text,
            table_catalog -> Text,
            table_schema -> Text,
            table_name -> Text,
            column_name -> Text,
            ordinal_position -> Integer,
        }
    }

    diesel::table! {
        information_schema.referential_constraints (constraint_catalog, constraint_schema, constraint_name) {
            constraint_catalog -> Text,
            constraint_schema -> Text,
            constraint_name -> Text,
            unique_constraint_catalog -> Text,
            unique_constraint_schema -> Text,
            unique_constraint_name -> Text,
        }
    }

    diesel::table! {
        information_schema.constraint_column_usage (constraint_catalog, constraint_schema, constraint_name) {
            constraint_catalog -> Text,
            constraint_schema -> Text,
            constraint_name -> Text,
            table_catalog -> Text,
            table_schema -> Text,
            table_name -> Text,
            column_name -> Text,
        }
    }
    diesel::allow_tables_to_appear_in_same_query!(
        table_constraints,
        key_column_usage,
        referential_constraints,
        constraint_column_usage,
    );
}
