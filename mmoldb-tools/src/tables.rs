use std::collections::HashMap;
use thiserror::Error;
use lazy_static::lazy_static;
use mmoldb_db::db;
use mmoldb_db::db::{ColumnType, DbTable};

#[derive(Debug)]
pub struct TableWithParent {
    pub table: &'static str,
    pub parent_column: &'static str,
    pub parent_table: &'static str,
}

#[derive(Debug)]
pub enum KindStyle {
    // game-style means that everything is connected by foreign key and you can
    // delete stuff by just deleting from the main table
    Game {
        root_table: &'static str,
        // Conceptual sub-tables (or sub-sub-tables, etc.) of the parent. These have
        // foreign key relationships that eventually lead to the parent. E.g.
        // `event_baserunners` is a child of `events`.
        child_tables: Vec<TableWithParent>,
        // Tables of extra information that the parent can reference. Unmoored from
        // time. E.g. `weather` is an auxiliary table for `games`.
        auxiliary_tables: Vec<&'static str>,
        // This one is self-explanatory
        materialized_views: Vec<&'static str>,
    },
    // version-style means that nothing is connected by foreign key and you
    // have to enumerate every table AND un-close-out existing rows to delete
    // stuff
    Version {
        // Tables derived from the `kind` polling
        version_derived_tables: Vec<&'static str>,

        // Tables of extra information that the kind can reference. Unmoored from
        // time. E.g. `modifications` is an auxiliary table for `player_versions`.
        auxiliary_tables: Vec<&'static str>,

        // Tables derived from feed events
        feed_derived_tables: Vec<&'static str>,
    },
}

lazy_static! {
    static ref KIND_TABLES: HashMap<&'static str, KindStyle> = {
        let mut m = HashMap::new();
        m.insert("game", KindStyle::Game {
            root_table: "games",
            child_tables: vec![
                TableWithParent { table: "events",                     parent_table: "games",  parent_column: "game_id"  },
                TableWithParent { table: "event_baserunners",          parent_table: "events", parent_column: "event_id" },
                TableWithParent { table: "event_fielders",             parent_table: "events", parent_column: "event_id" },
                TableWithParent { table: "event_balk_reasons",         parent_table: "events", parent_column: "event_id" },
                TableWithParent { table: "aurora_photos",              parent_table: "events", parent_column: "event_id" },
                TableWithParent { table: "consumption_contest_events", parent_table: "games",  parent_column: "game_id"  },
                TableWithParent { table: "consumption_contests",       parent_table: "games",  parent_column: "game_id"  },
                TableWithParent { table: "door_prizes",                parent_table: "events", parent_column: "event_id" },
                TableWithParent { table: "door_prize_items",           parent_table: "events", parent_column: "event_id" },
                TableWithParent { table: "efflorescence",              parent_table: "events", parent_column: "event_id" },
                TableWithParent { table: "efflorescence_growth",       parent_table: "events", parent_column: "event_id" },
                TableWithParent { table: "ejections",                  parent_table: "events", parent_column: "event_id" },
                TableWithParent { table: "failed_ejections",           parent_table: "events", parent_column: "event_id" },
                TableWithParent { table: "wither",                     parent_table: "games",  parent_column: "game_id"  },
                TableWithParent { table: "parties",                    parent_table: "games",  parent_column: "game_id"  },
                TableWithParent { table: "pitcher_changes",            parent_table: "games",  parent_column: "game_id"  },
                TableWithParent { table: "event_cheers",               parent_table: "events", parent_column: "event_id" },
            ],
            auxiliary_tables: vec![
                "weather",
                "cheers",
                "balk_reasons",
            ],
            materialized_views: vec![
                "events_extended"
            ],
        });
        m.insert("team", KindStyle::Version {
            version_derived_tables: vec![
                "team_versions",
                "team_player_versions",
            ],
            auxiliary_tables: vec![
                "modifications",
            ],
            feed_derived_tables: vec![
                "team_games_played",
            ],
        });
        m.insert("player", KindStyle::Version {
            version_derived_tables: vec![
                "player_versions",
                "player_modification_versions",
                "player_equipment_versions",
                "player_equipment_effect_versions",
                "player_report_versions",
                "player_report_attribute_versions",
                "player_pitch_type_versions",
                "player_pitch_type_bonus_versions",
                "player_pitch_category_bonus_versions",
            ],
            auxiliary_tables: Vec::new(),
            feed_derived_tables: vec![
                "player_recompositions",
                "player_attribute_augments",
                "player_paradigm_shifts",
            ],
        });
        m
    };
}

#[derive(Debug, Error)]
pub enum CheckTablesError {
    #[error(transparent)]
    PoolError(#[from] mmoldb_db::PoolError),

    #[error(transparent)]
    DbMetaQueryError(#[from] mmoldb_db::DbMetaQueryError),

    #[error("Table exists which was not found in KIND_TABLES: {0:#?}")]
    TablesNotListed(Vec<String>),

    #[error("Table in KIND_TABLES did not exist, or appeared in KIND_TABLES too many times: {0}")]
    ListedTableDoesNotExist(String),

    #[error("Supposed parent column {column} of table {table} in KIND_TABLES did not exist")]
    ListedParentColumnDoesNotExist {
        column: String,
        table: String,
    },

    #[error("Supposed parent column {column} of table {table} in KIND_TABLES was not bigint type")]
    ListedParentColumnIsNotBigint {
        column: String,
        table: String,
    },

    #[error("Table {parent}, in KIND_TABLES as the parent of {of}, does not exist")]
    ParentTableDoesNotExist {
        parent: String,
        of: String,
    },

    #[error("Table {table} column {reference_column} is supposed to reference {reference_table}, but was a value instead")]
    ListedReferenceIsValue {
        table: String,
        reference_column: String,
        reference_table: String,
    },

    #[error(
        "Table `{reference_table}` column `{reference_column}` is listed as referencing table \
        `{referenced_table}`, but it actually referenced table `{actual_table}` column \
        `{actual_column}`"
    )]
    ListedReferenceTableDoesNotMatch {
        reference_table: String,
        reference_column: String,
        referenced_table: String,
        actual_table: String,
        actual_column: String,
    }
}

pub fn kind_tables() -> &'static HashMap<&'static str, KindStyle> {
    &KIND_TABLES
}

pub fn check_tables() -> Result<(), CheckTablesError> {
    // 4 is arbitrary
    let pool = mmoldb_db::get_pool(4)?;
    let mut conn = pool.get()?;

    let mut unaccountedfor_tables =
        db::tables_for_schema(&mut conn, "mmoldb", "data")?;

    // Root tables (non-derived data) and _processed tables
    record_table(&mut unaccountedfor_tables, "entities")?;
    record_table(&mut unaccountedfor_tables, "versions")?;
    record_table(&mut unaccountedfor_tables, "versions_processed")?;
    record_table(&mut unaccountedfor_tables, "feed_events")?;
    record_table(&mut unaccountedfor_tables, "feed_events_processed")?;
    // This is going to be deleted soon
    record_table(&mut unaccountedfor_tables, "feed_event_versions")?;

    // Generated from included data
    record_table(&mut unaccountedfor_tables, "modification_effects")?;

    for (kind, tables) in kind_tables() {
        println!("Checking {}", kind);
        match tables {
            KindStyle::Game { root_table, child_tables, auxiliary_tables, materialized_views } => {
                check_child_tables(&mut unaccountedfor_tables, child_tables)?;
                record_tables(&mut unaccountedfor_tables, auxiliary_tables)?;
                record_tables(&mut unaccountedfor_tables, materialized_views)?;
                record_table(&mut unaccountedfor_tables, root_table)?;
            },
            KindStyle::Version { version_derived_tables, auxiliary_tables, feed_derived_tables } => {
                record_tables(&mut unaccountedfor_tables, version_derived_tables)?;
                record_tables(&mut unaccountedfor_tables, auxiliary_tables)?;
                record_tables(&mut unaccountedfor_tables, feed_derived_tables)?;
            }
        }
    }

    if !unaccountedfor_tables.is_empty() {
        Err(CheckTablesError::TablesNotListed(
            unaccountedfor_tables.into_iter().map(|t| t.name).collect(),
        ))
    } else {
        Ok(())
    }
}

fn record_tables(unaccountedfor_tables: &mut Vec<DbTable>, tables: &[&'static str]) -> Result<(), CheckTablesError> {
    for table_name in tables {
        record_table(unaccountedfor_tables, table_name)?;
    }
    Ok(())
}

fn record_table(unaccountedfor_tables: &mut Vec<DbTable>, table_name: &str) -> Result<(), CheckTablesError> {
    if let Some(idx) = unaccountedfor_tables.iter().position(|t| t.name == *table_name) {
        unaccountedfor_tables.swap_remove(idx);
    } else {
        return Err(CheckTablesError::ListedTableDoesNotExist(table_name.to_string()));
    }
    Ok(())
}

fn check_child_tables(unaccountedfor_tables: &mut Vec<DbTable>, descendant_tables: &[TableWithParent]) -> Result<(), CheckTablesError> {
    for descendant in descendant_tables {
        // Find the descendant table, erroring if it doesn't exist
        let table_idx = unaccountedfor_tables.iter()
            .position(|table| table.name == descendant.table)
            .ok_or_else(|| CheckTablesError::ListedTableDoesNotExist(descendant.table.to_string()))?;

        // Find the column that points to the parent, erroring if it doesn't exist
        let column = unaccountedfor_tables[table_idx].columns.iter()
            .find(|column| column.name == descendant.parent_column)
            .ok_or_else(|| CheckTablesError::ListedParentColumnDoesNotExist {
                column: descendant.parent_column.to_string(),
                table: descendant.table.to_string(),
            })?;

        // Make sure the column supposedly points to the parent really does so
        match &column.r#type {
            ColumnType::ValueType(_) => {
                Err(CheckTablesError::ListedReferenceIsValue {
                    table: descendant.table.to_string(),
                    reference_column: descendant.parent_column.to_string(),
                    reference_table: descendant.parent_table.to_string(),
                })?
            }
            ColumnType::ReferenceType { references_table, foreign_key_column } => {
                if descendant.parent_table != references_table {
                    // TODO I do not trust the values here
                    Err(CheckTablesError::ListedReferenceTableDoesNotMatch {
                        reference_table: descendant.table.to_string(),
                        reference_column: descendant.parent_column.to_string(),
                        referenced_table: descendant.parent_table.to_string(),
                        actual_table: references_table.to_string(),
                        actual_column: foreign_key_column.to_string(),
                    })?;
                }
                if descendant.parent_column != foreign_key_column {
                    todo!("Error for this")
                }
            }
        }

        // Ensure the parent table exists
        unaccountedfor_tables.iter()
            .find(|table| table.name == descendant.parent_table)
            .ok_or_else(|| CheckTablesError::ParentTableDoesNotExist {
                parent: descendant.parent_table.to_string(),
                of: descendant.table.to_string(),
            })?;
    }

    for descendant in descendant_tables {
        record_table(unaccountedfor_tables, descendant.table)?;
    }

    Ok(())
}