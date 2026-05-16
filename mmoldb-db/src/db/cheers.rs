// This entire file is a copy of weather.rs with changed types, because generic
// Diesel is hard
use crate::models::{DbCheer, NewCheer};
use diesel::connection::DefaultLoadingMode;
use diesel::prelude::*;
use diesel::{QueryResult, RunQueryDsl};
use hashbrown::{HashMap, HashSet};
use itertools::Itertools;
use tracing::info;

pub(crate) type CheerTable = HashMap<String, i64>;

pub(crate) fn create_cheers_table(
    conn: &mut PgConnection,
    cheers: &HashSet<String>,
) -> QueryResult<CheerTable> {
    if cheers.is_empty() {
        return Ok(CheerTable::new())
    }

    let cheer_table = loop {
        let result = conn.transaction(|conn| create_cheers_table_inner(conn, cheers))?;
        match result {
            OrRetry::Result(cheer_table) => break cheer_table,
            OrRetry::Retry => {
                info!("Unique constraint violation while adding a new cheer. Trying again.");
            }
        };
    };

    Ok(cheer_table)
}

enum OrRetry<T> {
    Result(T),
    Retry,
}

fn create_cheers_table_inner(
    conn: &mut PgConnection,
    cheers: &HashSet<String>,
) -> QueryResult<OrRetry<CheerTable>> {
    use crate::data_schema::data::cheers::dsl as ch_dsl;

    // Populate with the cheers that are there already
    let mut cheer_table = ch_dsl::cheers
        .filter(ch_dsl::cheer.eq_any(cheers))
        .select(DbCheer::as_select())
        .load_iter::<DbCheer, DefaultLoadingMode>(conn)?
        .map_ok(|cheer| (cheer.cheer, cheer.id))
        .collect::<QueryResult<HashMap<String, i64>>>()?;

    let new_cheers = cheers.iter()
        .filter_map(|cheer| if cheer_table.contains_key(cheer) {
            None
        } else {
            Some(NewCheer { cheer })
        })
        // Sort them to avoid deadlocks when multiple tasks try to insert
        // the same cheers in a different order
        .sorted_by_key(|cheer| cheer.cheer)
        .collect_vec();

    if !new_cheers.is_empty() {
        let inserted_cheers = diesel::insert_into(ch_dsl::cheers)
            .values(&new_cheers)
            .on_conflict(ch_dsl::cheer)
            .do_nothing()
            .returning(DbCheer::as_returning())
            .get_results::<DbCheer>(conn)?;

        // The behavior of "on conflict do nothing" is to return no values when
        // there was a conflict
        if inserted_cheers.len() < new_cheers.len() {
            return Ok(OrRetry::Retry);
        }

        cheer_table.extend(inserted_cheers.into_iter().map(|cheer| {
            let id = cheer.id; // Lifetime reasons
            (cheer.cheer, id)
        }));
    }

    assert!(cheers.iter().all(|m| cheer_table.contains_key(m)));

    Ok(OrRetry::Result(cheer_table))
}
