// This entire file is a copy of weather.rs with changed types, because generic
// Diesel is hard
use crate::models::{DbCheerMessage, NewCheerMessage};
use diesel::connection::DefaultLoadingMode;
use diesel::prelude::*;
use diesel::{QueryResult, RunQueryDsl};
use hashbrown::{HashMap, HashSet};
use itertools::Itertools;
use tracing::info;

pub(crate) type CheerTable = HashMap<String, i64>;

pub(crate) fn create_cheers_table(
    conn: &mut PgConnection,
    messages: &HashSet<String>,
) -> QueryResult<CheerTable> {
    if messages.is_empty() {
        return Ok(CheerTable::new())
    }

    let cheer_table = loop {
        let result = conn.transaction(|conn| create_cheers_table_inner(conn, messages))?;
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
    messages: &HashSet<String>,
) -> QueryResult<OrRetry<CheerTable>> {
    use crate::data_schema::data::cheer_messages::dsl as cm_dsl;

    // Populate with the messages that are there already
    let mut cheer_table = cm_dsl::cheer_messages
        .filter(cm_dsl::message.eq_any(messages))
        .select(DbCheerMessage::as_select())
        .load_iter::<DbCheerMessage, DefaultLoadingMode>(conn)?
        .map_ok(|cheer| (cheer.message, cheer.id))
        .collect::<QueryResult<HashMap<String, i64>>>()?;

    let new_messages = messages.iter()
        .filter_map(|message| if cheer_table.contains_key(message) {
            None
        } else {
            Some(NewCheerMessage { message })
        })
        // Sort them to avoid deadlocks when multiple tasks try to insert
        // the same messages in a different order
        .sorted_by_key(|message| message.message)
        .collect_vec();

    if !new_messages.is_empty() {
        let inserted_messages = diesel::insert_into(cm_dsl::cheer_messages)
            .values(&new_messages)
            .on_conflict(cm_dsl::message)
            .do_nothing()
            .returning(DbCheerMessage::as_returning())
            .get_results::<DbCheerMessage>(conn)?;

        // The behavior of "on conflict do nothing" is to return no values when
        // there was a conflict
        if inserted_messages.len() < new_messages.len() {
            return Ok(OrRetry::Retry);
        }

        cheer_table.extend(inserted_messages.into_iter().map(|cheer| {
            let id = cheer.id; // Lifetime reasons
            (cheer.message, id)
        }));
    }

    assert!(messages.iter().all(|m| cheer_table.contains_key(m)));

    Ok(OrRetry::Result(cheer_table))
}
