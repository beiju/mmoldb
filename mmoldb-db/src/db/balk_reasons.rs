// This entire file is a copy of weather.rs with changed types, because generic
// Diesel is hard
use crate::models::{DbBalkReason, NewBalkReason};
use diesel::connection::DefaultLoadingMode;
use diesel::prelude::*;
use diesel::{QueryResult, RunQueryDsl};
use hashbrown::{HashMap, HashSet};
use itertools::Itertools;
use tracing::info;

pub(crate) type BalkReasonTable = HashMap<String, i64>;

pub(crate) fn create_balk_reasons_table(
    conn: &mut PgConnection,
    balk_reasons: &HashSet<String>,
) -> QueryResult<BalkReasonTable> {
    if balk_reasons.is_empty() {
        return Ok(BalkReasonTable::new())
    }

    let balk_reason_table = loop {
        let result = conn.transaction(|conn| create_balk_reasons_table_inner(conn, balk_reasons))?;
        match result {
            OrRetry::Result(balk_reason_table) => break balk_reason_table,
            OrRetry::Retry => {
                info!("Unique constraint violation while adding a new balk reason. Trying again.");
            }
        };
    };

    Ok(balk_reason_table)
}

enum OrRetry<T> {
    Result(T),
    Retry,
}

fn create_balk_reasons_table_inner(
    conn: &mut PgConnection,
    balk_reasons: &HashSet<String>,
) -> QueryResult<OrRetry<BalkReasonTable>> {
    use crate::data_schema::data::balk_reasons::dsl as ch_dsl;

    // Populate with the balk_reasons that are there already
    let mut balk_reason_table = ch_dsl::balk_reasons
        .filter(ch_dsl::balk_reason.eq_any(balk_reasons))
        .select(DbBalkReason::as_select())
        .load_iter::<DbBalkReason, DefaultLoadingMode>(conn)?
        .map_ok(|balk_reason| (balk_reason.balk_reason, balk_reason.id))
        .collect::<QueryResult<HashMap<String, i64>>>()?;

    let new_balk_reasons = balk_reasons.iter()
        .filter_map(|balk_reason| if balk_reason_table.contains_key(balk_reason) {
            None
        } else {
            Some(NewBalkReason { balk_reason })
        })
        // Sort them to avoid deadlocks when multiple tasks try to insert
        // the same balk_reasons in a different order
        .sorted_by_key(|balk_reason| balk_reason.balk_reason)
        .collect_vec();

    if !new_balk_reasons.is_empty() {
        let inserted_balk_reasons = diesel::insert_into(ch_dsl::balk_reasons)
            .values(&new_balk_reasons)
            .on_conflict(ch_dsl::balk_reason)
            .do_nothing()
            .returning(DbBalkReason::as_returning())
            .get_results::<DbBalkReason>(conn)?;

        // The behavior of "on conflict do nothing" is to return no values when
        // there was a conflict
        if inserted_balk_reasons.len() < new_balk_reasons.len() {
            return Ok(OrRetry::Retry);
        }

        balk_reason_table.extend(inserted_balk_reasons.into_iter().map(|balk_reason| {
            let id = balk_reason.id; // Lifetime reasons
            (balk_reason.balk_reason, id)
        }));
    }

    assert!(balk_reasons.iter().all(|m| balk_reason_table.contains_key(m)));

    Ok(OrRetry::Result(balk_reason_table))
}
