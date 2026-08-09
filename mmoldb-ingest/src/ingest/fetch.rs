use crate::IngestFatalError;
use chron::{Chron, ChronEntity};
use chrono::NaiveDateTime;
use futures::{FutureExt, StreamExt};
use futures::{TryStreamExt, pin_mut};
use mmoldb_db::{ConnectionPool, db};
use std::num::NonZero;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

#[derive(Debug, Clone)]
pub struct ChronFetchArgs {
    pub shutdown_requested: CancellationToken,
    pub pool: ConnectionPool,
    pub use_local_cheap_cashews: bool,
    pub enabled: bool,
    pub chron_fetch_interval_seconds: u64,
    pub chron_fetch_batch_size: NonZero<usize>,
    pub insert_raw_entity_batch_size: NonZero<usize>,
}

// It may be possible to remove 'static
pub async fn fetch_entity_kind(
    kind: &'static str,
    args: ChronFetchArgs,
) -> Result<(), IngestFatalError> {
    let mut conn = args.pool.get()?;
    let chron = Chron::new(args.chron_fetch_batch_size);

    let start_date = db::get_latest_entity_valid_from(&mut conn, kind)?
        .as_ref()
        .map(NaiveDateTime::and_utc);

    info!("{} fetch will start from date {:?}", kind, start_date);

    let stream = chron
        .entities(kind, start_date, 3, args.use_local_cheap_cashews)
        // End the stream early when cancellation is requested. By ending the stream at this
        // point, we stop waiting for any more network requests but we still process any that
        // are still waiting to be collected in the next try_chunks item.
        .take_until(args.shutdown_requested.cancelled().then(|()| {
            // Some detail of the Rust compiler makes it forget that this is 'static
            // during some important checking phase. The only way I've found to make
            // that not cause issues is to make it an owned value.
            let kind = kind.to_string();
            async move {
                info!(
                    "Closing {} entities fetch stream because shutdown was requested",
                    kind
                );
            }
        }))
        .try_chunks(args.insert_raw_entity_batch_size.into());
    pin_mut!(stream);

    while let Some(chunk) = stream.next().await {
        // When a chunked stream encounters an error, it returns the portion
        // of the chunk that was collected before the error and the error
        // itself. We want to insert the successful portion of the chunk,
        // _then_ propagate any error.
        let (chunk, maybe_err): (Vec<ChronEntity<serde_json::Value>>, _) = match chunk {
            Ok(chunk) => (chunk, None),
            Err(err) => (err.0, Some(err.1)),
        };
        info!("Saving {} {}(s)", chunk.len(), kind);
        let inserted = db::insert_entities(&mut conn, chunk)?;
        info!("Saved {} {}(s)", inserted, kind);

        if let Some(err) = maybe_err {
            Err(err)?;
        }
    }

    Ok(())
}

// It may be possible to remove 'static
pub async fn fetch_version_kind(
    kind: &'static str,
    args: ChronFetchArgs,
) -> Result<(), IngestFatalError> {
    let mut conn = args.pool.get()?;
    let chron = Chron::new(args.chron_fetch_batch_size);

    let start_cursor =
        db::get_latest_raw_version_cursor(&mut conn, kind)?.map(|(dt, id)| (dt.and_utc(), id));
    let start_date = start_cursor.as_ref().map(|(dt, _)| *dt);

    info!("{} fetch will start from date {:?}", kind, start_date);

    let stream = chron
        .versions(kind, start_date, 3, args.use_local_cheap_cashews)
        // End the stream early when cancellation is requested. By ending the stream at this
        // point, we stop waiting for any more network requests but we still process any that
        // are still waiting to be collected in the next try_chunks item.
        .take_until(args.shutdown_requested.cancelled().then(|()| {
            // Some detail of the Rust compiler makes it forget that this is 'static
            // during some important checking phase. The only way I've found to make
            // that not cause issues is to make it an owned value.
            let kind = kind.to_string();
            async move {
                info!(
                    "Closing {} versions fetch stream because shutdown was requested",
                    kind
                );
            }
        }))
        // We ask Chron to start at a given valid_from. It will give us all versions whose
        // valid_from is greater than _or equal to_ that value. That's good, because it
        // means that if we left off halfway through processing a batch of entities with
        // identical valid_from, we won't miss the rest of the batch. However, we will
        // receive the first half of the batch again. Later steps in the ingest code will
        // error if we attempt to ingest a value that's already in the database, so we
        // have to filter them out. That's what this skip_while is doing. It returns a
        // future just because that's what's dictated by the stream api.
        .skip_while(|result| {
            let skip_this =
                start_cursor
                    .as_ref()
                    .is_some_and(|(start_valid_from, start_entity_id)| {
                        result.as_ref().is_ok_and(|entity| {
                            (entity.valid_from, &entity.entity_id)
                                <= (*start_valid_from, start_entity_id)
                        })
                    });

            futures::future::ready(skip_this)
        })
        .try_chunks(args.insert_raw_entity_batch_size.into());
    pin_mut!(stream);

    while let Some(chunk) = stream.next().await {
        // When a chunked stream encounters an error, it returns the portion
        // of the chunk that was collected before the error and the error
        // itself. We want to insert the successful portion of the chunk,
        // _then_ propagate any error.
        let (chunk, maybe_err): (Vec<ChronEntity<serde_json::Value>>, _) = match chunk {
            Ok(chunk) => (chunk, None),
            Err(err) => (err.0, Some(err.1)),
        };
        info!("{kind} stage 1 ingest saving {} {kind}(s)", chunk.len());
        let inserted = match db::insert_versions_one_error(&mut conn, &chunk) {
            Ok(x) => Ok(x),
            Err((entity, err)) => {
                error!("Error in stage 1 ingest write: {err} while inserting: {entity:#?}");
                Err(err)
            }
        }?;
        info!("{kind} stage 1 ingest saved {inserted} {kind}(s)");

        if let Some(err) = maybe_err {
            Err(err)?;
        }
    }

    info!("{} stage 1 ingest finished", kind);
    Ok(())
}

// It may be possible to remove 'static
pub async fn fetch_feed_events(
    args: ChronFetchArgs,
) -> Result<(), IngestFatalError> {
    let mut conn = args.pool.get()?;
    let chron = Chron::new(args.chron_fetch_batch_size);

    let start_cursor = db::get_latest_combined_feed_event_version_cursor(&mut conn)?;
    let start_cursor_utc = start_cursor.as_ref().map(|(dt, id)| (dt.and_utc(), id));

    let start_date = start_cursor_utc.as_ref().map(|(dt, _)| *dt);
    info!("Combined feed event fetch will start from date {:?}", start_date,);

    let stream = chron
        .feed_events(start_date, 3, args.use_local_cheap_cashews)
        // End the stream early when cancellation is requested. By ending the stream at this
        // point, we stop waiting for any more network requests but we still process any that
        // are still waiting to be collected in the next try_chunks item.
        .take_until(args.shutdown_requested.cancelled().then(|()| async {
            info!("Closing feed events fetch stream because shutdown was requested");
        }))
        // We ask Chron to start at a given timestamp. It will give us
        // all feed events whose timestamp is greater than _or equal to_
        // that value. That's good, because it means that if we left
        // off halfway through processing a batch of feed events with
        // identical timestamp, we won't miss the rest of the batch.
        // However, we will receive the first half of the batch again.
        // Later steps in the ingest code will error if we attempt to
        // ingest a value that's already in the database, so we have to
        // filter them out. That's what this skip_while is doing.
        // It returns a future just because that's what's dictated by
        // the stream api.
        .skip_while(|result| {
            let skip_this =
                start_cursor_utc
                    .as_ref()
                    .is_some_and(|(start_timestamp, start_event_id)| {
                        result.as_ref().is_ok_and(|entity| {
                            (entity.timestamp, &entity.event_id)
                                <= (*start_timestamp, start_event_id)
                        })
                    });

            futures::future::ready(skip_this)
        })
        .map(|result| {
            result
                .map_err(IngestFatalError::ChronStreamError)
                .map(|event| (event.event_id, event.subject_type, event.subject_id, event.timestamp.naive_utc(), event.data))
        })
        .try_chunks(args.insert_raw_entity_batch_size.into());
    pin_mut!(stream);

    while let Some(chunk) = stream.next().await {
        // When a chunked stream encounters an error, it returns the portion
        // of the chunk that was collected before the error and the error
        // itself. We want to insert the successful portion of the chunk,
        // _then_ propagate any error.
        let (chunk, maybe_err): (Vec<_>, _) = match chunk {
            Ok(chunk) => (chunk, None),
            Err(err) => (err.0, Some(err.1)),
        };

        assert!(chunk.is_sorted_by_key(|(event_id, _, _, timestamp, _)| (timestamp, event_id)));

        info!("Combined feed stage 1 ingest saving {} feed event(s)", chunk.len());
        let inserted = match db::insert_feed_events(&mut conn, &chunk) {
            Ok(x) => Ok(x),
            Err(err) => {
                error!("Error in stage 1 ingest write: {err}");
                Err(err)
            }
        }?;
        info!("Combined stage 1 ingest saved {inserted} feed event(s)");

        if let Some(err) = maybe_err {
            Err(err)?;
        }
    }

    info!("Combined feed stage 1 ingest finished");
    Ok(())
}