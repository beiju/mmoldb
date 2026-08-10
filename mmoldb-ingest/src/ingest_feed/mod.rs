use rayon::iter::ParallelIterator;
use futures::FutureExt;
use crate::{IngestFatalError, ProcessingArgs, VersionIngestLogs};
use chron::ChronFeedEvent;
use chrono::Utc;
use futures::{StreamExt, TryStreamExt, pin_mut};
use itertools::Itertools;
use rayon::iter::IntoParallelIterator;
use serde::de::IntoDeserializer;
use mmoldb_db::models::{NewFeedEventProcessed, NewPlayerAttributeAugment, NewPlayerParadigmShift, NewPlayerRecomposition, NewTeamGamePlayed, NewVersionIngestLog};
use mmoldb_db::{async_db, db, AsyncConnection, AsyncPgConnection};
use tracing::info;
use mmoldb_db::taxa::Taxa;

mod ingest_feed_shared;
mod ingest_team_feed;
mod ingest_player_feed;


pub async fn process_feed(
    args: ProcessingArgs,
) -> Result<(), IngestFatalError> {
    let mut conn = args.pool.get()?;
    let taxa = Taxa::new(&mut conn)?;

    // TODO Pool this too?
    let url = mmoldb_db::postgres_url_from_environment();
    let mut async_conn = AsyncPgConnection::establish(&url).await?;

    let feed_events_stream = async_db::stream_unprocessed_feed_events(
        &mut async_conn,
    )
        .await?
        .take_until(args.shutdown_requested.cancelled().then(|()| async {
            info!(
                "Closing feed event processing stream because shutdown was requested",
            );
        }))
        .try_chunks(args.process_batch_size.into());
    pin_mut!(feed_events_stream);

    let mut wait_for_chunk_start = Utc::now();
    while let Some(chunk) = feed_events_stream.next().await {
        let wait_for_chunk_duration = (Utc::now() - wait_for_chunk_start).as_seconds_f64();
        let save_start = Utc::now();

        // When a chunked stream encounters an error, it returns the portion
        // of the chunk that was collected before the error and the error
        // itself. We want to insert the successful portion of the chunk,
        // _then_ propagate any error.
        let (chunk, maybe_err): (Vec<ChronFeedEvent<serde_json::Value>>, _) = match chunk {
            Ok(chunk) => (chunk, None),
            Err(err) => (err.0, Some(err.1)),
        };

        info!(
            "Got batch of {} feed events to process in {:.03} seconds",
            chunk.len(),
            wait_for_chunk_duration,
        );

        let earliest_time = chunk
            .first()
            .map(|version| version.timestamp)
            .unwrap_or(Utc::now());
        let earliest_time_ago = earliest_time.signed_duration_since(Utc::now());
        let earliest_human_time_ago = chrono_humanize::HumanTime::from(earliest_time_ago).to_string();
        let latest_time = chunk
            .last()
            .map(|version| version.timestamp)
            .unwrap_or(Utc::now());
        let latest_time_ago = latest_time.signed_duration_since(Utc::now());
        let latest_human_time_ago = chrono_humanize::HumanTime::from(latest_time_ago).to_string();

        // We need the deserialize result to be owned at this level
        let feed_events: Vec<Result<_, _>> = chunk.into_par_iter()
            .map(|event| {
                let des = (&event.data).into_deserializer();
                match serde_path_to_error::deserialize(des) {
                    Ok(data) => {
                        let item: ChronFeedEvent<mmolb_parsing::feed_event::FeedEvent> = ChronFeedEvent {
                            event_id: event.event_id,
                            subject_type: event.subject_type,
                            subject_id: event.subject_id,
                            timestamp: event.timestamp,
                            data,
                        };
                        Ok(item)
                    },
                    Err(err) => Err((err, event)),
                }
            })
            .collect();

        let items = feed_events.iter()
            .map(|result| chron_feed_event_as_new(&taxa, result))
            .collect_vec();

        let to_insert = items.len();
        let (total, inserted) = db::insert_team_feed_versions(&mut conn, &items)?;

        let human_time_ago = if latest_human_time_ago == earliest_human_time_ago {
            format!("{}", latest_human_time_ago)
        } else {
            format!("{} to {}", earliest_human_time_ago, latest_human_time_ago)
        };

        let save_duration = (Utc::now() - save_start).as_seconds_f64();
        info!(
            "Sent rows for {to_insert} new feed events to the database in {:.03} seconds. \
            {inserted}/{total} rows were actually inserted, the rest were duplicates. \
            Currently processing events from {human_time_ago}.",
            save_duration,
        );

        if let Some(err) = maybe_err {
            Err(err)?;
        }

        wait_for_chunk_start = Utc::now();
    }

    info!("Feed stage 2 ingest finished");

    Ok(())
}

fn chron_feed_event_as_new<'e>(
    taxa: &Taxa,
    feed_event: &'e Result<
        ChronFeedEvent<mmolb_parsing::feed_event::FeedEvent>,
        (serde_path_to_error::Error<serde_json::Error>, ChronFeedEvent<serde_json::Value>),
    >,
) -> (
    NewFeedEventProcessed<'e>,
    Option<NewPlayerAttributeAugment<'e>>,
    Option<NewPlayerParadigmShift<'e>>,
    Vec<NewPlayerRecomposition<'e>>,
    Option<NewTeamGamePlayed<'e>>,
    Vec<NewVersionIngestLog<'e>>,
) {
    match feed_event {
        Ok(feed_event) => {
            let processed = NewFeedEventProcessed {
                subject_type: &feed_event.subject_type,
                event_id: &feed_event.event_id,
                timestamp: feed_event.timestamp.naive_utc(),
                skipped: false, // Feed event items are never skipped, I think?
                fatal_error: false, // This is the happy path
            };

            match feed_event.subject_type.as_str() {
                "team" => {
                    let mut ingest_logs = VersionIngestLogs::new("team_feed", &feed_event.event_id, feed_event.timestamp);
                    let new_game_played = ingest_team_feed::chron_team_feed_as_new(feed_event, &mut ingest_logs);

                    (processed, None, None, Vec::new(), new_game_played, ingest_logs.into_vec())
                },
                "player" => {
                    let mut ingest_logs = VersionIngestLogs::new("player_feed", &feed_event.event_id, feed_event.timestamp);
                    let (
                        attribute_augment,
                        paradigm_shift,
                        recompositions,
                    ) = ingest_player_feed::chron_player_feed_as_new(taxa, feed_event, &mut ingest_logs);

                    (processed, attribute_augment, paradigm_shift, recompositions, None, ingest_logs.into_vec())
                },
                other => {
                    // It's not player feed, but if I put it as anything besides "player_feed" and
                    // "team_feed" then I won't ever see the errors. So this is a hack.
                    let mut ingest_logs = VersionIngestLogs::new("player_feed", &feed_event.event_id, feed_event.timestamp);
                    let processed = NewFeedEventProcessed {
                        fatal_error: true,
                        ..processed
                    };
                    ingest_logs.critical(format!("Unexpected feed subject_type: {}", other));
                    (processed, None, None, Vec::new(), None, ingest_logs.into_vec())
                }
            }
        },
        Err((error, feed_event)) => {
            let processed = NewFeedEventProcessed {
                subject_type: &feed_event.subject_type,
                event_id: &feed_event.event_id,
                timestamp: feed_event.timestamp.naive_utc(),
                skipped: false,
                fatal_error: true, // This is the sad path
            };
            let mut ingest_logs = VersionIngestLogs::new("feed", &feed_event.event_id, feed_event.timestamp);
            ingest_logs.critical(format!("Deserialization failed: {}", error));
            (processed, None, None, Vec::new(), None, ingest_logs.into_vec())
        }
    }
}
