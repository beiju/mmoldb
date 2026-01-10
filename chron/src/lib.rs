use std::future;
use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt, stream, TryStreamExt};
use log::{debug, warn};
use miette::Diagnostic;
use serde::{Deserialize, Serialize};
use thiserror::Error;

const CUTOVER_DATE: &str = "2025-09-13T22:02:43.355548Z";
const CUTBACK_DATE: &str = "2025-10-27T11:16:00.000Z";

// This is one millisecond after the last game Chron has before it ran out of disk space in
// December 2025. All the other entities have times that are later, so I can be sure that they're
// accurate for this game. I can't be sure that they're accurate for the next game, so that's why
// I chose to use this cutoff date.
const CUTOVER_DATE_2: &str = "2025-12-28T00:47:38.244248Z";

#[derive(Debug, Error, Diagnostic)]
pub enum ChronStreamError {
    #[error("background fetch task exited abnormally")]
    JoinFailure(#[source] tokio::task::JoinError),

    #[error("error building Chron request")]
    RequestBuildError(#[source] reqwest::Error),

    #[error("error executing Chron request")]
    RequestExecuteError(#[source] reqwest::Error),

    #[error("chron reported a server error")]
    ChronStatusError(#[source] reqwest::Error),

    #[error("error extracting response body")]
    RequestBodyError(#[source] reqwest::Error),

    #[error("error deserializing Chron response")]
    RequestDeserializeError(#[source] reqwest::Error),

    #[error("error deserializing Chron response structure")]
    DeserializeError(#[source] serde_json::Error),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChronEntities<EntityT> {
    pub items: Vec<ChronEntity<EntityT>>,
    pub next_page: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChronEntity<EntityT> {
    pub kind: String,
    pub entity_id: String,
    pub valid_from: DateTime<Utc>,
    pub valid_to: Option<DateTime<Utc>>,
    pub data: EntityT,
}

pub struct Chron {
    client: reqwest::Client,
    page_size: usize,
}

impl Chron {
    pub fn new(page_size: usize) -> Self {
        Self {
            client: reqwest::Client::new(),
            page_size,
        }
    }

    pub fn versions(
        &self,
        kind: &'static str,
        start_at: Option<DateTime<Utc>>,
        max_retries: usize,
        use_local_cheap_cashews: bool,
    ) -> impl Stream<Item = Result<ChronEntity<serde_json::Value>, ChronStreamError>> {
        let free_cashews_url = "https://freecashe.ws/api/chron/v0/versions";
        let cheap_cashews_url = if use_local_cheap_cashews {
            "http://10.0.0.71:3001/chron/v0/versions"
        } else {
            "https://cheapcashews.beiju.me/chron/v0/versions"
        };

        self.chained_api_call(
            kind,
            start_at,
            max_retries,
            free_cashews_url,
            cheap_cashews_url,
        )
    }

    pub fn entities(
        &self,
        kind: &'static str,
        start_at: Option<DateTime<Utc>>,
        max_retries: usize,
        use_local_cheap_cashews: bool,
    ) -> impl Stream<Item = Result<ChronEntity<serde_json::Value>, ChronStreamError>> {
        let free_cashews_url = "https://freecashe.ws/api/chron/v0/entities";
        let cheap_cashews_url = if use_local_cheap_cashews {
            "http://10.0.0.71:3001/chron/v0/entities"
        } else {
            "https://cheapcashews.beiju.me/chron/v0/entities"
        };

        self.chained_api_call(
            kind,
            start_at,
            max_retries,
            free_cashews_url,
            cheap_cashews_url,
        )
    }

    pub async fn entities_by_id(
        &self,
        kind: &'static str,
        ids: &[&str],
    ) -> Result<ChronEntities<serde_json::Value>, ChronStreamError> {
        debug!("Fetching {} {kind} entities", ids.len());
        let client = self.client.clone(); // This is internally reference counted

        let request_builder = client.get("https://freecashe.ws/api/chron/v0/entities").query(&[
            ("kind", kind),
            ("id", &ids.join(",")),
            ("order", "asc"),
        ]);

        let request = request_builder
            .build()
            .map_err(ChronStreamError::RequestBuildError)?;

        let response = client
            .execute(request)
            .await
            .map_err(ChronStreamError::RequestExecuteError)?
            .error_for_status()
            .map_err(ChronStreamError::ChronStatusError)?;

        let result = response
            .text()
            .await
            .map_err(ChronStreamError::RequestBodyError)?;

        let items: ChronEntities<serde_json::Value> =
            serde_json::from_str(&result).map_err(ChronStreamError::DeserializeError)?;

        Ok(items)
    }

    fn chained_api_call(
        &self,
        kind: &'static str,
        start_at: Option<DateTime<Utc>>,
        max_retries: usize,
        free_cashews_url: &'static str,
        cheap_cashews_url: &'static str,
    ) -> impl Stream<Item = Result<ChronEntity<serde_json::Value>, ChronStreamError>> {

        let segments = vec![
            // Start with freecashews and stick with it until CUTOVER_DATE
            (free_cashews_url, Some(CUTOVER_DATE)),
            // Then go with cheapcashews until CUTBACK_DATE
            (cheap_cashews_url, Some(CUTBACK_DATE)),
            // Then back to freecashews until CUTOVER_DATE_2
            (free_cashews_url, Some(CUTOVER_DATE_2)),
            // Finally (for now) back to cheapcashews with no end date
            (cheap_cashews_url, None),
        ];

        // The for loop below requires that segment_end not be None except for the last. That's
        // enforced here
        assert!(segments.iter().rev().skip(1).all(|(_, end_time)| end_time.is_some()));
        assert!(segments.iter().rev().take(1).all(|(_, end_time)| end_time.is_none()));

        // I tried to write this with combinators but it bounced off my brain
        let mut streams = Vec::new();
        let mut segment_start = start_at;
        for (url, segment_end_str) in segments {
            let segment_end = segment_end_str.map(|segment_end_str| {
                DateTime::parse_from_rfc3339(segment_end_str)
                    .expect("Hard-coded cutover or cutback date must parse")
                    .with_timezone(&Utc)
            });

            // If this segment starts after it ends, it's gotta be empty
            if segment_start.is_some_and(|segment_start| segment_end.is_some_and(|segment_end| segment_start > segment_end)) {
                continue;
            }

            // Otherwise, we should do some API calls about it
            debug!("Making paginated Chron API call for kind={kind} to {url} from date {segment_start:?} to {segment_end:?}");
            streams.push(self.items(url, kind, max_retries, segment_start, segment_end));

            // Next segment starts when this one ends. Note that this assignment does not happen if
            // the start date is after the end date due to the continue; above. That's important.
            // This also breaks if a non-terminal segment has a None segment_end, which is checked
            // before this loop begins.
            segment_start = segment_end;
        }

        stream::iter(streams).flatten()
    }

    fn items(
        &self,
        url: &'static str,
        kind: &'static str,
        max_retries: usize,
        start_at: Option<DateTime<Utc>>,
        end_at: Option<DateTime<Utc>>,
    ) -> impl Stream<Item = Result<ChronEntity<serde_json::Value>, ChronStreamError>> {
        self.pages(url, kind, max_retries, start_at, end_at)
            .flat_map(|val| match val {
                Ok(vec) => {
                    // Turn Vec<T> into a stream of Result<T, E>
                    let results = vec.into_iter().map(Ok);
                    stream::iter(results).left_stream()
                }
                Err(e) => {
                    // Return a single error, as a stream
                    stream::once(async { Err(e) }).right_stream()
                }
            })
            // We shouldn't get items past end_at from the api, but cut them off
            // just in case
            .try_take_while(move |entity| {
                if end_at.is_some_and(|e| entity.valid_from > e) {
                    warn!("API gave us a version that started past the `before` parameter");
                    future::ready(Ok(false))
                } else {
                    future::ready(Ok(true))
                }
            })
    }

    fn pages(
        &self,
        url: &'static str,
        kind: &'static str,
        max_retries: usize,
        start_at: Option<DateTime<Utc>>,
        end_at: Option<DateTime<Utc>>,
    ) -> impl Stream<Item = Result<Vec<ChronEntity<serde_json::Value>>, ChronStreamError>> {
        // For lifetimes
        let page_size = self.page_size;
        let client = self.client.clone(); // This is internally reference counted

        // Use tokio::spawn to eagerly fetch the next page while the caller is doing other work
        let start_at_for_first_fetch = start_at;
        let next_page = tokio::spawn(async move {
            get_next_page_with_retries(client, url, kind, max_retries, page_size, start_at_for_first_fetch, end_at, None)
        });

        // I do not understand why a non-async closure with an async block inside works,
        // but an async closure does not. Nevertheless, that's the situation.
        stream::unfold(Some(next_page), move |next_page| {
            async move {
                let Some(next_page) = next_page else {
                    // next_page being None indicates that we've finished. We couldn't
                    // end the stream before because we hadn't produced the current page
                    // yet.
                    debug!("Stream of pages has finished");
                    return None;
                };

                // Can't use ? in here because the closure must return an Option.
                // Note the double nesting is because the join can fail, and the
                // join can succeed but the underlying task produced an error.
                let (client, page) = match next_page.await {
                    Ok(fut) => match fut.await {
                        Ok(page) => page,
                        Err(err) => {
                            // Yield the current error, then end iteration
                            debug!("Stream of pages is yielding an error");
                            return Some((Err(err), None));
                        }
                    },
                    Err(err) => {
                        // Yield the current error, then end iteration
                        debug!("Stream of pages is yielding an error");
                        return Some((Err(ChronStreamError::JoinFailure(err)), None));
                    }
                };

                if let Some(next_page_token) = page.next_page {
                    if page.items.len() >= page_size {
                        // Then there are more pages
                        let next_page_fut = tokio::spawn(async move {
                            get_next_page_with_retries(
                                client,
                                url,
                                kind,
                                max_retries,
                                page_size,
                                start_at_for_first_fetch,
                                end_at,
                                Some(next_page_token),
                            )
                        });

                        debug!("Yielding a page");
                        Some((Ok(page.items), Some(next_page_fut)))
                    } else {
                        // Then this was the last page. Yield this page, but there is no next page
                        debug!("Yielding the last page");
                        Some((Ok(page.items), None))
                    }
                } else {
                    // If there's no next page token it's the last page. Yield this page,
                    // but there is no next page
                    debug!("Yielding the last page");
                    Some((Ok(page.items), None))
                }
            }
        })
    }
}

async fn get_next_page_with_retries(
    client: reqwest::Client,
    url: &str,
    kind: &str,
    max_retries: usize,
    page_size: usize,
    start_at: Option<DateTime<Utc>>,
    end_at: Option<DateTime<Utc>>,
    page: Option<String>,
) -> Result<(reqwest::Client, ChronEntities<serde_json::Value>), ChronStreamError> {
    let mut retries = 0;
    loop {
        match get_next_page(&client, url, kind, page_size, start_at, end_at, page.as_deref()).await {
            Ok(next_page) => return Ok((client, next_page)),
            Err(e) => {
                if retries < max_retries {
                    warn!("Chron encountered an error, will try again up to {} more times: {:?}", max_retries - retries, e);
                    retries += 1;
                } else {
                    return Err(e);
                }
            }
        }
    }
}

async fn get_next_page(
    client: &reqwest::Client,
    url: &str,
    kind: &str,
    page_size: usize,
    start_at: Option<DateTime<Utc>>,
    end_at: Option<DateTime<Utc>>,
    page: Option<&str>,
) -> Result<ChronEntities<serde_json::Value>, ChronStreamError> {
    debug!("Fetching {kind} page {page:?} starting at {start_at:?}");

    let page_size_string = page_size.to_string();

    let mut request_builder = client.get(url).query(&[
        ("kind", kind),
        ("count", &page_size_string),
        ("order", "asc"),
    ]);

    if let Some(start_at) = start_at {
        request_builder = request_builder.query(&[("after", &start_at.to_rfc3339())]);
    }

    if let Some(end_at) = end_at {
        request_builder = request_builder.query(&[("before", &end_at.to_rfc3339())]);
    }

    if let Some(page) = page {
        request_builder = request_builder.query(&[("page", page)]);
    }

    let request = request_builder
        .build()
        .map_err(ChronStreamError::RequestBuildError)?;

    debug!("Requesting {}", request.url());
    let response = client
        .execute(request)
        .await
        .map_err(ChronStreamError::RequestExecuteError)?
        .error_for_status()
        .map_err(ChronStreamError::ChronStatusError)?;

    let result = response
        .text()
        .await
        .map_err(ChronStreamError::RequestBodyError)?;

    let items: ChronEntities<serde_json::Value> =
        serde_json::from_str(&result).map_err(ChronStreamError::DeserializeError)?;

    Ok(items)
}
