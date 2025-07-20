use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt, stream};
use log::{debug, error};
use miette::Diagnostic;
use serde::{Deserialize, Serialize};
use thiserror::Error;

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
    pub valid_until: Option<DateTime<Utc>>,
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
    ) -> impl Stream<Item = Result<ChronEntity<serde_json::Value>, ChronStreamError>> {
        self.items("https://freecashe.ws/api/chron/v0/versions", kind, start_at)
    }

    pub fn entities(
        &self,
        kind: &'static str,
        start_at: Option<DateTime<Utc>>,
    ) -> impl Stream<Item = Result<ChronEntity<serde_json::Value>, ChronStreamError>> {
        self.items("https://freecashe.ws/api/chron/v0/entities", kind, start_at)
    }

    fn items(
        &self,
        url: &'static str,
        kind: &'static str,
        start_at: Option<DateTime<Utc>>,
    ) -> impl Stream<Item = Result<ChronEntity<serde_json::Value>, ChronStreamError>> {
        self.pages(url, kind, start_at).flat_map(|val| match val {
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
    }

    fn pages(
        &self,
        url: &'static str,
        kind: &'static str,
        start_at: Option<DateTime<Utc>>,
    ) -> impl Stream<Item = Result<Vec<ChronEntity<serde_json::Value>>, ChronStreamError>> {
        // For lifetimes
        let page_size = self.page_size;
        let client = self.client.clone(); // This is internally reference counted

        // Use tokio::spawn to eagerly fetch the next page while the caller is doing other work
        let start_at_for_first_fetch = start_at;
        let next_page = tokio::spawn(async move {
            get_next_page(client, url, kind, page_size, start_at_for_first_fetch, None)
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
                            get_next_page(
                                client,
                                url,
                                kind,
                                page_size,
                                start_at_for_first_fetch,
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

async fn get_next_page(
    client: reqwest::Client,
    url: &str,
    kind: &str,
    page_size: usize,
    start_at: Option<DateTime<Utc>>,
    page: Option<String>,
) -> Result<(reqwest::Client, ChronEntities<serde_json::Value>), ChronStreamError> {
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

    if let Some(page) = page {
        request_builder = request_builder.query(&[("page", &page)]);
    }

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

    Ok((client, items))
}
