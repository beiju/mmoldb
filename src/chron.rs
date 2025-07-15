use chrono::{DateTime, Utc};
use humansize::{DECIMAL, format_size};
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use std::error::Error;
use futures::{stream, Stream, StreamExt};
use miette::Diagnostic;
use thiserror::Error;

#[derive(Debug, Error, Diagnostic)]
pub enum ChronError {
    #[error("Error building Chron request: {0}")]
    RequestBuildError(reqwest::Error),

    #[error("Error searching cache for games page: {0}")]
    CacheGetError(sled::Error),

    #[error("Error executing Chron request: {0}")]
    RequestExecuteError(reqwest::Error),

    #[error("Error deserializing Chron response: {0}")]
    RequestDeserializeError(reqwest::Error),

    #[error("Error encoding Chron response for cache: {0}")]
    CacheSerializeError(rmp_serde::encode::Error),

    #[error("Error inserting games page into cache: {0}")]
    CachePutError(sled::Error),

    #[error("Error removing invalid games page from cache: {0}")]
    CacheRemoveError(sled::Error),

    #[error("Error flushing cache to disk: {0}")]
    CacheFlushError(sled::Error),
}



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
    Stage1DeserializeError(#[source] serde_json::Error),

    #[error("error deserializing Chron entity")]
    Stage2DeserializeError(#[source] serde_json::Error),

    #[error("error re-serializing Chron entity for display")]
    ReserializeForDisplayError {
        #[source]
        err: serde_json::Error,
        from_err: serde_json::Error,
    },
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
    cache: Option<sled::Db>,
    client: reqwest::Client,
    page_size: usize,
    page_size_string: String,
}

#[derive(Debug, Serialize, Deserialize)]
enum VersionedCacheEntry<T> {
    V0(T),
}

pub trait GameExt {
    /// Returns true for any game which will never be updated. This includes all
    /// finished games and a set of games from s0d60 that the sim lost track of
    /// and will never be finished.
    fn is_terminal(&self) -> bool;

    /// True for any game in the "Completed" state
    fn is_completed(&self) -> bool;
}

impl GameExt for mmolb_parsing::Game {
    fn is_terminal(&self) -> bool {
        // There are some games from season 0 that aren't completed and never
        // will be.
        self.season == 0 || self.is_completed()
    }

    fn is_completed(&self) -> bool {
        self.state == "Complete"
    }
}

impl Chron {
    pub fn new<P: AsRef<std::path::Path>>(
        use_cache: bool,
        cache_path: P,
        page_size: usize,
    ) -> Result<Self, sled::Error> {
        let cache = if use_cache {
            info!(
                "Opening cache db. This can be very, very slow for unknown reasons. Contributions \
                to speed up the cache db would be greatly appreciated.",
            );
            let cache_path = cache_path.as_ref();
            let open_cache_start = Utc::now();
            let cache = sled::open(cache_path)?;
            let open_cache_duration = (Utc::now() - open_cache_start).as_seconds_f64();
            if cache.was_recovered() {
                match cache.size_on_disk() {
                    Ok(size) => {
                        info!(
                            "Opened existing {} cache at {cache_path:?} in {open_cache_duration}s",
                            format_size(size, DECIMAL),
                        );
                    }
                    Err(err) => {
                        info!(
                            "Opened existing cache at {cache_path:?} in {open_cache_duration}s. \
                            Error retrieving size: {err}",
                        );
                    }
                }
            } else {
                info!("Created new cache at {cache_path:?}");
            }

            Some(cache)
        } else {
            None
        };

        Ok(Self {
            cache,
            client: reqwest::Client::new(),
            page_size,
            page_size_string: page_size.to_string(),
        })
    }

    fn entities_request(
        &self,
        kind: &str,
        count: &str,
        page: Option<&str>,
    ) -> reqwest::RequestBuilder {
        let request = self
            .client
            .get("https://freecashe.ws/api/chron/v0/entities")
            .query(&[("kind", kind), ("count", count), ("order", "asc")]);

        if let Some(page_token) = page {
            request.query(&[("page", page_token)])
        } else {
            request
        }
    }

    fn get_cached<T: for<'d> Deserialize<'d>>(&self, key: &str) -> Result<Option<T>, ChronError> {
        let Some(cache) = &self.cache else {
            return Ok(None);
        };

        let Some(cache_entry) = cache.get(key).map_err(ChronError::CacheGetError)? else {
            return Ok(None);
        };

        let versions = match rmp_serde::from_slice(&cache_entry) {
            Ok(versions) => versions,
            Err(err) => {
                warn!(
                    "Cache entry could not be decoded: {:?}. Removing it from the cache.",
                    err
                );
                cache.remove(key).map_err(ChronError::CacheRemoveError)?;
                return Ok(None);
            }
        };

        match versions {
            VersionedCacheEntry::V0(data) => Ok(Some(data)),
        }
    }

    pub async fn games_page(
        &self,
        page: Option<&str>,
    ) -> Result<ChronEntities<mmolb_parsing::Game>, ChronError> {
        let request = self
            .entities_request("game", &self.page_size_string, page.as_deref())
            .build()
            .map_err(ChronError::RequestBuildError)?;
        let url = request.url().to_string();
        let result = if let Some(cache_entry) = self.get_cached(&url)? {
            info!("Returning page {page:?} from cache");
            Ok(cache_entry)
        } else {
            // Cache miss -- request from chron
            info!("Requesting page {page:?} from chron");
            let response = self
                .client
                .execute(request)
                .await
                .map_err(ChronError::RequestExecuteError)?;
            let entities: ChronEntities<mmolb_parsing::Game> =
                response.json().await.map_err(|e| {
                    if let Some(source_err) = e.source() {
                        error!("Failed to parse Chron response: {source_err}");
                    };

                    ChronError::RequestDeserializeError(e)
                })?;

            let Some(cache) = &self.cache else {
                return Ok(entities);
            };

            if entities.next_page.is_none() {
                info!("Not caching page {page:?} because it's the last page");
                return Ok(entities);
            }

            if entities.items.len() != self.page_size {
                info!("Not caching page {page:?} because it's the last page");
                return Ok(entities);
            }

            let has_incomplete_game = entities.items.iter().any(|item| !item.data.is_terminal());
            if has_incomplete_game {
                info!(
                    "Not caching page {page:?} because it contains at least one non-terminal game"
                );
                return Ok(entities);
            }

            // Otherwise, save to cache
            let cache_entry = VersionedCacheEntry::V0(entities);

            // Save to cache
            let entities_bin =
                rmp_serde::to_vec(&cache_entry).map_err(ChronError::CacheSerializeError)?;
            cache
                .insert(url.as_str(), entities_bin.as_slice())
                .map_err(ChronError::CachePutError)?;

            // Immediately fetch again from cache to verify everything is working
            let entities = self
                .get_cached(&url.as_str())
                .expect("Error getting cache entry immediately after it was saved")
                .expect("Cache entry was not found immediately after it was saved");

            Ok(entities)
        };

        if let Some(cache) = &self.cache {
            // Fetches are already so slow that cache flushing should be a drop in the bucket. Non-fetch
            // requests shouldn't dirty the cache at all and so this should be near-instant.
            cache.flush().map_err(ChronError::CacheFlushError)?;
        }

        result
    }

    fn pages<T: for<'de> serde::Deserialize<'de> + 'static>(&self, url: &'static str, kind: &'static str, start_at: Option<DateTime<Utc>>) -> impl Stream<Item = Result<Vec<ChronEntity<T>>, ChronStreamError>> {
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
                            return Some((Err(err), None))
                        }

                    }
                    Err(err) => {
                        // Yield the current error, then end iteration
                        return Some((Err(ChronStreamError::JoinFailure(err)), None))
                    }
                };

                if let Some(next_page_token) = page.next_page {
                    if page.items.len() >= page_size {
                        // Then there are more pages
                        let next_page_fut = tokio::spawn(async move {
                            get_next_page(client, url, kind, page_size, start_at_for_first_fetch, Some(next_page_token))
                        });

                        Some((Ok(page.items), Some(next_page_fut)))
                    } else {
                        // Then this was the last page. Yield this page, but there is no next page
                        Some((Ok(page.items), None))
                    }
                } else {
                    // If there's no next page token it's the last page. Yield this page,
                    // but there is no next page
                    Some((Ok(page.items), None))
                }
            }
        })
    }

    pub fn versions<T: for<'de> serde::Deserialize<'de> + 'static>(&self, kind: &'static str, start_at: Option<DateTime<Utc>>) -> impl Stream<Item = Result<ChronEntity<T>, ChronStreamError>> {
        self.items("https://freecashe.ws/api/chron/v0/versions", kind, start_at)
    }

    pub fn entities<T: for<'de> serde::Deserialize<'de> + 'static>(&self, kind: &'static str, start_at: Option<DateTime<Utc>>) -> impl Stream<Item = Result<ChronEntity<T>, ChronStreamError>> {
        self.items("https://freecashe.ws/api/chron/v0/entities", kind, start_at)
    }

    fn items<T: for<'de> serde::Deserialize<'de> + 'static>(&self, url: &'static str, kind: &'static str, start_at: Option<DateTime<Utc>>) -> impl Stream<Item = Result<ChronEntity<T>, ChronStreamError>> {
        self.pages(url, kind, start_at)
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
    }
}

async fn get_next_page<T: for<'de> serde::Deserialize<'de>>(
    client: reqwest::Client,
    url: &str,
    kind: &str,
    page_size: usize,
    start_at: Option<DateTime<Utc>>, page: Option<String>,
) -> Result<(reqwest::Client, ChronEntities<T>), ChronStreamError> {
    let page_size_string = page_size.to_string();

    let mut request_builder = client
        .get(url)
        .query(&[("kind", kind), ("count", &page_size_string), ("order", "asc")]);

    if let Some(start_at) = start_at {
        request_builder = request_builder.query(&[("after", &start_at.to_rfc3339())]);
    }

    if let Some(page) = page {
        request_builder = request_builder.query(&[("page", &page)]);
    }

    let request = request_builder.build()
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

    let partial_deserialized: ChronEntities<serde_json::Value> = serde_json::from_str(&result)
        .map_err(ChronStreamError::Stage1DeserializeError)?;

    let items = partial_deserialized.items.into_iter()
        .map(|item| {
            let entity = serde_json::from_value::<T>(item.data.clone())
                .map_err(|from_err| {
                    match serde_json::to_string_pretty(&item.data) {
                        Ok(pretty_ser) => {
                            println!("Error deserializing {{{pretty_ser}}}");

                            ChronStreamError::Stage2DeserializeError(from_err)
                        }
                        Err(err) => {
                            ChronStreamError::ReserializeForDisplayError {
                                err,
                                from_err,
                            }
                        }
                    }
                })?;

            Ok(ChronEntity {
                kind: item.kind,
                entity_id: item.entity_id,
                valid_from: item.valid_from,
                valid_until: item.valid_until,
                data: entity,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok((client, ChronEntities {
        items,
        next_page: partial_deserialized.next_page,
    }))
}