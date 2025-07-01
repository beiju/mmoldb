mod models;

pub use models::*;

use std::error::Error;
use chrono::{DateTime, Utc};
use humansize::{DECIMAL, format_size};
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use std::io;
use futures::{stream, Stream, stream::StreamExt};
use rocket::tokio;
use strum::IntoDiscriminant;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ChronStartupError {
    #[error("Couldn't create parent folder for HTTP cache database: {0}")]
    CreateDbFolder(io::Error),

    #[error(transparent)]
    OpenDb(#[from] heed::Error),
}

#[derive(Debug, Error)]
pub enum ChronError {
    #[error("Error building Chron request: {0}")]
    RequestBuildError(reqwest::Error),

    #[error("Cache database error: {0}")]
    CacheDbError(#[from] heed::Error),

    #[error("Error executing Chron request: {0}")]
    RequestExecuteError(reqwest::Error),

    #[error("Error deserializing Chron response: {0}")]
    RequestDeserializeError(reqwest::Error),
}

#[derive(Debug, Error)]
pub enum ChronStreamError {
    #[error("Background fetch task exited abnormally: {0}")]
    JoinFailure(tokio::task::JoinError),

    #[error("Error building Chron request: {0}")]
    RequestBuildError(reqwest::Error),

    #[error("Error executing Chron request: {0}")]
    RequestExecuteError(reqwest::Error),

    #[error("Chron reported error: {0}")]
    ChronStatusError(reqwest::Error),

    #[error("Error deserializing Chron response: {0}")]
    RequestDeserializeError(reqwest::Error),
}

pub struct Chron {
    cache: Option<(heed::Env, heed::Database<heed::types::Str, heed::types::SerdeRmp<VersionedCacheEntry<ChronEntities<mmolb_parsing::Game>>>>)>,
    client: reqwest::Client,
    page_size: usize,
    page_size_string: String,
}

#[derive(Debug, Serialize, Deserialize)]
enum VersionedCacheEntry<T> {
    V0(T),
    V1(GenericChronEntities)
}

impl Chron {
    pub fn new<P: AsRef<std::path::Path>>(
        use_cache: bool,
        cache_path: P,
        page_size: usize,
    ) -> Result<Self, ChronStartupError> {
        let cache = if use_cache {
            info!("Opening LMDB db");
            let cache_path = cache_path.as_ref().join("http-cache-v2");
            match std::fs::create_dir(&cache_path) {
                Ok(()) => info!("Created HTTP cache directory: {}", &cache_path.display()),
                Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
                    info!("HTTP cache directory already exists: {}", &cache_path.display())
                },
                Err(err) => return Err(ChronStartupError::CreateDbFolder(err)),
            }
            let open_cache_start = Utc::now();
            let env = unsafe {
                heed::EnvOpenOptions::new()
                    // Sets the maximum size of the database
                    .map_size(1024 * 1024 * 1024 * 100) // 100 GiB
                    // I think unnamed DB counts towards the limit
                    .max_dbs(2)
                    .open(&cache_path)
            }?;
            let open_cache_duration = (Utc::now() - open_cache_start).as_seconds_f64();

            match env.real_disk_size() {
                Ok(size) => {
                    info!(
                        "Opened {} cache at {cache_path:?} in {open_cache_duration}s",
                        format_size(size, DECIMAL),
                    );
                }
                Err(err) => {
                    info!(
                        "Opened cache at {cache_path:?} in {open_cache_duration}s. \
                        Error retrieving size: {err}",
                    );
                }
            }

            let mut create_db_txn = env.write_txn()?;

            let db = env.database_options()
                .types() // Deduced from the assignment to Chron::cache
                .name("chron-response-cache")
                .create(&mut create_db_txn)?;

            create_db_txn.commit()?;

            Some((env, db))
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

    fn versions_request(
        &self,
        kind: &str,
        count: &str,
        page: Option<&str>,
    ) -> reqwest::RequestBuilder {
        let request = self
            .client
            .get("https://freecashe.ws/api/chron/v0/versions")
            .query(&[("kind", kind), ("count", count), ("order", "asc")]);

        if let Some(page_token) = page {
            request.query(&[("page", page_token)])
        } else {
            request
        }
    }

    fn get_cached(&self, key: &str) -> Result<Option<ChronEntities<mmolb_parsing::Game>>, ChronError> {
        let total_start = Utc::now();
        let Some((env, db)) = &self.cache else {
            return Ok(None);
        };

        let open_read_txn_start = Utc::now();
        let read_txn = env.read_txn()?;
        let open_read_txn_duration = (Utc::now() - open_read_txn_start).as_seconds_f64();

        let get_start = Utc::now();
        let Some(cache_entry) = db.get(&read_txn, key)? else {
            let get_duration = (Utc::now() - get_start).as_seconds_f64();

            let commit_read_txn_start = Utc::now();
            read_txn.commit()?;
            let commit_read_txn_duration = (Utc::now() - commit_read_txn_start).as_seconds_f64();

            let total_duration = (Utc::now() - total_start).as_seconds_f64();
            info!("Processed cache miss in {total_duration:.3}s ({open_read_txn_duration:.3}s to open txn, {get_duration:.3}s to get(), {commit_read_txn_duration:.3}s to commit txn)");
            return Ok(None);
        };
        let get_duration = (Utc::now() - get_start).as_seconds_f64();

        let commit_read_txn_start = Utc::now();
        read_txn.commit()?;
        let commit_read_txn_duration = (Utc::now() - commit_read_txn_start).as_seconds_f64();

        let total_duration = (Utc::now() - total_start).as_seconds_f64();
        info!("Fetched cache entry in {total_duration:.3}s (\
                {open_read_txn_duration:.3}s to open txn, \
                {get_duration:.3}s to get(), \
                {commit_read_txn_duration:.3}s to commit read txn\
            )");

        match cache_entry {
            VersionedCacheEntry::V0(data) => Ok(Some(data)),
            VersionedCacheEntry::V1(GenericChronEntities::Game(data)) => Ok(Some(data)),
            VersionedCacheEntry::V1(other_type) => {
                warn!(
                    "We wanted this cache entry to have type {:?}, but it had type {:?}. \
                    This entry will be removed.",
                    GenericChronEntitiesDiscriminants::Game, other_type.discriminant(),
                );

                let mut write_txn = env.write_txn()?;
                db.delete(&mut write_txn, key)?;
                write_txn.commit()?;

                Ok(None)
            },
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

            let Some((env, db)) = &self.cache else {
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
            let cache_entry = VersionedCacheEntry::V1(GenericChronEntities::Game(entities));

            // Save to cache
            let mut write_txn = env.write_txn()?;
            db.put(&mut write_txn, url.as_str(), &cache_entry)?;
            write_txn.commit()?;

            // Immediately fetch again from cache to verify everything is working
            let entities = self
                .get_cached(&url.as_str())
                .expect("Error getting cache entry immediately after it was saved")
                .expect("Cache entry was not found immediately after it was saved");

            Ok(entities)
        };

        result
    }

    fn player_version_pages(&self, start_at: Option<DateTime<Utc>>) -> impl Stream<Item = Result<Vec<ChronEntity<ChronPlayer>>, ChronStreamError>> {
        // For lifetimes
        let page_size = self.page_size;
        let client = self.client.clone(); // This is internally reference counted
        
        // Use tokio::spawn to eagerly fetch the next page while the caller is doing other work
        let start_at_for_first_fetch = start_at;
        let next_page = tokio::spawn(async move {
            next_page_of_player_versions(client, page_size, start_at_for_first_fetch, None)
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
                        // Then this was the last page. Yield this page, but there is no next page
                        Some((Ok(page.items), None))
                    } else {
                        // Then there are more pages
                        let next_page_fut = tokio::spawn(async move {
                            next_page_of_player_versions(client, page_size, start_at_for_first_fetch, Some(next_page_token))
                        });
    
                        Some((Ok(page.items), Some(next_page_fut)))
                    }
                } else {
                    // If there's no next page token it's the last page. Yield this page,
                    // but there is no next page
                    Some((Ok(page.items), None))
                }
            }
        })
    }

    pub fn player_versions(&self, start_at: Option<DateTime<Utc>>) -> impl Stream<Item = Result<ChronEntity<ChronPlayer>, ChronStreamError>> {
        self.player_version_pages(start_at)
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

async fn next_page_of_player_versions(
    client: reqwest::Client,
    page_size: usize,
    start_at: Option<DateTime<Utc>>, page: Option<String>,
) -> Result<(reqwest::Client, ChronEntities<ChronPlayer>), ChronStreamError> {
    let page_size_string = page_size.to_string();
    
    let mut request_builder = client
        .get("https://freecashe.ws/api/chron/v0/versions")
        .query(&[("kind", "player"), ("count", &page_size_string), ("order", "asc")]);

    if let Some(start_at) = start_at {
        request_builder = request_builder.query(&[("after", &start_at.to_string())]);
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
        .json()
        .await
        .map_err(ChronStreamError::RequestDeserializeError)?;
    
    Ok((client, result))
}
