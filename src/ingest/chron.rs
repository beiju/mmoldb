use std::error::Error;
use chrono::{DateTime, Utc};
use humansize::{DECIMAL, format_size};
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use std::io;
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

    #[error("Error encoding Chron response for cache: {0}")]
    CacheSerializeError(rmp_serde::encode::Error),
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
    cache: Option<(heed::Env, heed::Database<heed::types::Str, heed::types::Bytes>)>,
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
                .types::<heed::types::Str, heed::types::Bytes>()
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

    fn get_cached<T: for<'d> Deserialize<'d>>(&self, key: &str) -> Result<Option<T>, ChronError> {
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

        let bindecode_start = Utc::now();
        let (versions, bindecode_duration, commit_read_txn_duration) = match rmp_serde::from_slice(&cache_entry) {
            Ok(versions) => {
                let bindecode_duration = (Utc::now() - bindecode_start).as_seconds_f64();

                let commit_read_txn_start = Utc::now();
                read_txn.commit()?;
                let commit_read_txn_duration = (Utc::now() - commit_read_txn_start).as_seconds_f64();

                (versions, bindecode_duration, commit_read_txn_duration)
            },
            Err(err) => {
                let bindecode_duration = (Utc::now() - bindecode_start).as_seconds_f64();

                let commit_read_txn_start = Utc::now();
                read_txn.commit()?;
                let commit_read_txn_duration = (Utc::now() - commit_read_txn_start).as_seconds_f64();

                warn!(
                    "Cache entry could not be decoded: {:?}. Removing it from the cache.",
                    err
                );
                
                let combined_delete_start = Utc::now();
                let mut write_txn = env.write_txn()?;
                db.delete(&mut write_txn, key)?;
                write_txn.commit()?;
                let combined_delete_duration = (Utc::now() - combined_delete_start).as_seconds_f64();

                let total_duration = (Utc::now() - total_start).as_seconds_f64();
                info!("Processed corrupt cache entry in {total_duration:.3}s (\
                {open_read_txn_duration:.3}s to open txn, \
                {get_duration:.3}s to get(), \
                {bindecode_duration:.3}s to decode, \
                {commit_read_txn_duration:.3}s to commit read txn, \
                {combined_delete_duration:.3}s to delete invalid entry\
                )");
                
                return Ok(None);
            }
        };

        let total_duration = (Utc::now() - total_start).as_seconds_f64();
        info!("Fetched cache entry in {total_duration:.3}s (\
                {open_read_txn_duration:.3}s to open txn, \
                {get_duration:.3}s to get(), \
                {bindecode_duration:.3}s to decode, \
                {commit_read_txn_duration:.3}s to commit read txn\
            )");


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
            let cache_entry = VersionedCacheEntry::V0(entities);

            // Save to cache
            let entities_bin =
                rmp_serde::to_vec(&cache_entry).map_err(ChronError::CacheSerializeError)?;
            let mut write_txn = env.write_txn()?;
            db.put(&mut write_txn, url.as_str(), entities_bin.as_slice())?;
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
}
