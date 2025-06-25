use allocative::Allocative;
use chrono::{DateTime, Utc};
use humansize::{format_size, DECIMAL};
use log::{info, warn};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const BLOCK_LEGACY_DELETION: bool = true;
const DB_V2_DIR_NAME: &'static str = "cache-v2";

#[derive(Debug, Error)]
pub enum ChronOpenError {
    #[error("Error opening (potentially new) rocksdb: {0}")]
    OpenDbError(rocksdb::Error),
}

#[derive(Debug, Error)]
pub enum ChronGetError {
    #[error("Error building Chron request: {0}")]
    RequestBuildError(reqwest::Error),

    #[error("Error searching cache for games page: {0}")]
    CacheGetError(rocksdb::Error),

    #[error("Error executing Chron request: {0}")]
    RequestExecuteError(reqwest::Error),
    
    #[error("Error deserializing Chron response: {0}")]
    RequestDeserializeError(reqwest::Error),

    #[error("Error encoding Chron response for cache: {0}")]
    CacheSerializeError(rmp_serde::encode::Error),

    #[error("Error inserting games page into cache: {0}")]
    CachePutError(rocksdb::Error),

    #[error("Error removing invalid games page from cache: {0}")]
    CacheRemoveError(rocksdb::Error),

    #[error("Error flushing cache to disk: {0}")]
    CacheFlushError(rocksdb::Error),
}

#[derive(Debug, Serialize, Deserialize, Allocative)]
pub struct ChronEntities<EntityT> {
    pub items: Vec<ChronEntity<EntityT>>,
    pub next_page: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Allocative)]
pub struct ChronEntity<EntityT> {
    pub kind: String,
    pub entity_id: String,
    #[allocative(skip)]
    pub valid_from: DateTime<Utc>,
    #[allocative(skip)]
    pub valid_until: Option<DateTime<Utc>>,
    pub data: EntityT,
}

pub struct Chron {
    cache: rocksdb::DB,
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

fn delete_legacy_db(cache_path: &std::path::Path) {
    // The original resident of this directory was a sled database at the
    // root. Use the existence of "conf" as an indication that that db is
    // still there.
    let legacy_db_exists = match cache_path.join("conf").try_exists() {
        Ok(exists) => exists,
        Err(err) => {
            warn!(
                "Check for legacy http cache failed: {err}. You may still have a legacy http cache \
                 taking up disk space that mmoldb cannot delete.",
            );
            return;
        }
    };

    if legacy_db_exists {
        warn!("Deleting legacy cache...");

        let dir = match std::fs::read_dir(cache_path) {
            Ok(dir) => dir,
            Err(err) => {
                warn!(
                    "Listing files in legacy http cache failed: {err}. You may still have a legacy \
                    http cache taking up disk space that mmoldb cannot delete.",
                );
                return;
            }
        };

        for item in dir {
            let item = match item {
                Ok(item) => item,
                Err(err) => {
                    warn!(
                        "Listing file in legacy http cache failed: {err}. You may still have a \
                        file within the legacy http cache taking up disk space that mmoldb cannot \
                        delete.",
                    );
                    continue;
                }
            };

            // Keep this condition up to date with the parent folders
            // of any databases that shouldn't be deleted
            if item.file_name() != DB_V2_DIR_NAME {
                info!("Removing {}", item.path().display());
                if BLOCK_LEGACY_DELETION {
                    info!("(In debug: Not removing the directory yet)");
                } else {
                    match std::fs::remove_dir_all(item.path()) {
                        Ok(()) => {}
                        Err(err) => {
                            warn!(
                                "Deleting file in legacy http cache failed: {err}. You may still \
                                have a legacy http cache file taking up disk space that mmoldb \
                                cannot delete.",
                            );
                        }
                    }
                }
            }
        }

        info!("Finished deleting legacy cache");
    }

}

impl Chron {
    pub fn new<P: AsRef<std::path::Path>>(
        cache_path: P,
        page_size: usize,
    ) -> Result<Self, ChronOpenError> {
        let cache_base_path = cache_path.as_ref();
        delete_legacy_db(cache_base_path);

        // HELP WANTED: Allow the user to configure these options using Rocket.toml,
        // environment variables, or some other solution
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.set_enable_blob_files(true);
        opts.set_min_blob_size(1024);
        opts.set_enable_blob_gc(true);

        let cache_path = cache_base_path.join(DB_V2_DIR_NAME);
        let cache = rocksdb::DB::open(&opts, cache_path)
            .map_err(ChronOpenError::OpenDbError)?;

        Ok(Self {
            cache,
            client: reqwest::Client::new(),
            page_size,
            page_size_string: page_size.to_string(),
        })
    }

    fn entities_request(&self, kind: &str, count: &str, page: Option<&str>) -> reqwest::RequestBuilder {
        let request = self.client
            .get("https://freecashe.ws/api/chron/v0/entities")
            .query(&[("kind", kind), ("count", count), ("order", "asc")]);

        if let Some(page_token) = page {
            request.query(&[("page", page_token)])
        } else {
            request
        }
    }

    fn get_cached<T: for<'d> Deserialize<'d>>(&self, key: &str) -> Result<Option<T>, ChronGetError> {
        let cache_search_start = Utc::now();
        let Some(cache_entry) = self.cache.get_pinned(key).map_err(ChronGetError::CacheGetError)? else {
            let cache_search_duration = (Utc::now() - cache_search_start).as_seconds_f64();
            info!("Negative cache query finished in {cache_search_duration}s");
            return Ok(None)
        };

        let cache_search_duration = (Utc::now() - cache_search_start).as_seconds_f64();
        info!("Positive cache query finished in {cache_search_duration}s");

        let deserialize_start = Utc::now();
        let versions = match rmp_serde::from_slice(&cache_entry) {
            Ok(versions) => versions,
            Err(err) => {
                warn!("Cache entry could not be decoded: {:?}. Removing it from the cache.", err);
                self.cache.delete(key).map_err(ChronGetError::CacheRemoveError)?;
                return Ok(None);
            }
        };
        let deserialize_duration = Utc::now().signed_duration_since(deserialize_start).as_seconds_f64();
        info!("Deserialize took {} seconds", deserialize_duration);

        match versions {
            VersionedCacheEntry::V0(data) => Ok(Some(data)),
        }
    }

    pub async fn games_page(&self, page: Option<&str>) -> Result<ChronEntities<mmolb_parsing::Game>, ChronGetError> {
        info!("User requested page {:?}", page);
        let q_start_time = Utc::now();
        let request = self.entities_request("game", &self.page_size_string, page.as_deref()).build()
            .map_err(ChronGetError::RequestBuildError)?;
        let url = request.url().to_string();
        let result = if let Some(cache_entry) = self.get_cached(&url)? {
            let q_duration = (Utc::now() - q_start_time).as_seconds_f64();
            info!("Returning page {page:?} from cache after {q_duration:.03} seconds");
            Ok(cache_entry)
        } else {
            let q_duration = (Utc::now() - q_start_time).as_seconds_f64();
            info!("Page {page:?} not found in cache after {q_duration:.03} seconds");
            // Cache miss -- request from chron
            info!("Requesting page {page:?} from chron");
            let response = self.client.execute(request).await
                .map_err(ChronGetError::RequestExecuteError)?;
            let entities: ChronEntities<mmolb_parsing::Game> = response.json()
                .await
                .map_err(ChronGetError::RequestDeserializeError)?;

            if entities.next_page.is_none() {
                info!("Not caching page {page:?} because it's the last page");
                return Ok(entities);
            }

            if entities.items.len() != self.page_size {
                info!("Not caching page {page:?} because it's the last page");
                return Ok(entities);
            }

            let has_incomplete_game = entities.items.iter()
                .any(|item| !item.data.is_terminal());
            if has_incomplete_game {
                info!("Not caching page {page:?} because it contains at least one non-terminal game");
                return Ok(entities);
            }

            // Otherwise, save to cache
            let cache_entry = VersionedCacheEntry::V0(entities);

            // Save to cache
            let entities_bin = rmp_serde::to_vec(&cache_entry)
                .map_err(ChronGetError::CacheSerializeError)?;
            self.cache.put(url.as_str(), entities_bin.as_slice()).map_err(ChronGetError::CachePutError)?;

            // Immediately fetch again from cache to verify everything is working
            let entities = self.get_cached(&url.as_str())
                .expect("Error getting cache entry immediately after it was saved")
                .expect("Cache entry was not found immediately after it was saved");
            
            Ok(entities)
        };
        
        // Fetches are already so slow that cache flushing should be a drop in the bucket. Non-fetch
        // requests shouldn't dirty the cache at all and so this should be near-instant.
        self.cache.flush().map_err(ChronGetError::CacheFlushError)?;
        
        result
    }
}
