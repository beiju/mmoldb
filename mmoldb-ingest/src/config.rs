use figment::Figment;
use figment::providers::{Env, Format, Serialized, Toml};
use mmolb_parsing::player::Deserialize;
use serde::Serialize;
use std::num::NonZero;

#[derive(Debug, PartialEq, Deserialize, Serialize)]
pub struct IngestibleConfig {
    pub enable_fetch: bool,
    pub enable_processing: bool,
    pub chron_fetch_batch_size: NonZero<usize>,
    pub chron_fetch_interval_seconds: u64,
    pub insert_raw_entity_batch_size: NonZero<usize>,
    pub processing_interval_seconds: u64,
    pub process_batch_size: NonZero<usize>,
    pub ingest_parallelism: Option<NonZero<usize>>,
    pub debug_db_insert_delay: f64,
}

impl Default for IngestibleConfig {
    fn default() -> Self {
        Self {
            enable_fetch: true,
            enable_processing: true,
            chron_fetch_interval_seconds: 10 * 60,
            chron_fetch_batch_size: 1000.try_into().unwrap(),
            insert_raw_entity_batch_size: 1000.try_into().unwrap(),
            processing_interval_seconds: 10 * 60,
            process_batch_size: 1000.try_into().unwrap(),
            ingest_parallelism: None,
            debug_db_insert_delay: 0.0,
        }
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize)]
pub struct IngestConfig {
    pub db_pool_size: u32,
    pub set_postgres_statement_timeout: Option<i64>,
    pub use_local_cheap_cashews: bool,
    pub fetch_known_missing_games: bool,
    pub team_ingest: IngestibleConfig,
    pub team_feed_ingest: IngestibleConfig,
    pub player_ingest: IngestibleConfig,
    pub player_feed_ingest: IngestibleConfig,
    pub game_ingest: IngestibleConfig,
}

impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            db_pool_size: 20,
            set_postgres_statement_timeout: Some(0), // 0 means no timeout
            use_local_cheap_cashews: false,
            fetch_known_missing_games: false,
            team_ingest: Default::default(),
            team_feed_ingest: Default::default(),
            player_ingest: Default::default(),
            player_feed_ingest: Default::default(),
            game_ingest: Default::default(),
        }
    }
}

impl IngestConfig {
    pub fn figment() -> Figment {
        Figment::from(Serialized::defaults(Self::default()))
            .merge(Toml::file("MMOLDB.toml"))
            .merge(Env::prefixed("MMOLDB_"))
    }

    pub fn config() -> figment::Result<Self> {
        Self::figment().extract()
    }
}
