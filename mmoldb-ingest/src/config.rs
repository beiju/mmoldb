use std::num::NonZero;
use figment::Figment;
use figment::providers::{Env, Format, Serialized, Toml};
use mmolb_parsing::player::Deserialize;
use serde::Serialize;

#[derive(Debug, PartialEq, Deserialize, Serialize)]
pub struct IngestibleConfig {
    pub enable: bool,
    pub chron_fetch_batch_size: usize,
    pub insert_raw_entity_batch_size: usize,
    pub process_batch_size: usize,
    pub ingest_parallelism: Option<NonZero<usize>>,
}

impl Default for IngestibleConfig {
    fn default() -> Self {
        Self {
            enable: true,
            chron_fetch_batch_size: 1000,
            insert_raw_entity_batch_size: 1000,
            process_batch_size: 1000,
            ingest_parallelism: None,
        }
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize)]
pub struct IngestConfig {
    pub start_ingest_every_launch: bool,
    pub ingest_period: i64,
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
            start_ingest_every_launch: true,
            ingest_period: 30 * 60, // 30 minutes in seconds
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