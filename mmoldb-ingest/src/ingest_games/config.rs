use log::warn;
use serde::Deserialize;
use serde_default::DefaultFromSerde;

fn default_ingest_period() -> u64 {
    30 * 60 // 30 minutes, expressed in seconds
}

fn default_page_size() -> usize {
    1000
}

fn default_ingest_parallelism() -> usize {
    match std::thread::available_parallelism() {
        Ok(parallelism) => parallelism.into(),
        Err(err) => {
            warn!(
                "Unable to detect available parallelism (falling back to 1): {}",
                err
            );
            1
        }
    }
}

// TODO Use this again, probably with tweaked fields
#[allow(unused)] // Until addressing the above TODO
#[derive(Clone, Deserialize, DefaultFromSerde)]
pub struct IngestConfig {
    #[serde(default = "default_ingest_period")]
    pub ingest_period_sec: u64,
    #[serde(default)]
    pub start_ingest_every_launch: bool,
    #[serde(default)]
    pub reimport_all_games: bool,
    #[serde(default = "default_page_size")]
    pub game_list_page_size: usize,
    #[serde(default = "default_ingest_parallelism")]
    pub ingest_parallelism: usize,
}
