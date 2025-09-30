use strum::IntoEnumIterator;
use std::sync::{Arc, Mutex};
use chrono::{DateTime, Utc};
use diesel::Connection;
use itertools::Itertools;
use log::error;
use serde::Serialize;
use thiserror::Error;
use tokio::task::JoinHandle;
use mmoldb_db::ConnectionPool;
use mmoldb_db::taxa::{Taxa, TaxaAttribute};

#[derive(Debug, Clone, Serialize)]
pub struct DisplayDate {

}

#[derive(Debug, Clone, Serialize)]
pub struct GameTime {
    pub time: DateTime<Utc>,
    pub season: i32,
    pub day: Option<i32>,
    pub superstar_day: Option<i32>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum RecordHolder {
    Game {
        mmolb_game_id: String,
        away_team_mmolb_id: String,
        away_team_emoji: String,
        away_team_full_name: String,
        home_team_mmolb_id: String,
        home_team_emoji: String,
        home_team_full_name: String,
    },
    TeamInGame {
        mmolb_game_id: String,
        team_mmolb_id: String,
        team_emoji: String,
        team_full_name: String,
    },
    Player {
        mmolb_team_id: String,
        team_emoji: String,
        team_location: String,
        team_name: String,
        mmolb_player_id: String,
        player_name: String,
    },
    PlayerInGame {
        mmolb_team_id: String,
        team_emoji: String,
        team_location: String,
        team_name: String,
        mmolb_player_id: String,
        player_name: String,
        mmolb_game_id: String,
        game_event_index: i32,
    },
}

#[derive(Debug, Clone, Serialize)]
pub struct Record {
    pub title: String,
    pub description: Option<&'static str>,
    pub holder: RecordHolder,
    pub record: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct Records {
    pub latest_game: Option<GameTime>,
    pub records: Vec<Record>,
}

#[derive(Debug, Error)]
pub enum ComputeRecordsError {
    #[error(transparent)]
    PoolGetConnectionError(#[from] mmoldb_db::PoolError),

    #[error(transparent)]
    QueryError(#[from] mmoldb_db::QueryError),
}

enum RecordsCacheUpdate {
    None,
    InProgress {
        started: DateTime<Utc>,
        task: JoinHandle<()>,
    },
    Error(ComputeRecordsError),
}

fn update_all_records(pool: ConnectionPool) -> Result<Records, ComputeRecordsError> {
    let mut conn = pool.get()?;

    let latest_game = (*conn).transaction(mmoldb_db::db::latest_game)?
        .map(|(time, season, day, superstar_day)| GameTime {
            time: time.and_utc(),
            season,
            day,
            superstar_day
        });

    let fastest_pitch = (*conn).transaction(mmoldb_db::db::fastest_pitch)?
        .map(|r| Record {
            title: "Fastest Pitch".to_string(),
            description: None,
            holder: RecordHolder::PlayerInGame {
                mmolb_team_id: r.mmolb_team_id,
                team_emoji: r.team_emoji,
                team_location: r.team_location,
                team_name: r.team_name,
                mmolb_player_id: r.mmolb_player_id,
                player_name: r.player_name,
                mmolb_game_id: r.mmolb_game_id,
                game_event_index: r.game_event_index,
            },
            record: format!("{:.1} MPH", r.pitch_speed),
        });

    let highest_scoring_game = (*conn).transaction(mmoldb_db::db::highest_scoring_game)?
        .map(|g| {
            Record {
                title: "Highest scoring game".to_string(),
                description: Some("Both teams combined"),
                holder: RecordHolder::Game {
                    mmolb_game_id: g.mmolb_game_id,
                    away_team_mmolb_id: g.away_team_mmolb_id,
                    away_team_emoji: g.away_team_emoji,
                    away_team_full_name: g.away_team_name,
                    home_team_mmolb_id: g.home_team_mmolb_id,
                    home_team_emoji: g.home_team_emoji,
                    home_team_full_name: g.home_team_name,
                }
                ,
                record: match (g.away_team_final_score, g.home_team_final_score) {
                    (Some(away), Some(home)) => format!("{away}-{home}"),
                    (Some(away), None) => format!("{away}-?"),
                    (None, Some(home)) => format!("?-{home}"),
                    (None, None) => "?-?".to_string(),
                },
            }
        });

    let highest_score_in_a_game = (*conn).transaction(mmoldb_db::db::highest_score_in_a_game)?
        .and_then(|g| g.away_team_final_score.map(|away_score| (g, away_score)))
        .and_then(|(g, away_score)| g.home_team_final_score.map(|s| (g, away_score, s)))
        .map(|(g, away_score, home_score)| {
            Record {
                title: "Highest score in a game".to_string(),
                description: Some("Single team"),
                holder: if away_score > home_score {
                    RecordHolder::TeamInGame {
                        mmolb_game_id: g.mmolb_game_id,
                        team_mmolb_id: g.away_team_mmolb_id,
                        team_emoji: g.away_team_emoji,
                        team_full_name: g.away_team_name,
                    }
                } else {
                    RecordHolder::TeamInGame {
                        mmolb_game_id: g.mmolb_game_id,
                        team_mmolb_id: g.home_team_mmolb_id,
                        team_emoji: g.home_team_emoji,
                        team_full_name: g.home_team_name,
                    }
                },
                record: format!("{} runs", std::cmp::max(away_score, home_score)),
            }
        });

    let longest_game_by_events = (*conn).transaction(mmoldb_db::db::longest_game_by_events)?
        .map(|g| {
            Record {
                title: "Longest game by events".to_string(),
                description: Some("Includes pitches and balks, but not other things like mound visits or weather events"),
                holder: RecordHolder::Game {
                    mmolb_game_id: g.mmolb_game_id,
                    away_team_mmolb_id: g.away_team_mmolb_id,
                    away_team_emoji: g.away_team_emoji,
                    away_team_full_name: g.away_team_name,
                    home_team_mmolb_id: g.home_team_mmolb_id,
                    home_team_emoji: g.home_team_emoji,
                    home_team_full_name: g.home_team_name,
                },
                record: format!("{} events", g.count),
            }
        });

    let longest_game_by_innings = (*conn).transaction(mmoldb_db::db::longest_game_by_innings)?
        .map(|g| {
            Record {
                title: "Longest game by innings".to_string(),
                description: None,
                holder: RecordHolder::Game {
                    mmolb_game_id: g.mmolb_game_id,
                    away_team_mmolb_id: g.away_team_mmolb_id,
                    away_team_emoji: g.away_team_emoji,
                    away_team_full_name: g.away_team_name,
                    home_team_mmolb_id: g.home_team_mmolb_id,
                    home_team_emoji: g.home_team_emoji,
                    home_team_full_name: g.home_team_name,
                },
                record: format!("{} innings", g.count),
            }
        });

    let attribute_records = TaxaAttribute::iter()
        .map(|attr| {
            (*conn).transaction(|c| mmoldb_db::db::highest_reported_attribute(c, attr.into()))
                .map(|r| r.map(|r| {
                    Record {
                        title: format!("Highest reported {attr} stars"),
                        description: Some("Does not include items or boons"),
                        holder: RecordHolder::Player {
                            mmolb_team_id: r.mmolb_team_id,
                            team_emoji: r.team_emoji,
                            team_location: r.team_location,
                            team_name: r.team_name,
                            mmolb_player_id: r.mmolb_player_id,
                            player_name: r.player_name,
                        },
                        record: format!("{} stars", r.count),
                    }
                }))
        })
        .filter_map_ok(|record| record)
        .collect::<Result<Vec<_>, _>>()?;

    let records = [
        fastest_pitch,
        highest_scoring_game,
        highest_score_in_a_game,
        longest_game_by_events,
        longest_game_by_innings,
    ];

    Ok(Records {
        latest_game,
        records: records.into_iter()
            .filter_map(|record| record)
            .chain(attribute_records)
            .collect(),
    })
}

impl RecordsCacheUpdate {
    pub fn new(pool: ConnectionPool, records: Arc<Mutex<Option<Records>>>) -> Arc<Mutex<Self>> {
        let update = Arc::new(Mutex::new(RecordsCacheUpdate::None));
        let update_for_task = update.clone();
        let task = tokio::task::spawn_blocking(move || {
            match update_all_records(pool) {
                Ok(new_records) => {
                    {
                        let mut records = records.lock()
                            .expect("Error locking records for task");
                        *records = Some(new_records);
                    }
                    {
                        let mut update = update_for_task.lock()
                            .expect("Error locking update for task");
                        *update = RecordsCacheUpdate::None;
                    }
                },
                Err(e) => {
                    error!("{e:?}");
                    let mut update = update_for_task.lock()
                        .expect("Error locking update for task");
                    *update = RecordsCacheUpdate::Error(e);
                },
            }
        });
        {
            let mut update = update.lock()
                .expect("Error locking update for holder");
            *update = Self::InProgress {
                started: Utc::now(),
                task,
            };
        }
        update
    }
}

pub struct RecordsCache {
    active_update: Arc<Mutex<RecordsCacheUpdate>>,
    latest_records: Arc<Mutex<Option<Records>>>,
}

impl RecordsCache {
    pub fn new(db: ConnectionPool) -> Self {
        let latest_records = Arc::new(Mutex::new(None));
        let active_update = RecordsCacheUpdate::new(db, latest_records.clone());
        Self { active_update, latest_records }
    }

    pub fn latest(&self) -> Option<Records> {
        let records = self.latest_records.lock()
            .expect("Error locking records");
        records.clone()
    }

    pub fn update_date(&self) -> Option<DateTime<Utc>> {
        let update = self.active_update.lock()
            .expect("Error locking update");
        if let RecordsCacheUpdate::InProgress { started, .. } = *update {
            Some(started)
        } else {
            None
        }
    }

    pub fn update_error(&self) -> Option<String> {
        let update = self.active_update.lock()
            .expect("Error locking update");
        if let RecordsCacheUpdate::Error(err) = &*update {
            Some(format!("{}", err))
        } else {
            None
        }
    }
}