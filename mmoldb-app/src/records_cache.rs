use std::sync::{Arc, Mutex};
use chrono::{DateTime, Utc};
use diesel::Connection;
use log::error;
use serde::Serialize;
use thiserror::Error;
use tokio::task::JoinHandle;
use mmoldb_db::ConnectionPool;

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
pub struct Records {
    pub latest_game: Option<GameTime>,
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

    Ok(Records {
        latest_game,
    })
}

impl RecordsCacheUpdate {
    pub fn new(pool: ConnectionPool, records: Arc<Mutex<Option<Records>>>) -> Self {
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
        Self::InProgress {
            started: Utc::now(),
            task,
        }
    }
}

pub struct RecordsCache {
    active_update: RecordsCacheUpdate,
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
        if let RecordsCacheUpdate::InProgress { started, .. } = self.active_update {
            Some(started)
        } else {
            None
        }
    }
}