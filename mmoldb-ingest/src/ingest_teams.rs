use crate::config::IngestibleConfig;
use crate::ingest::{IngestStage, Ingestable, IngestibleFromVersions, Stage2Ingest, VersionIngestLogs, VersionStage1Ingest};
use chrono::{DateTime, NaiveDateTime, Utc};
use itertools::Itertools;
use mmolb_parsing::enums::Slot;
use mmolb_parsing::{team::TeamPlayerCollection, AddedLater, AddedLaterResult, MaybeRecognizedResult, NotRecognized};
use mmoldb_db::models::{NewTeamPlayerVersion, NewTeamVersion, NewVersionIngestLog};
use mmoldb_db::taxa::Taxa;
use mmoldb_db::{async_db, db, AsyncPgConnection, BestEffortSlot, PgConnection, QueryResult};
use std::str::FromStr;
use std::sync::Arc;
use futures::Stream;
use serde_json::Value;
use chron::ChronEntity;

pub struct TeamIngestFromVersions;

impl IngestibleFromVersions for TeamIngestFromVersions {
    type Entity = mmolb_parsing::team::Team;

    fn get_start_cursor(conn: &mut PgConnection) -> QueryResult<Option<(NaiveDateTime, String)>> {
        db::get_team_ingest_start_cursor(conn)
    }

    fn trim_unused(version: &serde_json::Value) -> serde_json::Value {
        match version {
            serde_json::Value::Object(obj) => serde_json::Value::Object({
                obj.iter()
                    .filter_map(|(k, v)| {
                        // I think it's fine to ignore feed because (1) There are no new team
                        // versions created with an integrated feed, and (2) the Feed is persistent,
                        // so we can ignore hundreds of versions and then the first standalone feed
                        // version will capture all the backlog
                        if k == "SeasonStats"
                            || k == "Record"
                            || k == "Feed"
                            || k == "Augments"
                            || k == "MotesUsed"
                            || k == "SeasonRecords"
                            || k == "Inventory"
                        {
                            None
                        } else if k == "Players" {
                            let new_v = match v {
                                serde_json::Value::Array(arr) => serde_json::Value::Array({
                                    arr.iter()
                                        .map(|pl| match pl {
                                            serde_json::Value::Object(pobj) => {
                                                serde_json::Value::Object({
                                                    pobj.iter()
                                                        .flat_map(|(pk, pv)| {
                                                            if pk == "Stats" {
                                                                None
                                                            } else {
                                                                Some((pk.clone(), pv.clone()))
                                                            }
                                                        })
                                                        .collect()
                                                })
                                            }
                                            other => other.clone(),
                                        })
                                        .collect()
                                }),
                                other => other.clone(),
                            };
                            Some((k.clone(), new_v))
                        } else {
                            Some((k.clone(), v.clone()))
                        }
                    })
                    .collect()
            }),
            other => other.clone(),
        }
    }

    fn insert_batch(conn: &mut PgConnection, taxa: &Taxa, versions: &Vec<ChronEntity<Self::Entity>>) -> QueryResult<usize> {
        let new_team_versions = versions.iter()
            .map(|team| chron_team_as_new(taxa, &team.entity_id, team.valid_from, &team.data))
            .collect_vec();

        db::insert_team_versions(conn, &new_team_versions)
    }

    async fn stream_versions_at_cursor(conn: &mut AsyncPgConnection, kind: &str, cursor: Option<(NaiveDateTime, String)>) -> QueryResult<impl Stream<Item=QueryResult<ChronEntity<Value>>>> {
        async_db::stream_versions_at_cursor(conn, kind, cursor).await
    }
}

pub struct TeamIngest(&'static IngestibleConfig);

impl TeamIngest {
    pub fn new(config: &'static IngestibleConfig) -> TeamIngest {
        TeamIngest(config)
    }
}

impl Ingestable for TeamIngest {
    const KIND: &'static str = "team";

    fn config(&self) -> &'static IngestibleConfig {
        &self.0
    }

    fn stages(&self) -> Vec<Arc<dyn IngestStage>> {
        vec![
            Arc::new(VersionStage1Ingest::new(Self::KIND)),
            Arc::new(Stage2Ingest::new(Self::KIND, TeamIngestFromVersions))
        ]
    }
}

fn chron_team_as_new<'a>(
    taxa: &Taxa,
    team_id: &'a str,
    valid_from: DateTime<Utc>,
    team: &'a mmolb_parsing::team::Team,
) -> (
    NewTeamVersion<'a>,
    Vec<NewTeamPlayerVersion<'a>>,
    Vec<NewVersionIngestLog<'a>>,
) {
    let mut ingest_logs = VersionIngestLogs::new(TeamIngest::KIND, team_id, valid_from);

    let new_team_players = match &team.players {
        TeamPlayerCollection::Vec(v) => {
            v.iter()
                .enumerate()
                .map(|(idx, pl)| {
                    chron_team_player_as_new(taxa, team_id, valid_from, idx, pl, &pl.slot, &mut ingest_logs)
                })
                .collect_vec()
        }
        TeamPlayerCollection::Map(m) => {
            m.iter()
                .enumerate()
                .map(|(idx, (slot_str, pl))| {
                    let slot = Ok(maybe_recognized_from_str(&slot_str));
                    chron_team_player_as_new(taxa, team_id, valid_from, idx, pl, &slot, &mut ingest_logs)
                })
                .collect_vec()
        }
    };

    let new_team = NewTeamVersion {
        mmolb_team_id: team_id,
        valid_from: valid_from.naive_utc(),
        valid_until: None,
        name: &team.name,
        emoji: &team.emoji,
        color: &team.color,
        location: &team.location,
        full_location: team.full_location.as_deref().ok(),
        abbreviation: team.abbreviation.as_deref().ok(),
        championships: team.championships.as_ref().map(|c| *c as i32),
        mmolb_league_id: team.league.as_deref(),
        ballpark_name: team.ballpark_name.as_ref().ok().map(|s| s.as_str()),
        num_players: new_team_players.len() as i32,
    };

    let num_unique = new_team_players
        .iter()
        .unique_by(|v| (v.mmolb_team_id, v.team_player_index))
        .count();

    if num_unique != new_team_players.len() {
        ingest_logs.error("Got a duplicate team player");
    }

    (new_team, new_team_players, ingest_logs.into_vec())
}


pub fn chron_team_player_as_new<'a>(
    taxa: &Taxa,
    team_id: &'a str,
    valid_from: DateTime<Utc>,
    idx: usize,
    pl: &'a mmolb_parsing::team::TeamPlayer,
    slot: &AddedLaterResult<MaybeRecognizedResult<Slot>>, // note this is NOT 'a
    ingest_logs: &mut VersionIngestLogs
) -> NewTeamPlayerVersion<'a> {
    // Note: I have to include undrafted players because the closeout
    // logic otherwise doesn't handle full team redraft properly
    NewTeamPlayerVersion {
        mmolb_team_id: team_id,
        team_player_index: idx as i32,
        valid_from: valid_from.naive_utc(),
        valid_until: None,
        first_name: &pl.first_name,
        last_name: &pl.last_name,
        number: pl.number as i32,
        slot: match slot {
            Ok(Ok(slot)) => Some(taxa.slot_id(BestEffortSlot::from_slot(*slot).into())),
            Ok(Err(NotRecognized(other))) => {
                ingest_logs.error(format!(
                    "Failed to parse {} {}'s slot ({other:?}",
                    pl.first_name, pl.last_name
                ));
                None
            }
            Err(AddedLater) => None,
        }
            .or_else(|| match &pl.position {
                Some(Ok(position)) => {
                    Some(taxa.slot_id(BestEffortSlot::from_position(*position).into()))
                }
                Some(Err(NotRecognized(other))) => {
                    ingest_logs.error(format!(
                        "Failed to parse {} {}'s position ({other:?}",
                        pl.first_name, pl.last_name
                    ));
                    None
                }
                None => None,
            }),
        // MMOLB uses "#" for undrafted players
        mmolb_player_id: if pl.player_id == "#" {
            None
        } else {
            Some(&pl.player_id)
        },
    }
}

// copied out of mmolb_parsing
pub(crate) fn maybe_recognized_from_str<T: FromStr>(value: &str) -> MaybeRecognizedResult<T> {
    T::from_str(value).map_err(|_| {
        NotRecognized(serde_json::Value::String(value.to_string()))
    })
}

