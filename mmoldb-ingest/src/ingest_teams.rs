use chrono::{DateTime, Utc};
use itertools::Itertools;
use log::{debug, error, info};
use miette::{Context, IntoDiagnostic};
use mmolb_parsing::{AddedLater, NotRecognized};
use crate::ingest::{batch_by_entity, IngestFatalError, VersionIngestLogs};
use chron::ChronEntity;
use mmoldb_db::taxa::Taxa;
use mmoldb_db::{db, BestEffortSlot, PgConnection};
use tokio_util::sync::CancellationToken;
use rayon::prelude::*;
use mmoldb_db::models::{NewPlayerAttributeAugment, NewPlayerFeedVersion, NewPlayerParadigmShift, NewPlayerRecomposition, NewTeamPlayerVersion, NewTeamVersion, NewVersionIngestLog};

// I made this a constant because I'm constant-ly terrified of typoing
// it and introducing a difficult-to-find bug
const TEAM_KIND: &'static str = "team";
const CHRON_FETCH_PAGE_SIZE: usize = 1000;
const RAW_TEAM_INSERT_BATCH_SIZE: usize = 1000;
const PROCESS_TEAM_BATCH_SIZE: usize = 1000;

pub async fn ingest_teams(ingest_id: i64, pg_url: String, abort: CancellationToken) -> miette::Result<()> {
    crate::ingest::ingest(
        ingest_id,
        TEAM_KIND,
        CHRON_FETCH_PAGE_SIZE,
        RAW_TEAM_INSERT_BATCH_SIZE,
        PROCESS_TEAM_BATCH_SIZE,
        pg_url,
        abort,
        db::get_team_ingest_start_cursor,
        ingest_page_of_teams,
    ).await
}

pub fn ingest_page_of_teams(
    taxa: &Taxa,
    raw_teams: Vec<ChronEntity<serde_json::Value>>,
    conn: &mut PgConnection,
    worker_id: usize,
) -> miette::Result<i32> {
    debug!(
        "Starting ingest of {} teams on worker {worker_id}",
        raw_teams.len()
    );
    let save_start = Utc::now();

    let deserialize_start = Utc::now();
    // TODO Gracefully handle team deserialize failure
    let teams = raw_teams
        .into_par_iter()
        .map(|entity| {
            let data = serde_json::from_value(entity.data)
                .into_diagnostic()
                .with_context(|| format!(
                    "Error deserializing team {} at {}", entity.entity_id, entity.valid_from,
                ))?;

            Ok::<ChronEntity<mmolb_parsing::team::Team>, miette::Report>(ChronEntity {
                kind: entity.kind,
                entity_id: entity.entity_id,
                valid_from: entity.valid_from,
                valid_to: entity.valid_to,
                data,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let deserialize_duration = (Utc::now() - deserialize_start).as_seconds_f64();
    debug!(
        "Deserialized page of {} teams in {:.2} seconds on worker {}",
        teams.len(), deserialize_duration, worker_id
    );

    let latest_time = teams
        .last()
        .map(|version| version.valid_from)
        .unwrap_or(Utc::now());
    let time_ago = latest_time.signed_duration_since(Utc::now());
    let human_time_ago = chrono_humanize::HumanTime::from(time_ago);

    // Convert to Insertable type
    let new_teams = teams
        .iter()
        .map(|v| chron_team_as_new(&taxa, &v.entity_id, v.valid_from, &v.data))
        .collect_vec();

    // The handling of valid_until is entirely in the database layer, but its logic
    // requires that a given batch of teams does not have the same team twice. We
    // provide that guarantee here.
    let new_teams_len = new_teams.len();
    let mut total_inserted = 0;
    for batch in batch_by_entity(new_teams, |v| v.0.mmolb_team_id) {
        let to_insert = batch.len();
        info!(
            "Sent {} new team versions out of {} to the database.",
            to_insert,
            new_teams_len,
        );

        let inserted = db::insert_team_versions(conn, batch).into_diagnostic()?;
        total_inserted += inserted as i32;

        info!(
            "Sent {} new team versions out of {} to the database. \
            {inserted} versions were actually inserted, the rest were duplicates. \
            Currently processing team versions from {human_time_ago}.",
            to_insert,
            teams.len(),
        );
    }

    let save_duration = (Utc::now() - save_start).as_seconds_f64();

    info!(
        "Ingested page of {} team versions in {save_duration:.3} seconds.",
        teams.len(),
    );

    Ok(total_inserted)
}

pub fn chron_team_as_new<'a>(
    taxa: &Taxa,
    team_id: &'a str,
    valid_from: DateTime<Utc>,
    team: &'a mmolb_parsing::team::Team,
) -> (
    NewTeamVersion<'a>,
    Vec<NewTeamPlayerVersion<'a>>,
    Vec<NewVersionIngestLog<'a>>
) {
    let mut ingest_logs = VersionIngestLogs::new(TEAM_KIND, team_id, valid_from);
    let ballpark_suffix = match &team.ballpark_suffix {
        Ok(Ok(suffix)) => { Some(suffix.to_string()) }
        Ok(Err(NotRecognized(unrecognized))) => match unrecognized.as_str() {
            Some(value) => Some(value.to_string()),
            None => {
                ingest_logs.error(
                    format!("Ballpark suffix was not a string: {unrecognized:?}"),
                );
                None
            },
        }
        Err(AddedLater) => { None }
    };

    let new_team_players = team.players
        .iter()
        .enumerate()
        .map(|(idx, pl)| {
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
                slot: match &pl.slot {
                    Ok(Ok(slot)) => Some(taxa.slot_id(BestEffortSlot::Slot(*slot).into())),
                    Ok(Err(NotRecognized(other))) => {
                        ingest_logs.error(format!(
                            "Failed to parse {} {}'s slot ({other:?}",
                            pl.first_name, pl.last_name
                        ));
                        None
                    },
                    Err(AddedLater) => None,
                },
                mmolb_player_id: &pl.player_id,
            }
        })
        .collect_vec();

    let new_team = NewTeamVersion {
        mmolb_team_id: team_id,
        valid_from: valid_from.naive_utc(),
        valid_until: None,
        name: &team.name,
        emoji: &team.emoji,
        color: &team.color,
        location: &team.location,
        full_location: &team.full_location,
        abbreviation: &team.abbreviation,
        eligible: team.eligible.as_ref().ok().cloned(),
        championships: team.championships as i32,
        motes_used: team.motes_used.as_ref().ok().map(|m| *m as i32),
        mmolb_league_id: team.league.as_deref(),
        ballpark_name: team.ballpark_name.as_ref().ok().map(|s| s.as_str()),
        ballpark_word_1: team.ballpark_word_1.as_ref().ok().and_then(|s| s.as_deref()),
        ballpark_word_2: team.ballpark_word_2.as_ref().ok().and_then(|s| s.as_deref()),
        ballpark_suffix,
        ballpark_use_city: team.ballpark_use_city.as_ref().ok().cloned(),
        num_players: 0,
    };

    let num_unique = new_team_players
        .iter()
        .unique_by(|v| (v.mmolb_team_id, v.team_player_index))
        .count();

    if num_unique != new_team_players.len() {
        error!("Got a duplicate team player");
    }

    (new_team, new_team_players, ingest_logs.into_vec())
}