-- Delete all versions past the cutback date
delete
from data.versions
where valid_from <= '2025-10-27T11:16:00.000Z';

-- Un-close-out all versions that were closed back after the cutback date
update data.versions
set valid_to = null
where valid_to > '2025-10-27T11:16:00.000Z';

-- Delete all entities past the cutback date
delete
from data.entities
where valid_from <= '2025-10-27T11:16:00.000Z';

-- Delete ALL derived data
truncate table
    -- ingest
    info.event_ingest_log,
    info.ingest_timings,
    info.ingests,
    info.ingest_counts,
    info.version_ingest_log,

    -- game
    data.weather,
    data.games,
    data.events,
    data.event_fielders,
    data.event_baserunners,
    data.ejections,
    data.aurora_photos,
    data.door_prizes,
    data.door_prize_items,
    data.pitcher_changes,
    data.parties,
    data.wither,
    info.raw_events,

    -- player
    data.modifications,
    data.player_versions,
    data.player_modification_versions,
    data.player_attribute_augments,
    data.player_recompositions,
    data.player_report_versions,
    data.player_report_attribute_versions,
    data.player_paradigm_shifts,
    data.player_feed_versions,
    data.player_equipment_versions,
    data.player_equipment_effect_versions,

    -- team
    data.team_versions,
    data.team_player_versions,
    data.team_feed_versions,
    data.team_games_played;
