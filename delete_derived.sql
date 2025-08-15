-- this statement should delete all derived data
truncate table
    -- ingest
    info.event_ingest_log,
    info.ingest_timings,
    info.ingests,

    -- game
    data.weather,
    data.games,
    data.events,
    data.event_fielders,
    data.event_baserunners,
    data.ejections,
    data.aurora_photos,
    info.raw_events,
    info.ingest_counts,
    info.version_ingest_log,

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
    data.team_versions;
