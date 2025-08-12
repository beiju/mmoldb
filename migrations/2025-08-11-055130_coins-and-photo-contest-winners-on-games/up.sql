-- force reingest everything bc it's easier
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
    data.player_equipment_effect_versions;

alter table data.games
    add column home_team_earned_coins int, -- null = this was not a coin weather
    add column away_team_earned_coins int, -- null = this was not a coin weather
    add column home_team_photo_contest_top_scorer text, -- null = no photo contest
    add column home_team_photo_contest_score int, -- null = no photo contest
    add column away_team_photo_contest_top_scorer text, -- null = no photo contest
    add column away_team_photo_contest_score int; -- null = no photo contest
