-- There's a lot of `on delete cascade` on this table, so this deletes quite a lot
delete from data.games where from_version >= '2025-12-28T00:47:38.244248Z';

-- These are ad-hoc, need to hit every table or there's problems
delete from data.player_versions where valid_from >= '2025-12-28T00:47:38.244248Z';
delete from data.player_modification_versions where valid_from >= '2025-12-28T00:47:38.244248Z';
-- Using `time` isn't perfect but it's the best I got for now
delete from data.player_attribute_augments where time >= '2025-12-28T00:47:38.244248Z';
delete from data.player_recompositions where time >= '2025-12-28T00:47:38.244248Z';
delete from data.player_report_versions where valid_from >= '2025-12-28T00:47:38.244248Z';
delete from data.player_report_attribute_versions where valid_from >= '2025-12-28T00:47:38.244248Z';
delete from data.player_paradigm_shifts where time >= '2025-12-28T00:47:38.244248Z';
delete from data.player_equipment_versions where valid_from >= '2025-12-28T00:47:38.244248Z';
delete from data.player_equipment_effect_versions where valid_from >= '2025-12-28T00:47:38.244248Z';
delete from data.team_versions where valid_from >= '2025-12-28T00:47:38.244248Z';
delete from data.team_player_versions where valid_from >= '2025-12-28T00:47:38.244248Z';
delete from data.team_games_played where time >= '2025-12-28T00:47:38.244248Z';
delete from data.feed_events_processed where valid_from >= '2025-12-28T00:47:38.244248Z';
delete from info.version_ingest_log where valid_from >= '2025-12-28T00:47:38.244248Z';

-- The big ones
delete from data.versions where valid_from >= '2025-12-28T00:47:38.244248Z';
delete from data.entities where valid_from >= '2025-12-28T00:47:38.244248Z';
