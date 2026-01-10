\set Date '2025-12-28T00:47:38.244248Z'

-- There's a lot of `on delete cascade` on this table, so this deletes quite a lot
delete from data.games where from_version >= :Date;

delete from data.player_versions where valid_from >= :Date;
delete from data.player_modification_versions where valid_from >= :Date;
-- TODO Add from_version column to player_attribute_augments and friends so that this delete can be accurate
delete from data.player_attribute_augments where time >= :Date;
delete from data.player_recompositions where time >= :Date;
delete from data.player_report_versions where valid_from >= :Date;
delete from data.player_report_attribute_versions where valid_from >= :Date;
delete from data.player_paradigm_shifts where time >= :Date;
delete from data.player_equipment_versions where valid_from >= :Date;
delete from data.player_equipment_effect_versions where valid_from >= :Date;
delete from data.team_versions where valid_from >= :Date;
delete from data.team_player_versions where valid_from >= :Date;
delete from data.team_games_played where time >= :Date;
delete from data.feed_events_processed where valid_from >= :Date;
delete from info.version_ingest_log where valid_from >= :Date;

-- The big ones
delete from data.versions where valid_from >= :Date;
delete from data.entities where valid_from >= :Date;
