delete from data.games where season = 6;

delete from info.version_ingest_log where valid_from > '2025-10-09T04:00:00.963000+00:00';
delete from data.player_versions where valid_from > '2025-10-09T04:00:00.963000+00:00';
delete from data.player_modification_versions where valid_from > '2025-10-09T04:00:00.963000+00:00';
delete from data.player_attribute_augments where time > '2025-10-09T04:00:00.963000+00:00';
delete from data.player_recompositions where time > '2025-10-09T04:00:00.963000+00:00';
delete from data.player_report_versions where valid_from > '2025-10-09T04:00:00.963000+00:00';
delete from data.player_report_attribute_versions where valid_from > '2025-10-09T04:00:00.963000+00:00';
delete from data.player_paradigm_shifts where time > '2025-10-09T04:00:00.963000+00:00';
delete from data.player_feed_versions where valid_from > '2025-10-09T04:00:00.963000+00:00';
delete from data.player_equipment_versions where valid_from > '2025-10-09T04:00:00.963000+00:00';
delete from data.player_equipment_effect_versions where valid_from > '2025-10-09T04:00:00.963000+00:00';
delete from data.team_versions where valid_from > '2025-10-09T04:00:00.963000+00:00';
delete from data.team_player_versions where valid_from > '2025-10-09T04:00:00.963000+00:00';
delete from data.team_feed_versions where valid_from > '2025-10-09T04:00:00.963000+00:00';
delete from data.team_games_played where time > '2025-10-09T04:00:00.963000+00:00';
