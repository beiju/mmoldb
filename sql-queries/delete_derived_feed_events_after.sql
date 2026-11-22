begin;
    delete from info.version_ingest_log where (kind = 'team_feed' or kind = 'player_feed') and valid_from >= '2025-06-09 13:59:33.704448';
    delete from data.team_games_played where time >= '2025-06-09 13:59:33.704448';
    delete from data.player_recompositions where time >= '2025-06-09 13:59:33.704448';
    delete from data.player_attribute_augments where time >= '2025-06-09 13:59:33.704448';
    delete from data.player_paradigm_shifts where time >= '2025-06-09 13:59:33.704448';
    delete from data.feed_events_processed where timestamp >= '2025-06-09 13:59:33.704448';

	refresh materialized view info.entities_count;
	refresh materialized view info.entities_with_issues_count;
end;
