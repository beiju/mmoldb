begin;
    delete from info.version_ingest_log where (kind = 'team_feed' or kind = 'player_feed');
    truncate table data.team_games_played;
    truncate table data.player_recompositions;
    truncate table data.player_attribute_augments;
    truncate table data.player_paradigm_shifts;
    truncate table data.feed_events_processed;

	refresh materialized view info.entities_count;
	refresh materialized view info.entities_with_issues_count;
end;
