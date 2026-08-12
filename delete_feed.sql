begin;
    truncate table
        data.team_games_played,
        data.player_attribute_augments,
        data.player_recompositions,
        data.player_paradigm_shifts,
        data.feed_events_processed;
    delete from info.version_ingest_log
        where kind='player_feed'
            or kind='team_feed';
    refresh materialized view info.entities_count;
    refresh materialized view info.entities_with_issues_count;
end;
