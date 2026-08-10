drop materialized view info.entities_count;
create materialized view info.entities_count as (
    select 'game' as kind, count(1) as count from data.games
    union
    select 'player' as kind, count(1) as count from data.player_versions
    union
    select 'team' as kind, count(1) as count from data.team_versions
    union
    select subject_type, count(1) as count from data.feed_events_processed group by subject_type
);