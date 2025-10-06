-- Your SQL goes here
create materialized view info.entities_with_issues_count as (
    select kind, count(distinct entity_id) as entities_with_issues
    from info.version_ingest_log
    where log_level < 3
    group by kind
    union all
    select 'game' as kind, count(distinct game_id) as entities_with_issues
    from info.event_ingest_log
    where log_level < 3
);