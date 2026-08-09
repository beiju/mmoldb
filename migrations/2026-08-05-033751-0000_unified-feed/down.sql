drop table data.feed_events;

drop materialized view info.entities_count;
drop table data.feed_events_processed;
create table data.feed_events_processed (
    kind text not null,
    entity_id text not null,
    feed_event_index integer not null,
    valid_from timestamp without time zone not null,
    skipped boolean not null default false,
    fatal_error boolean not null default false,
    primary key (kind, entity_id, feed_event_index, valid_from)
);

create materialized view info.entities_count as (
    select 'game' as kind, count(1) as count from data.games
    union
    select 'player' as kind, count(1) as count from data.player_versions
    union
    select 'team' as kind, count(1) as count from data.team_versions
    union
    select kind, count(1) as count from data.feed_events_processed group by kind
);