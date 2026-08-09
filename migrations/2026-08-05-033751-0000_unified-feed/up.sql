create table data.feed_events (
    event_id text not null,
    subject_type text not null,
    subject_id text not null,
    timestamp timestamp without time zone not null,
    data jsonb not null,

    -- theoretically just event_id should be unique, but throwing in timestamp there too
    -- to protect against errors if danny ever manually adds events with the same id or
    -- something
    primary key (event_id, timestamp)
);

drop materialized view info.entities_count;
drop table data.feed_events_processed;
create table data.feed_events_processed (
    subject_type text not null,
    event_id text not null,
    timestamp timestamp without time zone not null,
    skipped boolean not null default false,
    fatal_error boolean not null default false,
    primary key (event_id, timestamp)
);

create materialized view info.entities_count as (
    select 'game' as kind, count(1) as count from data.games
    union
    select 'player' as kind, count(1) as count from data.player_versions
    union
    select 'team' as kind, count(1) as count from data.team_versions
    union
    select subject_type, count(1) as count from data.feed_events_processed group by subject_type
);