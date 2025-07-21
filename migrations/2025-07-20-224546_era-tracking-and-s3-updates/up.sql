-- Adding new not-null columns to a bunch of tables
truncate table
    info.event_ingest_log,
    info.raw_events,
    data.event_fielders,
    data.event_baserunners,
    data.events,
    data.games,
    taxa.event_type;

alter table taxa.event_type
    add column is_error bool not null;
alter table data.games
    add column stadium_name text; -- null = pre-stadiums game
alter table data.events
    add column errors_before int not null,
    add column errors_after int not null,
    add column cheer text; -- null = no cheer on this event
alter table data.event_baserunners
    add column source_event_index int, -- null for ghost runners
    add column is_earned bool not null;
