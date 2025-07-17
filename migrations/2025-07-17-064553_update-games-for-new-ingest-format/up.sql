-- Your SQL goes here

-- gotta delete games to be able to add a new not null column
-- gotta delete all foreign key relations recursively
-- cascade would get these but it's really slow
truncate table info.event_ingest_log, info.raw_events, data.event_fielders, data.event_baserunners, data.events, data.games;

-- noinspection SqlAddNotNullColumn
alter table data.games
    drop column is_finished,
    add column is_ongoing bool not null,
    add column from_version timestamp without time zone not null;
