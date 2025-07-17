-- This file should undo anything in `up.sql`

-- gotta delete games to be able to add a new not null column
-- gotta delete all foreign key relations recursively
-- cascade would get these but it's really slow
truncate table info.event_ingest_log, info.raw_events, data.event_fielders, data.event_baserunners, data.events, data.games;

-- noinspection SqlAddNotNullColumn
alter table data.games
    drop column from_version,
    drop column is_ongoing,
    add column is_finished bool not null;
