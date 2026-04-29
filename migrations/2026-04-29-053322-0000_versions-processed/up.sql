create table data.versions_processed (
    kind text not null,
    entity_id text not null,
    valid_from timestamp without time zone not null,
    skipped bool not null,
    fatal_error bool not null,  -- only for diagnostic purposes
    primary key (kind, entity_id, valid_from)
);

alter table data.feed_events_processed
    add column skipped bool not null default false,
    add column fatal_error bool not null default false;  -- only for diagnostic purposes