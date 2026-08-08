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