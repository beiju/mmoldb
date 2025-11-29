create table data.feed_events_processed (
    kind text not null,
    entity_id text not null,
    feed_event_index integer not null,
    valid_from timestamp without time zone not null,
    primary key (kind, entity_id, feed_event_index, valid_from)
);