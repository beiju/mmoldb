create table data.versions_processed (
    kind text not null,
    entity_id text not null,
    valid_from timestamp without time zone not null,
    skipped bool not null,
    primary key (kind, entity_id, valid_from)
);
