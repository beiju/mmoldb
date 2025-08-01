create schema info;

create table info.ingests (
    id bigserial primary key not null,
    started_at timestamp without time zone not null,
    -- If this is null the ingest_games is ongoing or it crashed
    finished_at timestamp without time zone,
    -- If this is null the ingest_games is ongoing, finished successfully, or
    -- crashed in a way that prevented the database from being updated
    aborted_at timestamp without time zone,

    -- The next_page token that came with the latest Chronicler page that
    -- we've fully ingested. This is the token to use for the first API
    -- call in the next ingest_games. Null indicates that we didn't make it
    -- through the first page.
    start_next_ingest_at_page text
)
