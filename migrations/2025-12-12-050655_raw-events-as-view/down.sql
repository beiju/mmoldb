drop view info.raw_events;

create table info.raw_events (
    -- bookkeeping
    id bigserial primary key not null,
    game_id bigserial references data.games on delete cascade not null,
    game_event_index int not null,
    unique (game_id, game_event_index),

    -- event data
    event_text text not null
);

-- `on delete cascade` is very slow without the appropriate index
create index raw_events_game_id on info.raw_events (game_id);

-- This truncate is unavoidable to reset the database to its previous state
truncate table info.event_ingest_log;
alter table info.event_ingest_log
    add constraint event_ingest_log_game_id_game_event_index_fkey
        foreign key (game_id, game_event_index)
            references info.raw_events (game_id, game_event_index);
