create table data.efflorescence (
    -- bookkeeping
    id bigserial primary key not null,
    game_id bigint references data.games on delete cascade not null,
    game_event_index integer not null,
    efflorescence_index integer not null,
    player_name text not null,
    effloresced bool not null -- if null, this is a Growth, and there's entries in the efflorescence_growth table
);

create table data.efflorescence_growth (
    -- bookkeeping
    id bigserial primary key not null,
    game_id bigint references data.games on delete cascade not null,
    game_event_index integer not null,
    efflorescence_index integer not null,
    efflorescence_growth_index integer not null,
    value integer not null,
    attribute bigint references taxa.attribute not null
);
