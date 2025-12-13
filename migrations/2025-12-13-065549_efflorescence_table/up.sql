create table data.efflorescence (
    -- bookkeeping
    id bigserial primary key not null,
    event_id bigint references data.events on delete cascade not null,
    efflorescence_index integer not null,  -- orders efflorescences within one event

    -- actual data
    player_name text not null,
    effloresced boolean not null -- false means this was a Growth and there are entries in the efflorescence_growth table
);

create index efflorescence_event_id_index on data.efflorescence (event_id);

create table data.efflorescence_growth (
    -- bookkeeping
    id bigserial primary key not null,
    event_id bigint references data.events on delete cascade not null,
    efflorescence_index integer not null,  -- orders efflorescences within one event
    growth_index integer not null,  -- orders growths within one efflorescence

    -- actual data
    value float8 not null,
    attribute bigint references taxa.attribute not null
);

create index efflorescence_growth_event_id_efflorescence_index_index
    on data.efflorescence_growth (event_id, efflorescence_index);

create table data.failed_ejections (
    -- bookkeeping
    id bigserial primary key not null,
    event_id bigint references data.events on delete cascade not null,

    -- actual data
    player_name_1 text not null,
    player_name_2 text not null
);

create index failed_ejection_event_id_index on data.failed_ejections (event_id);
