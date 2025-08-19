create table data.door_prizes (
    -- bookkeeping
    id bigserial primary key not null,
    event_id bigint references data.events on delete cascade not null,
    door_prize_index integer not null,  -- orders door prizes within one event

    -- actual data
    player_name text not null,
    tokens integer -- null => this was an item prize
);

create index door_prizes_event_id_index on data.door_prizes (event_id);

create table data.door_prize_items (
    -- bookkeeping
    id bigserial primary key not null,
    event_id bigint references data.events on delete cascade not null,
    door_prize_index integer not null,  -- orders door prizes within one event
    item_index integer not null,  -- orders items within one door prize

    -- actual data
    emoji text not null,
    name text not null,
    rare_name text, -- null if the item is not rare or above
    prefix text, -- null if the item has no prefix
    suffix text -- null if the item has no suffix
);

create index door_prize_items_event_id_door_prize_index_index on data.door_prize_items (event_id, door_prize_index);