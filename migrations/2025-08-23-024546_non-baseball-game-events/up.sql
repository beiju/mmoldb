create table taxa.pitcher_change_source (
    id bigserial primary key not null,
    name text not null,
    display_name text not null,
    unique (name)
);

create table data.pitcher_changes (
    id bigserial primary key not null,
    game_id bigint references data.games not null,
    game_event_index integer not null,
    previous_game_event_index integer, -- null in the unlikely event this is the first event
    source bigint references taxa.pitcher_change_source not null,
    pitcher_name text not null,
    pitcher_slot bigint references taxa.slot not null,
    new_pitcher_name text, -- null if the pitcher remained in the game
    new_pitcher_slot bigint references taxa.slot, -- null if the pitcher remained in the game, or if the game didn't tell us what the slot was

    unique (game_id, game_event_index)
);

