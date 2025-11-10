alter table data.wither
    add column source_player_name text, -- null in s6 when source player name was not provided
    add column contain_attempted bool not null default false,
    add column contain_replacement_player_name text; -- null if no containment

alter table data.parties
    add column durability_loss integer default 3;
