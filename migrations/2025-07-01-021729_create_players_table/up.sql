create table taxa.handedness (
     id bigserial primary key not null,
     name text not null,
     unique (name)
);

create table taxa.day_type (
     id bigserial primary key not null,
     name text not null,
     display_name text not null,
     unique (name)
);

create table data.player_versions (
    -- bookkeeping
    id bigserial primary key not null,
    mmolb_id text not null,
    -- using "without time zone" because that's what the datablase did
    valid_from timestamp without time zone not null,
    valid_until timestamp without time zone, -- null means that it is currently valid
    
    -- data
    first_name text not null,
    last_name text not null,
    batting_handedness bigint references taxa.handedness not null,
    pitching_handedness bigint references taxa.handedness not null,
    home text not null, -- birth location
    birth_season int not null,
    birthday_type bigint references taxa.day_type not null, 
    birthday_day int, -- null indicates this player was not born on a regular season day
    birthday_superstar_day int, -- null indicates this player was not born on a superstar day
    likes text not null, -- flavor
    dislikes text not null, -- flavor
    number int not null, -- AFAWK this does nothing
    mmolb_team_id text, -- null indicates this player is no longer on a team (e.g. relegated)
    position bigint references taxa.position not null,
    durability double precision not null, -- changes often -- may be extracted into its own table
    -- TODO Lesser boon
    -- TODO Greater boon
    -- TODO modifications
    unique (mmolb_id, valid_from)
);