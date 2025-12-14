create table data.team_feed_versions (
    -- bookkeeping
    id bigserial primary key not null,
    mmolb_team_id text not null,
    -- using "without time zone" because that's what the datablase did
    valid_from timestamp without time zone not null,
    valid_until timestamp without time zone, -- null means that it is currently valid
    duplicates int not null default 0,

    -- we need a dedup key for when the feed was inside the player/team objects.
    -- the number of entries in it should be enough -- afaik no feed entries were
    -- ever edited retroactively
    num_entries int not null,

    unique (mmolb_team_id, valid_from),
    unique nulls not distinct (mmolb_team_id, valid_until),
    constraint dates_coherent check ( valid_until is null or valid_until > valid_from )
);

create table data.player_feed_versions (
    -- bookkeeping
    id bigserial primary key not null,
    mmolb_player_id text not null,
    -- using "without time zone" because that's what the datablase did
    valid_from timestamp without time zone not null,
    valid_until timestamp without time zone, -- null means that it is currently valid
    duplicates int not null default 0,

    -- we need a dedup key for when the feed was inside the player/team objects.
    -- the number of entries in it should be enough -- afaik no feed entries were
    -- ever edited retroactively
    num_entries int not null,

    unique (mmolb_player_id, valid_from),
    unique nulls not distinct (mmolb_player_id, valid_until),
    constraint dates_coherent check ( valid_until is null or valid_until > valid_from )
);

alter table data.wither rename player_slot to player_position;
alter table data.wither rename attempt_game_event_index to struggle_game_event_index;

drop materialized view info.entities_count;