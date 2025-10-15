alter table data.player_report_attribute_versions rename column stars to base_stars;
alter table data.player_report_attribute_versions rename column total to base_total;
alter table data.player_report_attribute_versions
    alter column base_stars drop not null,
    add column modified_stars integer, -- null == not provided (this was added to the API late)
    add column modified_total double precision; -- null == not provided (this was added to the API late)

create table data.wither (
    -- bookkeeping
    id bigserial primary key not null,
    game_id bigint references data.games on delete cascade not null,
    struggle_game_event_index integer not null,
    outcome_game_event_index integer not null,
    team_emoji text not null,
    player_position bigint references taxa.slot not null,
    player_name text not null,
    corrupted bool not null
);
