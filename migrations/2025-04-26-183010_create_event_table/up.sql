create schema data;
create schema taxa;

create table taxa.event_type (
    id bigserial primary key not null,
    name text not null,
    display_name text not null,
    unique (name)
);

create table taxa.hit_type (
    id bigserial primary key not null,
    name text not null,
    base_number bigint not null,
    unique (name)
);

create table taxa.position (
    id bigserial primary key not null,
    name text not null,
    display_name text not null,
    abbreviation text not null,
    --Keeping this NOT NULL despite DH not needing a position for code simplicity
    position_area text not null,
    unique (name)
);

create table taxa.fair_ball_type (
    id bigserial primary key not null,
    name text not null,
    display_name text not null,
    unique (name)
);

create table taxa.base (
    id bigserial primary key not null,
    name text not null,
    bases_achieved bigint not null,
    unique (name)
);

create table taxa.base_description_format (
    id bigserial primary key not null,
    name text not null,
    unique (name)
);

create table taxa.fielding_error_type (
    id bigserial primary key not null,
    name text not null,
    unique (name)
);

create table taxa.pitch_type (
    id bigserial primary key not null,
    name text not null,
    display_name text not null,
    unique (name)
);

create table taxa.leagues (
    id bigserial primary key not null,
    name text not null,
	color text not null,
	emoji text not null,
	league_type text not null,
	parent_team_id text not null,
	mmolb_league_id text not null
);

create table data.weather (
    id bigserial primary key not null,
    name text not null,
    emoji text not null,
    tooltip text not null,
    unique (name, emoji, tooltip)
);

create table data.games (
    -- bookkeeping
    id bigserial primary key not null,
    ingest bigserial references info.ingests not null,
    mmolb_game_id text not null unique, -- note: unique causes an index to be built

    -- game metadata
    weather bigint references data.weather not null,
    season int not null,
    day int not null,
    away_team_emoji text not null,
    away_team_name text not null,
    away_team_id text not null,
    home_team_emoji text not null,
    home_team_name text not null,
    home_team_id text not null,

    -- Indicates whether this game has been ingested
    is_finished bool not null
);

create index games_ingest_id_index on data.games (ingest);

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

create table info.event_ingest_log (
    -- bookkeeping
    id bigserial primary key not null,
    game_id bigserial references data.games on delete cascade not null,
    game_event_index int not null,
    log_index int not null,
    foreign key (game_id, game_event_index) references info.raw_events (game_id, game_event_index),

    -- log data
    log_level int not null,
    log_text text not null
);

-- `on delete cascade` is very slow without the appropriate index
create index event_ingest_log_game_id on info.event_ingest_log (game_id);
-- dramatically speeds up a query used on the home page
create index event_ingest_log_log_level_index on info.event_ingest_log (log_level);

create table info.ingest_timings (
    -- bookkeeping
    id bigserial primary key not null,
    ingest_id bigint references info.ingests on delete cascade not null ,
    index int not null,

    -- Note: The next page of games is fetched while the previous one 
    -- is being saved, so fetch_duration overlaps with save_duration
    fetch_duration float8 not null,
    
    filter_finished_games_duration float8 not null,
    parse_and_sim_duration float8 not null,
    
    db_insert_duration float8 not null, -- overlaps all the other db_insert_duration
    db_insert_delete_old_games_duration float8 not null,
    db_insert_update_weather_table_duration float8 not null,
    db_insert_insert_games_duration float8 not null,
    db_insert_insert_raw_events_duration float8 not null,
    db_insert_insert_logs_duration float8 not null,
    db_insert_insert_events_duration float8 not null,
    db_insert_get_event_ids_duration float8 not null,
    db_insert_insert_baserunners_duration float8 not null,
    db_insert_insert_fielders_duration float8 not null,
    
    db_fetch_for_check_duration float8 not null, -- overlaps all the other db_fetch_for_check
    db_fetch_for_check_get_game_id_duration float8 not null,
    db_fetch_for_check_get_events_duration float8 not null,
    db_fetch_for_check_group_events_duration float8 not null,
    db_fetch_for_check_get_runners_duration float8 not null,
    db_fetch_for_check_group_runners_duration float8 not null,
    db_fetch_for_check_get_fielders_duration float8 not null,
    db_fetch_for_check_group_fielders_duration float8 not null,
    db_fetch_for_check_post_process_duration float8 not null,
    
    check_round_trip_duration float8 not null,
    insert_extra_logs_duration float8 not null,
    save_duration float8 not null -- overlaps everything but fetch_duration
);

-- `on delete cascade` is very slow without the appropriate index
create index game_ingest_timing_ingest_id on info.ingest_timings (ingest_id);

create table data.events (
    -- bookkeeping
    id bigserial primary key not null,
    game_id bigserial references data.games on delete cascade not null,
    game_event_index int not null,
    -- the event index for the "<player> hit a <distance> to <destination>"
    -- event, if there is one
    fair_ball_event_index int,
    inning int not null,
    top_of_inning boolean not null,

    -- event data
    event_type bigint references taxa.event_type not null,
    -- should be populated for every event_type==Hit
    hit_type bigint references taxa.hit_type,
    -- should be populated for every event type where there's a fair ball
    fair_ball_type bigint references taxa.fair_ball_type,
    fair_ball_direction bigint references taxa.position,
    -- populated when there's a fielding error
    fielding_error_type bigint references taxa.fielding_error_type,
    -- should always be populated for current list of events
    pitch_type bigint references taxa.pitch_type,
    -- should always be populated for current list of events
    pitch_speed float8,
    -- should always be populated for current list of events
    pitch_zone int,
    -- this is specifically *described* as sacrifice, because there is
    -- or was a bug that caused plays to incorrectly be called
    -- sacrifices. The bug is as yet unconfirmed, so it might be
    -- intended behavior, but it still doesn't follow the MLB
    -- definition of a sacrifice.
    -- this is null for events that can never be called sacrifices
    -- (those that can are caught outs and grounded double plays)
    described_as_sacrifice bool,
    count_balls int not null,
    count_strikes int not null,
    outs_before int not null,
    outs_after int not null,
    -- note: runs scored, outs on play, steal info, etc. are all computed from data.event_baserunners

    -- player info
    pitcher_name text not null,
    -- a number that starts at 0 and is incremented whenever a new pitcher takes the mound.
    -- each team has a separate pitcher_count.
    pitcher_count int not null,
    batter_name text not null,
    -- a number that starts at 0 and is incremented whenever the *next* batter steps up to the plate.
    -- each team has a separate batter_count. this can be used to distinguish the boundary
    -- between batter appearances, in combination with batter_subcount.
    batter_count int not null,
    -- a number that starts as 0 and is incremented whenever the *same* batter steps up to the plate.
    -- this only happens the inning after an inning-ending CS (for now).
    -- each batter_count has a separate batter_subcount. this number will usually
    -- be 0, sometimes be 1, and as of season 1 there is no way for it to ever be higher than 1
    batter_subcount int not null
    -- note: more data is in data.event_baserunners and data.event_fielders

    -- fair_ball_event_index, fair_ball_type, and fair_ball_direction should in sync w/r/t null-ness
    -- DEBUG: These constraints are disabled because the nature of a batch insert makes it hard to debug them. I have
    -- Rust-layer checks for violations, and I'll enable these once the Rust ones pass
--     constraint fair_ball_type_null_sync check ((fair_ball_event_index is not null) = (fair_ball_type is not null)),
--     constraint fair_ball_direction_null_sync check ((fair_ball_event_index is not null) = (fair_ball_direction is not null))
 );

-- `on delete cascade` is very slow without the appropriate index
create index events_game_id_index on data.events (game_id);

create table data.event_baserunners (
    -- bookkeeping
    id bigserial primary key not null,
    event_id bigint references data.events on delete cascade not null,

    -- actual data
    baserunner_name text not null,
    -- Base numbering plan:
    --   - 0 is home base, 1 is first, etc.
    --   - The batter is not considered to be "at home". A batter who
    --     reaches first base moves from null to 1.
    --   - Runners who score always reach home, meaning they will have
    --     a row in this table where their base_after is 0.
    --   - When a runner is out, they have a row in this table where
    --     is_out is true. They will have a base_after, which indicates
    --     which base they were out at. This is also when
    --     base_description_format becomes relevant, but I don't
    --     recommend assuming base_description_format == null
    --     correlates with is_out == true.
    --   - Batters (who become batter-runners and) who are put out at a
    --     named base have a row in this table where both base_before
    --     is null and is_out is true. Batters who get out in a way
    --     that doesn't name a base don't have a row in this table at
    --     all.
    --   - Runners who don't move during an event have a row in this
    --     table with base_before == base_after.
    --   - Runners stranded on base simply stop showing up in this
    --     table.
    --   - Similarly, the automatic runner who stars on 2nd during
    --     extra innings just starts showing up with a base_before
    --     of 2nd and no previous row with a base_before of null.
    base_before bigint references taxa.base, -- null == not on base before (i.e. this was the hit/walk/etc that put them on base)
    base_after bigint references taxa.base not null, -- `not null` is an experiment, it may have to become nullable
    is_out bool not null,
    base_description_format bigint references taxa.base_description_format, -- null == not applicable because this event didn't name the base in a way that could be formatted
    steal bool not null -- this records all ATTEMPTED steals. identify failed steals by looking at is_out
);

-- `on delete cascade` is very slow without the appropriate index
create index event_baserunners_event_id_index on data.event_baserunners (event_id);

create table data.event_fielders (
    -- bookkeeping
    id bigserial primary key not null,
    event_id bigint references data.events on delete cascade not null,

    -- actual data
    fielder_name text not null,
    fielder_position bigint references taxa.position not null,
    play_order int not null,
    perfect_catch bool -- null indicates this was not a catch
);

-- `on delete cascade` is very slow without the appropriate index
create index event_fielders_event_id_index on data.event_fielders (event_id);
