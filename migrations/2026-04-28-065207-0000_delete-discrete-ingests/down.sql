create table info.ingests (
    id bigserial primary key not null,
    started_at timestamp without time zone not null,
    finished_at timestamp without time zone,
    aborted_at timestamp without time zone,
    message text
);

create table info.ingest_timings (
    -- bookkeeping
    id bigserial primary key not null,
    ingest_id bigint references info.ingests on delete cascade not null ,
    index int not null,

    filter_finished_games_duration float8 not null,
    parse_and_sim_duration float8 not null,

    db_insert_duration float8 not null, -- overlaps all the other db_insert_duration
    db_insert_delete_old_games_duration float8 not null,
    db_insert_update_weather_table_duration float8 not null,
    db_insert_insert_games_duration float8 not null,
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
    save_duration float8 not null, -- overlaps everything but fetch_duration

    -- added later
    deserialize_games_duration float8 not null,
    get_batch_to_process_duration float8 not null
);

create table info.ingest_counts (
    id bigserial primary key not null,
    ingest_id bigint references info.ingests not null,
    name text not null,
    count int not null,

    unique (ingest_id, name)  -- this constraint is used in update
);

alter table data.games
    add column ingest bigserial references info.ingests not null;