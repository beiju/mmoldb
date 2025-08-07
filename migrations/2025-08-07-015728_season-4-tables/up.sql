-- enforce full reingest
truncate table
    info.event_ingest_log,
    info.raw_events,
    data.event_fielders,
    data.event_baserunners,
    data.events,
    data.weather,
    data.games,
    data.player_versions,
    data.player_modification_versions,
    data.modifications,
    data.player_augments,
    data.player_recompositions,
    data.player_reports,
    data.player_paradigm_shifts,
    data.player_feed_versions,
    data.player_equipment_versions,
    data.player_equipment_effect_versions,
    info.ingest_timings,
    info.ingests;

create table data.ejections (
    -- bookkeeping
    id bigserial primary key not null,
    event_id bigint references data.events on delete cascade not null,

    -- actual data
    team_emoji text not null,
    team_name text not null,
    ejected_player_name text not null,
    ejected_player_slot bigint references taxa.slot not null,
    violation_type text not null,
    reason text not null,
    replacement_player_name text not null,
    replacement_player_slot bigint references taxa.slot -- null when this was a bench replacement
);

create index ejections_event_id_index on data.ejections (event_id);

create table data.aurora_photos (
    -- bookkeeping
    id bigserial primary key not null,
    event_id bigint references data.events on delete cascade not null,

    -- actual data
    is_listed_first bool not null,
    team_emoji text not null,
    player_slot bigint references taxa.slot not null,
    player_name text not null
);

create index aurora_photos_event_id_index on data.aurora_photos (event_id);
