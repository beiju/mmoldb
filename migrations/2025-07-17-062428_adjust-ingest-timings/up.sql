-- Your SQL goes here
alter table info.ingest_timings drop fetch_duration;
alter table info.ingest_timings add deserialize_games_duration float8 not null default 0.0;