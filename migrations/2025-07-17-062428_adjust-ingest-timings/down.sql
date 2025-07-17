-- This file should undo anything in `up.sql`
alter table info.ingest_timings drop get_batch_to_process_duration;
alter table info.ingest_timings drop deserialize_games_duration;
alter table info.ingest_timings add fetch_duration float8 not null default 0.0;