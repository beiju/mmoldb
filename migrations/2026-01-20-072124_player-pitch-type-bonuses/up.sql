-- this does not strictly belong here based on the filename but i'm sure it's fine
alter table data.player_pitch_type_versions
    alter column pitch_type drop not null; -- null = unrecognized pitch type. this is an ingest error
