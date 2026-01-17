drop trigger on_insert_player_pitch_type_version_trigger on data.player_pitch_type_versions;
drop function data.on_insert_player_pitch_type_version;
drop table data.player_pitch_type_versions;

alter table data.player_versions
    drop column priority;

alter table data.player_report_versions
    alter column quote set not null;
