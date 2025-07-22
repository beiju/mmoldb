drop trigger on_insert_player_version_trigger on data.player_versions;
drop function data.on_insert_player_version;
drop table data.player_versions;

drop trigger on_insert_player_modification_version_trigger on data.player_modification_versions;
drop function data.on_insert_player_modification_version;
drop table data.player_modification_versions;

drop table data.modifications;

drop table taxa.day_type;
drop table taxa.handedness;
