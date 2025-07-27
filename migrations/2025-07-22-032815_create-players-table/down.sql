drop trigger on_insert_player_report_trigger on data.player_reports;
drop function data.on_insert_player_report;
drop table data.player_reports;

drop trigger on_insert_player_recomposition_trigger on data.player_recompositions;
drop function data.on_insert_player_recomposition;
drop table data.player_recompositions;

drop trigger on_insert_player_paradigm_shift_trigger on data.player_paradigm_shifts;
drop function data.on_insert_player_paradigm_shift;
drop table data.player_paradigm_shifts;

drop trigger on_insert_player_augment_trigger on data.player_augments;
drop function data.on_insert_player_augment;
drop table data.player_augments;

drop table taxa.attribute;
drop table taxa.attribute_category;

drop trigger on_insert_player_version_trigger on data.player_versions;
drop function data.on_insert_player_version;
drop table data.player_versions;

drop trigger on_insert_player_modification_version_trigger on data.player_modification_versions;
drop function data.on_insert_player_modification_version;
drop table data.player_modification_versions;

drop table data.modifications;

drop table taxa.day_type;
drop table taxa.handedness;

drop index versions_cursor_index;