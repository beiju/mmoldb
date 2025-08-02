alter table data.player_recompositions
    drop column season,
    drop column day_type,
    drop column day,
    drop column superstar_day,
    drop column player_name_before,
    drop column player_name_after;

alter table data.player_paradigm_shifts
    drop column season,
    drop column day_type,
    drop column day,
    drop column superstar_day;

alter table data.player_augments
    drop column season,
    drop column day_type,
    drop column day,
    drop column superstar_day;

drop trigger on_insert_player_equipment_effect_versions_trigger on data.player_equipment_effect_versions;
drop function data.on_insert_player_equipment_effect_versions;
drop table data.player_equipment_effect_versions;

drop trigger on_insert_player_equipment_versions_trigger on data.player_equipment_versions;
drop function data.on_insert_player_equipment_versions;
drop table data.player_equipment_versions;

drop table taxa.attribute_effect_type;

drop trigger on_insert_player_feed_version_trigger on data.player_feed_versions;
drop function data.on_insert_player_feed_version;
drop table data.player_feed_versions;
