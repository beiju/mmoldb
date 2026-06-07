alter table data.player_equipment_versions
    drop column corrupted;

alter table data.player_equipment_effect_versions
    drop column phase,
    drop column zone,
    drop column implicit;

drop table taxa.attribute_effect_phase;