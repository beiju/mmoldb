-- gotta delete all player ingest because we're adding non-nullable fields
-- (we could save seasons before corrupted items were added, but i don't feel like
-- doing the work for that)
truncate table
    data.player_versions,
    data.player_modification_versions,
    data.player_attribute_augments,
    data.player_recompositions,
    data.player_report_versions,
    data.player_report_attribute_versions,
    data.player_paradigm_shifts,
    data.player_equipment_versions,
    data.player_equipment_effect_versions,
    data.player_pitch_type_versions,
    data.player_pitch_type_bonus_versions,
    data.player_pitch_category_bonus_versions;

delete from data.versions_processed where kind = 'player';
delete from info.version_ingest_log where kind = 'player';

refresh materialized view info.entities_count;
refresh materialized view info.entities_with_issues_count;

-- 'Batting' or 'Pitching'
create table taxa.attribute_effect_phase (
    id bigserial primary key not null,
    name text not null,
    unique (name)
);

alter table data.player_equipment_effect_versions
    add column implicit bool not null,
    add column zone integer, -- null => this effect is not zone-specific
    add column phase bigint references taxa.attribute_effect_phase; -- null => this effect is not phase-specific

alter table data.player_equipment_versions
    add column corrupted bool not null;