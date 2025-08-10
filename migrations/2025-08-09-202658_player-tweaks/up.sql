alter table taxa.attribute
    alter column category set not null;

alter table data.player_modification_versions
    rename column modification_order to modification_index;

alter table data.player_augments
    rename to player_attribute_augments;

alter table data.player_reports
    alter column observed type timestamp without time zone;

alter table data.player_reports
    rename to player_report_attributes;