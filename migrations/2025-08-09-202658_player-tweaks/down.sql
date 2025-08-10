alter table data.player_report_attributes
    rename to player_reports;

alter table data.player_reports
    alter column observed type timestamp with time zone;

alter table data.player_attribute_augments
    rename to player_augments;

alter table data.player_modification_versions
    rename column modification_index to modification_order;

alter table taxa.attribute
    alter column category drop not null;