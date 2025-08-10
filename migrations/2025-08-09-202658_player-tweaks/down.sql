alter table data.player_attribute_augments
    rename to player_augments;

alter table data.player_modification_versions
    rename column modification_index to modification_order;

alter table taxa.attribute
    alter column category drop not null;