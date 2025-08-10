alter table taxa.attribute
    alter column category set not null;

alter table data.player_modification_versions
    rename column modification_order to modification_index;