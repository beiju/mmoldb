alter table data.player_versions
    drop column greater_durability,
    drop column lesser_durability,
    alter column durability set not null;
