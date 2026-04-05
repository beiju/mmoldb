alter table data.player_versions
    alter column durability drop not null,
    add column lesser_durability integer, -- null => version is pre-durability-split
    add column greater_durability integer;  -- null => version is pre-durability-split
