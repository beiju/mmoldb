alter table data.door_prize_items
    add column equipped_by text, -- null means the item was not equipped
    add column discarded_item_emoji text, -- null means no item was discarded
    add column discarded_item_name text, -- null means no item was discarded
    add column discarded_item_rare_name text, -- null means no item was discarded or the item is not rare or above
    add column discarded_item_prefix text, -- null means no item was discarded or the item has no prefix
    add column discarded_item_suffix text; -- null means no item was discarded or the item has no suffix

alter table data.team_versions
    alter column abbreviation drop not null, -- removed from team objects
    alter column full_location drop not null; -- removed from team objects