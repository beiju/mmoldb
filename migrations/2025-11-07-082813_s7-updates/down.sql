alter table data.door_prize_items
    drop column equipped_by,
    drop column discarded_item_emoji,
    drop column discarded_item_name,
    drop column discarded_item_rare_name,
    drop column discarded_item_prefix,
    drop column discarded_item_suffix;

-- you can't revert this migration after null values have been inserted to this column
alter table data.team_versions
    alter column abbreviation set not null,
    alter column full_location set not null;