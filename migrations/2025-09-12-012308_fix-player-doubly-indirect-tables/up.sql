drop trigger on_insert_player_version_trigger on data.player_versions;
drop function data.on_insert_player_version;
create function data.on_insert_player_version()
    returns trigger as $$
begin
    -- check if the currently-valid version is exactly identical to the new version
    -- the list of columns must exactly match the ones in data.player_versions or
    -- we'll miss changes
    perform 1
    from data.player_versions pv
    where pv.mmolb_player_id = NEW.mmolb_player_id
      and pv.valid_until is null
      -- note: "is not distinct from" is like "=" except for how it treats nulls.
      -- in postgres, NULL = NULL is false but NULL is not distinct from NULL is true
      and pv.first_name is not distinct from NEW.first_name
      and pv.last_name is not distinct from NEW.last_name
      and pv.batting_handedness is not distinct from NEW.batting_handedness
      and pv.pitching_handedness is not distinct from NEW.pitching_handedness
      and pv.home is not distinct from NEW.home
      and pv.birthseason is not distinct from NEW.birthseason
      and pv.birthday_type is not distinct from NEW.birthday_type
      and pv.birthday_day is not distinct from NEW.birthday_day
      and pv.birthday_superstar_day is not distinct from NEW.birthday_superstar_day
      and pv.likes is not distinct from NEW.likes
      and pv.dislikes is not distinct from NEW.dislikes
      and pv.number is not distinct from NEW.number
      and pv.mmolb_team_id is not distinct from NEW.mmolb_team_id
      and pv.slot is not distinct from NEW.slot
      and pv.durability is not distinct from NEW.durability
      and pv.greater_boon is not distinct from NEW.greater_boon
      and pv.lesser_boon is not distinct from NEW.lesser_boon
      and pv.num_modifications is not distinct from NEW.num_modifications
      and pv.occupied_equipment_slots is not distinct from NEW.occupied_equipment_slots
      and pv.included_report_categories is not distinct from NEW.included_report_categories;

    -- if there was an exact match, suppress this insert
    if FOUND then
update data.player_versions
set duplicates = duplicates + 1
where mmolb_player_id = NEW.mmolb_player_id and valid_until is null;

return null;
end if;

    -- otherwise, close out the currently-valid version...
update data.player_versions
set valid_until = NEW.valid_from
where mmolb_player_id = NEW.mmolb_player_id and valid_until is null;

-- ...and any modifications that are now past the end of the modifications list...
update data.player_modification_versions
set valid_until = NEW.valid_from
where mmolb_player_id = NEW.mmolb_player_id
  and modification_index >= NEW.num_modifications
  and valid_until is null;

-- ...and any equipment in a slot that's no longer occupied...
update data.player_equipment_versions
set valid_until = NEW.valid_from
where mmolb_player_id = NEW.mmolb_player_id
  and not equipment_slot = ANY(NEW.occupied_equipment_slots)
  and valid_until is null;

-- ...and any equipment effects for equipment in a slot that's no longer occupied...
update data.player_equipment_effect_versions
set valid_until = NEW.valid_from
where mmolb_player_id = NEW.mmolb_player_id
  and not equipment_slot = ANY(NEW.occupied_equipment_slots)
  and valid_until is null;

-- ...and any reports that are no longer included...
update data.player_report_versions
set valid_until = NEW.valid_from
where mmolb_player_id = NEW.mmolb_player_id
  and not category = ANY(NEW.included_report_categories)
  and valid_until is null;

-- ...and any attributes for any reports that are no longer included...
update data.player_report_attribute_versions
set valid_until = NEW.valid_from
where mmolb_player_id = NEW.mmolb_player_id
  and not category = ANY(NEW.included_report_categories)
  and valid_until is null;

-- ...and return the new row so it gets inserted as normal
return NEW;
end;
$$ language plpgsql;

create trigger on_insert_player_version_trigger
    before insert on data.player_versions
    for each row
    execute function data.on_insert_player_version();


-- this change requires a reingest of player info to fix bad data
truncate table
    data.modifications,
    data.player_versions,
    data.player_modification_versions,
    data.player_attribute_augments,
    data.player_recompositions,
    data.player_report_versions,
    data.player_report_attribute_versions,
    data.player_paradigm_shifts,
    data.player_feed_versions,
    data.player_equipment_versions,
    data.player_equipment_effect_versions;
