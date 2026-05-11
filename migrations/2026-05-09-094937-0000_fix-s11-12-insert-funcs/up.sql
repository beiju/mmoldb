alter table data.team_versions
    add manager_name text; -- null = version from before manager names were added

drop trigger on_insert_team_version_trigger on data.team_versions;
drop function data.on_insert_team_version;
create function data.on_insert_team_version()
    returns trigger as $$
begin
    -- check if the currently-valid version is exactly identical to the new version
    -- the list of columns must exactly match the ones in data.team_versions or
    -- we'll miss changes
    perform 1
    from data.team_versions tv
    where tv.mmolb_team_id = NEW.mmolb_team_id
      and tv.valid_until is null
      -- note: "is not distinct from" is like "=" except for how it treats nulls.
      -- in postgres, NULL = NULL is false but NULL is not distinct from NULL is true
      and tv.name is not distinct from NEW.name
      and tv.emoji is not distinct from NEW.emoji
      and tv.color is not distinct from NEW.color
      and tv.location is not distinct from NEW.location
      and tv.full_location is not distinct from NEW.full_location
      and tv.abbreviation is not distinct from NEW.abbreviation
      and tv.championships is not distinct from NEW.championships
      and tv.mmolb_league_id is not distinct from NEW.mmolb_league_id
      and tv.ballpark_name is not distinct from NEW.ballpark_name
      and tv.manager_name is not distinct from NEW.manager_name
      and tv.num_players is not distinct from NEW.num_players;

    -- if there was an exact match, suppress this insert
    if FOUND then
        update data.team_versions
        set duplicates = duplicates + 1
        where mmolb_team_id = NEW.mmolb_team_id and valid_until is null;

        return null;
    end if;

    -- otherwise, close out the currently-valid version...
    update data.team_versions
    set valid_until = NEW.valid_from
    where mmolb_team_id = NEW.mmolb_team_id and valid_until is null;

    -- ...and close out any players that are now past the end...
    update data.team_player_versions
    set valid_until = NEW.valid_from
    where mmolb_team_id = NEW.mmolb_team_id
      and team_player_index >= NEW.num_players
      and valid_until is null;

    -- ...and return the new row so it gets inserted as normal
    return NEW;
end;
$$ language plpgsql;

create trigger on_insert_team_version_trigger
    before insert on data.team_versions
    for each row
execute function data.on_insert_team_version();

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
      and pv.greater_durability is not distinct from NEW.greater_durability
      and pv.lesser_durability is not distinct from NEW.lesser_durability
      and pv.num_modifications is not distinct from NEW.num_modifications
      and pv.num_greater_boons is not distinct from NEW.num_greater_boons
      and pv.num_lesser_boons is not distinct from NEW.num_lesser_boons
      and pv.occupied_equipment_slots is not distinct from NEW.occupied_equipment_slots
      and pv.included_report_categories is not distinct from NEW.included_report_categories
      and pv.priority is not distinct from NEW.priority
      and pv.xp is not distinct from NEW.xp
      and pv.name_suffix is not distinct from NEW.name_suffix
      and pv.level is not distinct from NEW.level
      and pv.num_pitch_types is not distinct from NEW.num_pitch_types
      and pv.included_pitch_type_bonuses is not distinct from NEW.included_pitch_type_bonuses
      and pv.included_pitch_category_bonuses is not distinct from NEW.included_pitch_category_bonuses;

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
      and modification_type = any(select id from taxa.modification_type where name='Modification')
      and modification_index >= NEW.num_modifications
      and valid_until is null;

    -- ...and any greater boons that are now past the end of the greater boons list...
    update data.player_modification_versions
    set valid_until = NEW.valid_from
    where mmolb_player_id = NEW.mmolb_player_id
      and modification_type = any(select id from taxa.modification_type where name='GreaterBoon')
      and modification_index >= NEW.num_greater_boons
      and valid_until is null;

    -- ...and any lesser boons that are now past the end of the lesser boons list...
    update data.player_modification_versions
    set valid_until = NEW.valid_from
    where mmolb_player_id = NEW.mmolb_player_id
      and modification_type = any(select id from taxa.modification_type where name='LesserBoon')
      and modification_index >= NEW.num_lesser_boons
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

    -- ...and any known pitch types which are now past the end of the array...
    update data.player_pitch_type_versions
    set valid_until = NEW.valid_from
    where mmolb_player_id = NEW.mmolb_player_id
      and pitch_type_index >= NEW.num_pitch_types
      and valid_until is null;

    -- ...and any pitch type bonuses that are no longer included...
    update data.player_pitch_type_bonus_versions
    set valid_until = NEW.valid_from
    where mmolb_player_id = NEW.mmolb_player_id
      and not pitch_type = ANY(NEW.included_pitch_type_bonuses)
      and valid_until is null;

    -- ...and any pitch category bonuses that are no longer included...
    update data.player_pitch_category_bonus_versions
    set valid_until = NEW.valid_from
    where mmolb_player_id = NEW.mmolb_player_id
      and not pitch_category = ANY(NEW.included_pitch_category_bonuses)
      and valid_until is null;

    -- ...and return the new row so it gets inserted as normal
    return NEW;
end;
$$ language plpgsql;

create trigger on_insert_player_version_trigger
    before insert on data.player_versions
    for each row
execute function data.on_insert_player_version();

