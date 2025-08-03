truncate table data.modifications, data.player_versions, data.player_modification_versions;
alter table data.player_versions
    add column occupied_equipment_slots text[] not null,
    add constraint dates_coherent check ( valid_until is null or valid_until > valid_from );

-- because we added a new column to player_versions, we have to update its trigger
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
      and pv.occupied_equipment_slots is not distinct from NEW.occupied_equipment_slots;

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

    -- ...and return the new row so it gets inserted as normal
    return NEW;
end;
$$ language plpgsql;

create trigger on_insert_player_version_trigger
    before insert on data.player_versions
    for each row
execute function data.on_insert_player_version();


alter table data.player_modification_versions
    add constraint dates_coherent check ( valid_until is null or valid_until > valid_from );

truncate table data.player_augments;
truncate table data.player_recompositions;
truncate table data.player_reports;

truncate table data.player_equipment_versions;
alter table data.player_equipment_versions
    add constraint dates_coherent check ( valid_until is null or valid_until > valid_from );

truncate table data.player_equipment_effect_versions;
alter table data.player_equipment_effect_versions
    add constraint dates_coherent check ( valid_until is null or valid_until > valid_from );

truncate table data.player_feed_versions;
alter table data.player_feed_versions
    add constraint dates_coherent check ( valid_until is null or valid_until > valid_from );



drop trigger on_insert_player_equipment_versions_trigger on data.player_equipment_versions;
drop function data.on_insert_player_equipment_versions;
create function data.on_insert_player_equipment_versions()
    returns trigger as $$
begin
    -- check if the currently-valid version is exactly identical to the new version
    -- the list of columns must exactly match the ones in data.player_feed_versions or
    -- we'll miss changes
    perform 1
    from data.player_equipment_versions pev
    where pev.mmolb_player_id = NEW.mmolb_player_id
      and pev.equipment_slot = NEW.equipment_slot
      and pev.valid_until is null
      -- note: "is not distinct from" is like "=" except for how it treats nulls.
      -- in postgres, NULL = NULL is false but NULL is not distinct from NULL is true
      and pev.emoji is not distinct from NEW.emoji
      and pev.name is not distinct from NEW.name
      and pev.special_type is not distinct from NEW.special_type
      and pev.description is not distinct from NEW.description
      and pev.rare_name is not distinct from NEW.rare_name
      and pev.cost is not distinct from NEW.cost
      and pev.prefixes is not distinct from NEW.prefixes
      and pev.suffixes is not distinct from NEW.suffixes
      and pev.rarity is not distinct from NEW.rarity
      and pev.num_effects is not distinct from NEW.num_effects;

    -- if there was an exact match, suppress this insert
    if FOUND then
        update data.player_equipment_versions
        set duplicates = duplicates + 1
        where mmolb_player_id = NEW.mmolb_player_id
          and equipment_slot = NEW.equipment_slot
          and valid_until is null;

        return null;
    end if;

    -- otherwise, close out the currently-valid version...
    update data.player_equipment_versions
    set valid_until = NEW.valid_from
    where mmolb_player_id = NEW.mmolb_player_id
      and equipment_slot = NEW.equipment_slot
      and valid_until is null;

    -- ...and return the new row so it gets inserted as normal
    return NEW;
end;
$$ language plpgsql;

create trigger on_insert_player_equipment_versions_trigger
    before insert on data.player_equipment_versions
    for each row
execute function data.on_insert_player_equipment_versions();
