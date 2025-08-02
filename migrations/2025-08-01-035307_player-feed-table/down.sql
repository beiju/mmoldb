drop index data.close_extra_modification_versions;
drop index data.close_extra_equipment_effect_versions;

-- we need to remove num_modifications from the trigger
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
      and pv.lesser_boon is not distinct from NEW.lesser_boon;

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


truncate table data.player_versions; -- it'll be truncated in up.sql anyway, and this makes the `alter table` fast
alter table data.player_versions
    drop column num_modifications;

truncate table data.player_recompositions; -- it'll be truncated in up.sql anyway, and this makes the `alter table` fast
alter table data.player_recompositions
    drop column season,
    drop column day_type,
    drop column day,
    drop column superstar_day,
    drop column player_name_before,
    drop column player_name_after;

truncate table data.player_paradigm_shifts; -- it'll be truncated in up.sql anyway, and this makes the `alter table` fast
alter table data.player_paradigm_shifts
    drop column season,
    drop column day_type,
    drop column day,
    drop column superstar_day;

truncate table data.player_augments; -- it'll be truncated in up.sql anyway, and this makes the `alter table` fast
alter table data.player_augments
    drop column season,
    drop column day_type,
    drop column day,
    drop column superstar_day;

drop trigger on_insert_player_equipment_effect_versions_trigger on data.player_equipment_effect_versions;
drop function data.on_insert_player_equipment_effect_versions;
drop table data.player_equipment_effect_versions;

drop trigger on_insert_player_equipment_versions_trigger on data.player_equipment_versions;
drop function data.on_insert_player_equipment_versions;
drop table data.player_equipment_versions;

drop table taxa.attribute_effect_type;

drop trigger on_insert_player_feed_version_trigger on data.player_feed_versions;
drop function data.on_insert_player_feed_version;
drop table data.player_feed_versions;
