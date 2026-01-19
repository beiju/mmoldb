drop trigger on_insert_player_report_attribute_version_trigger on data.player_report_attribute_versions;
drop function data.on_insert_player_report_attribute_version;

drop table data.player_report_attribute_versions;
create table data.player_report_attribute_versions (
    -- bookkeeping
    id bigserial primary key not null,
    mmolb_player_id text not null,
    category bigint references taxa.attribute_category not null,
    attribute bigint references taxa.attribute not null,
    valid_from timestamp without time zone not null,
    valid_until timestamp without time zone default null,
    -- data
    base_stars int,
    base_total float8,
    base_subtotal float8,
    modified_stars int,
    modified_total float8,

    unique (mmolb_player_id, category, attribute, valid_from),
    unique nulls not distinct (mmolb_player_id, category, attribute, valid_until)
);

create function data.on_insert_player_report_attribute_version()
    returns trigger as $$
begin
    -- check if the currently-valid version is exactly identical to the new version
    -- the list of columns must exactly match the ones in data.player_report_attribute_versions or
    -- we'll miss changes
    perform 1
    from data.player_report_attribute_versions prav
    where prav.mmolb_player_id = NEW.mmolb_player_id
      and prav.category = NEW.category
      and prav.attribute = NEW.attribute
      and prav.valid_until is null
      -- note: "is not distinct from" is like "=" except for how it treats nulls.
      -- in postgres, NULL = NULL is false but NULL is not distinct from NULL is true
      and prav.base_stars is not distinct from NEW.base_stars
      and prav.base_total is not distinct from NEW.base_total
      and prav.base_subtotal is not distinct from NEW.base_subtotal
      and prav.modified_stars is not distinct from NEW.modified_stars
      and prav.modified_total is not distinct from NEW.modified_total;

    -- if there was an exact match, suppress this insert
    if FOUND then
        return null;
    end if;

    -- otherwise, close out the currently-valid version...
    update data.player_report_attribute_versions
    set valid_until = NEW.valid_from
    where mmolb_player_id = NEW.mmolb_player_id
      and category = NEW.category
      and attribute = NEW.attribute
      and valid_until is null;

    -- ...and return the new row so it gets inserted as normal
    return NEW;
end;
$$ language plpgsql;

create trigger on_insert_player_report_attribute_version_trigger
    before insert on data.player_report_attribute_versions
    for each row
execute function data.on_insert_player_report_attribute_version();


alter table data.player_equipment_effect_versions
    drop column tier;

drop trigger on_insert_player_equipment_effect_versions_trigger on data.player_equipment_effect_versions;
drop function data.on_insert_player_equipment_effect_versions;
create function data.on_insert_player_equipment_effect_versions()
    returns trigger as $$
begin
    -- check if the currently-valid version is exactly identical to the new version
    -- the list of columns must exactly match the ones in data.player_feed_versions or
    -- we'll miss changes
    perform 1
    from data.player_equipment_effect_versions peev
    where peev.mmolb_player_id = NEW.mmolb_player_id
      and peev.equipment_slot = NEW.equipment_slot
      and peev.effect_index = NEW.effect_index
      and peev.valid_until is null
      -- note: "is not distinct from" is like "=" except for how it treats nulls.
      -- in postgres, NULL = NULL is false but NULL is not distinct from NULL is true
      and peev.attribute is not distinct from NEW.attribute
      and peev.effect_type is not distinct from NEW.effect_type
      and peev.value is not distinct from NEW.value;

    -- if there was an exact match, suppress this insert
    if FOUND then
        update data.player_equipment_effect_versions
        set duplicates = duplicates + 1
        where mmolb_player_id = NEW.mmolb_player_id
          and equipment_slot = NEW.equipment_slot
          and effect_index = NEW.effect_index
          and valid_until is null;

        return null;
    end if;

    -- otherwise, close out the currently-valid version...
    update data.player_equipment_effect_versions
    set valid_until = NEW.valid_from
    where mmolb_player_id = NEW.mmolb_player_id
      and equipment_slot = NEW.equipment_slot
      and effect_index = NEW.effect_index
      and valid_until is null;

    -- ...and return the new row so it gets inserted as normal
    return NEW;
end;
$$ language plpgsql;

create trigger on_insert_player_equipment_effect_versions_trigger
    before insert on data.player_equipment_effect_versions
    for each row
execute function data.on_insert_player_equipment_effect_versions();

alter table data.player_equipment_versions
    drop column durability,
    drop column prefix_position_type,
    drop column specialized;

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

alter table data.player_versions
    drop column priority,
    drop column xp,
    drop column name_suffix;

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

drop trigger on_insert_player_pitch_type_version_trigger on data.player_pitch_type_versions;
drop function data.on_insert_player_pitch_type_version;
drop table data.player_pitch_type_versions;

alter table data.player_report_versions
    alter column quote set not null;

drop table taxa.slot_type;