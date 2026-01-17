create table taxa.slot_type (
    id bigserial primary key not null,
    name text not null,
    unique (name)
);

alter table data.player_report_versions
    alter column quote drop not null;

create table data.player_pitch_type_versions (
    -- bookkeeping
    id bigserial primary key not null,
    mmolb_player_id text not null,
    pitch_type_index int not null, -- refers to the order it appears in the list
    -- using "without time zone" because that's what the datablase did
    valid_from timestamp without time zone not null,
    valid_until timestamp without time zone, -- null means that it is currently valid
    duplicates int not null default 0,

    -- WARNING: When you add a new value here, also add it to the perform statement in
    -- on_insert_player_version().
    pitch_type bigint references taxa.pitch_type not null,
    frequency float8 not null,
    expect_full_precision bool not null,

    unique (mmolb_player_id, pitch_type_index, valid_from),
    unique nulls not distinct (mmolb_player_id, pitch_type_index, valid_until)
);

create function data.on_insert_player_pitch_type_version()
    returns trigger as $$
begin
    -- check if the currently-valid version is exactly identical to the new version
    -- the list of columns must exactly match the ones in data.player_pitch_types or
    -- we'll miss changes
    perform 1
    from data.player_pitch_type_versions pptv
    where pptv.mmolb_player_id = NEW.mmolb_player_id
      and pptv.pitch_type_index = NEW.pitch_type_index
      and pptv.valid_until is null
      -- note: "is not distinct from" is like "=" except for how it treats nulls.
      -- in postgres, NULL = NULL is false but NULL is not distinct from NULL is true
      and pptv.pitch_type is not distinct from NEW.pitch_type
      and pptv.frequency is not distinct from NEW.frequency
      and pptv.expect_full_precision is not distinct from NEW.expect_full_precision;

    -- if there was an exact match, suppress this insert
    if FOUND then
        update data.player_pitch_type_versions
        set duplicates = duplicates + 1
        where mmolb_player_id = NEW.mmolb_player_id
          and pitch_type_index = NEW.pitch_type_index
          and valid_until is null;

        return null;
    end if;

    -- otherwise, close out the currently-valid version...
    update data.player_pitch_type_versions
    set valid_until = NEW.valid_from
    where mmolb_player_id = NEW.mmolb_player_id
      and pitch_type_index = NEW.pitch_type_index
      and valid_until is null;

    -- ...and return the new row so it gets inserted as normal
    return NEW;
end;
$$ language plpgsql;

create trigger on_insert_player_pitch_type_version_trigger
    before insert on data.player_pitch_type_versions
    for each row
execute function data.on_insert_player_pitch_type_version();

alter table data.player_versions
    add column priority float8, -- null = this player version is from before priority was available
    add column xp integer, -- null = this player version is from before xp existed
    add column name_suffix text; -- null = no name suffix

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
      and pv.included_report_categories is not distinct from NEW.included_report_categories
      and pv.priority is not distinct from NEW.priority
      and pv.xp is not distinct from NEW.xp
      and pv.name_suffix is not distinct from NEW.name_suffix;

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

alter table data.player_equipment_versions
    add column durability integer, -- null = this version is from before durability
    add column prefix_position_type bigint references taxa.slot_type, -- null = this version is from before prefix_position_type
    add column specialized boolean; -- null = this version is from before specialized

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
      and pev.num_effects is not distinct from NEW.num_effects
      and pev.durability is not distinct from NEW.durability
      and pev.prefix_position_type is not distinct from NEW.prefix_position_type
      and pev.specialized is not distinct from NEW.specialized;

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

alter table data.player_equipment_effect_versions
    add column tier integer; -- null = this version is from before equipment effect tiers

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

-- TODO:
--   - New equipment fields in https://discord.com/channels/1136709081319604324/1366806318408532089/1461605344479416473
--   - On teams: how the lineup is currently ordered. This is public on the site so must be in the API somewhere