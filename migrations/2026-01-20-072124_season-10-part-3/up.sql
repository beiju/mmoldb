-- This migration needs to add a non-nullable column to taxa.pitch_type
-- This will error if taxa.pitch_type has any values
-- So we want to truncate taxa.pitch_type
-- But we can't do that if there's values in data.player_pitch_type_versions, because it references taxa.pitch_type
-- So we want to truncate data.player_pitch_type_versions
-- The rule is that when you truncate one table, you have to truncate all tables derived from that entity
-- So we have to truncate all player-related tables, and taxa.pitch_type
-- But taxa.pitch_type is also referenced in data.events
-- So we have to truncate all game-related tables as well
truncate table
    data.weather,
    data.games,
    data.events,
    data.event_fielders,
    data.event_baserunners,
    data.ejections,
    data.failed_ejections,
    data.aurora_photos,
    data.door_prizes,
    data.door_prize_items,
    data.efflorescence,
    data.efflorescence_growth,
    data.pitcher_changes,
    data.parties,
    data.wither,
    data.consumption_contests,
    data.consumption_contest_events,
    info.event_ingest_log,

    data.player_versions,
    data.player_modification_versions,
    data.player_attribute_augments,
    data.player_recompositions,
    data.player_report_versions,
    data.player_report_attribute_versions,
    data.player_paradigm_shifts,
    data.player_equipment_versions,
    data.player_equipment_effect_versions,
    data.player_pitch_type_versions,
    taxa.pitch_type;

-- We deleted all the player versions, so we should delete the ingest logs related to them
-- (the ingest logs related to games are in a different table, which we truncated)
delete from info.version_ingest_log where kind='player';

alter table data.player_pitch_type_versions
    alter column pitch_type drop not null; -- null = unrecognized pitch type. this is an ingest error

create table data.player_pitch_type_bonus_versions (
    -- bookkeeping
    id bigserial primary key not null,
    mmolb_player_id text not null,
    pitch_type bigint not null references taxa.pitch_type,
    -- using "without time zone" because that's what the datablase did
    valid_from timestamp without time zone not null,
    valid_until timestamp without time zone, -- null means that it is currently valid
    duplicates int not null default 0,

    -- WARNING: When you add a new value here, also add it to the perform statement in
    -- on_insert_player_pitch_type_bonus_version().
    bonus float8 not null,

    unique (mmolb_player_id, pitch_type, valid_from),
    unique nulls not distinct (mmolb_player_id, pitch_type, valid_until)
);

create function data.on_insert_player_pitch_type_bonus_version()
    returns trigger as $$
begin
    -- check if the currently-valid version is exactly identical to the new version
    -- the list of columns must exactly match the ones in data.player_pitch_types or
    -- we'll miss changes
    perform 1
    from data.player_pitch_type_bonus_versions pptbv
    where pptbv.mmolb_player_id = NEW.mmolb_player_id
      and pptbv.pitch_type = NEW.pitch_type
      and pptbv.valid_until is null
      -- note: "is not distinct from" is like "=" except for how it treats nulls.
      -- in postgres, NULL = NULL is false but NULL is not distinct from NULL is true
      and pptbv.pitch_type is not distinct from NEW.pitch_type
      and pptbv.bonus is not distinct from NEW.bonus;

    -- if there was an exact match, suppress this insert
    if FOUND then
    update data.player_pitch_type_bonus_versions
    set duplicates = duplicates + 1
    where mmolb_player_id = NEW.mmolb_player_id
      and pitch_type = NEW.pitch_type
      and valid_until is null;

    return null;
    end if;

    -- otherwise, close out the currently-valid version...
    update data.player_pitch_type_bonus_versions
    set valid_until = NEW.valid_from
    where mmolb_player_id = NEW.mmolb_player_id
      and pitch_type = NEW.pitch_type
      and valid_until is null;

    -- ...and return the new row so it gets inserted as normal
    return NEW;
end;
$$ language plpgsql;

create trigger on_insert_player_pitch_type_bonus_version_trigger
    before insert on data.player_pitch_type_bonus_versions
    for each row
    execute function data.on_insert_player_pitch_type_bonus_version();

create table taxa.pitch_category (
    id bigserial primary key not null,
    name text not null,
    unique (name)
);

alter table taxa.pitch_type
    add column category bigint references taxa.pitch_category not null;

create table data.player_pitch_category_bonus_versions (
    -- bookkeeping
    id bigserial primary key not null,
    mmolb_player_id text not null,
    pitch_category bigint not null references taxa.pitch_category,
    -- using "without time zone" because that's what the datablase did
    valid_from timestamp without time zone not null,
    valid_until timestamp without time zone, -- null means that it is currently valid
    duplicates int not null default 0,

    -- WARNING: When you add a new value here, also add it to the perform statement in
    -- on_insert_player_pitch_category_bonus_version().
    bonus float8 not null,

    unique (mmolb_player_id, pitch_category, valid_from),
    unique nulls not distinct (mmolb_player_id, pitch_category, valid_until)
);

create function data.on_insert_player_pitch_category_bonus_version()
    returns trigger as $$
begin
    -- check if the currently-valid version is exactly identical to the new version
    -- the list of columns must exactly match the ones in data.player_pitch_categorys or
    -- we'll miss changes
    perform 1
    from data.player_pitch_category_bonus_versions pptbv
    where pptbv.mmolb_player_id = NEW.mmolb_player_id
      and pptbv.pitch_category = NEW.pitch_category
      and pptbv.valid_until is null
      -- note: "is not distinct from" is like "=" except for how it treats nulls.
      -- in postgres, NULL = NULL is false but NULL is not distinct from NULL is true
      and pptbv.pitch_category is not distinct from NEW.pitch_category
      and pptbv.bonus is not distinct from NEW.bonus;

    -- if there was an exact match, suppress this insert
    if FOUND then
    update data.player_pitch_category_bonus_versions
    set duplicates = duplicates + 1
    where mmolb_player_id = NEW.mmolb_player_id
      and pitch_category = NEW.pitch_category
      and valid_until is null;

    return null;
    end if;

    -- otherwise, close out the currently-valid version...
    update data.player_pitch_category_bonus_versions
    set valid_until = NEW.valid_from
    where mmolb_player_id = NEW.mmolb_player_id
      and pitch_category = NEW.pitch_category
      and valid_until is null;

    -- ...and return the new row so it gets inserted as normal
    return NEW;
end;
$$ language plpgsql;

create trigger on_insert_player_pitch_category_bonus_version_trigger
    before insert on data.player_pitch_category_bonus_versions
    for each row
    execute function data.on_insert_player_pitch_category_bonus_version();

------------------------------------------------------------------------------
-- Changes to data.player_versions, which is always very verbose to change. --
------------------------------------------------------------------------------

alter table data.player_versions
    add column level integer,  -- null = this version is from before players had levels
    add column num_greater_boons integer not null default 0,
    add column num_lesser_boons integer not null default 0,
    add column num_pitch_types integer not null default 0,
    add column included_pitch_type_bonuses bigint[] not null default array[]::bigint[],
    add column included_pitch_category_bonuses bigint[] not null default array[]::bigint[];

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

    -- ...and any greater boons that are now past the end of the modifications list...
    update data.player_modification_versions
    set valid_until = NEW.valid_from
    where mmolb_player_id = NEW.mmolb_player_id
      and modification_type = any(select id from taxa.modification_type where name='GreaterBoon')
      and modification_index >= NEW.num_greater_boons
      and valid_until is null;

    -- ...and any lesser boons that are now past the end of the modifications list...
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
