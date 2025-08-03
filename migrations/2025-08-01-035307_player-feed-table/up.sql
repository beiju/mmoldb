-- this table only exists to keep a record of which feeds we've ingested.
-- it probably won't be any use to anyone besides the ingest itself
create table data.player_feed_versions (
    -- bookkeeping
    id bigserial primary key not null,
    mmolb_player_id text not null,
    -- using "without time zone" because that's what the datablase did
    valid_from timestamp without time zone not null,
    valid_until timestamp without time zone, -- null means that it is currently valid
    duplicates int not null default 0,

    -- we need a dedup key for when the feed was inside the player/team objects.
    -- the number of entries in it should be enough -- afaik no feed entries were
    -- ever edited retroactively
    num_entries int not null,

    unique (mmolb_player_id, valid_from),
    unique nulls not distinct (mmolb_player_id, valid_until)
);

create function data.on_insert_player_feed_version()
    returns trigger as $$
begin
    -- check if the currently-valid version is exactly identical to the new version
    -- the list of columns must exactly match the ones in data.player_feed_versions or
    -- we'll miss changes
    perform 1
    from data.player_feed_versions pfv
    where pfv.mmolb_player_id = NEW.mmolb_player_id
      and pfv.valid_until is null
      -- note: "is not distinct from" is like "=" except for how it treats nulls.
      -- in postgres, NULL = NULL is false but NULL is not distinct from NULL is true
      and pfv.num_entries is not distinct from NEW.num_entries;

    -- if there was an exact match, suppress this insert
    if FOUND then
        update data.player_feed_versions
        set duplicates = duplicates + 1
        where mmolb_player_id = NEW.mmolb_player_id and valid_until is null;

        return null;
    end if;

    -- otherwise, close out the currently-valid version...
    update data.player_feed_versions
    set valid_until = NEW.valid_from
    where mmolb_player_id = NEW.mmolb_player_id and valid_until is null;

    -- ...and return the new row so it gets inserted as normal
    return NEW;
end;
$$ language plpgsql;

create trigger on_insert_player_feed_version_trigger
    before insert on data.player_feed_versions
    for each row
execute function data.on_insert_player_feed_version();


-- how effects are combined: multiplicative, additive, etc
create table taxa.attribute_effect_type (
    id bigserial primary key not null,
    name text not null,
    unique (name)
);


create table data.player_equipment_versions (
    -- bookkeeping
    id bigserial primary key not null,
    mmolb_player_id text not null,
    equipment_slot text not null,
    -- using "without time zone" because that's what the datablase did
    valid_from timestamp without time zone not null,
    valid_until timestamp without time zone, -- null means that it is currently valid
    duplicates int not null default 0,

    -- data
    emoji text not null,
    name text not null,
    special_type text, -- no idea what this is but it's null in mmolb_parsing
    description text,
    rare_name text,
    cost int,
    prefixes text[] not null,
    suffixes text[] not null,
    rarity text, -- null for reasons i dont understand"
    num_effects int not null, -- used in ingest

    unique (mmolb_player_id, equipment_slot, valid_from),
    unique nulls not distinct (mmolb_player_id, equipment_slot, valid_until)
);

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


create table data.player_equipment_effect_versions (
    -- bookkeeping
    id bigserial primary key not null,
    mmolb_player_id text not null,
    equipment_slot text not null,
    effect_index int not null,
    -- using "without time zone" because that's what the datablase did
    valid_from timestamp without time zone not null,
    valid_until timestamp without time zone, -- null means that it is currently valid
    duplicates int not null default 0,

    -- data
    attribute bigint references taxa.attribute not null,
    effect_type bigint references taxa.attribute_effect_type not null,
    value float8 not null,

    unique (mmolb_player_id, equipment_slot, effect_index, valid_from),
    unique nulls not distinct (mmolb_player_id, equipment_slot, effect_index, valid_until)
);

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

truncate table
    data.player_augments,
    data.player_paradigm_shifts,
    data.player_recompositions,
    data.player_versions;

alter table data.player_augments
    add column season int not null,
    add column day_type bigint references taxa.day_type, -- null for unrecognized day types
    add column day int, -- null indicates this is not a regular season day
    add column superstar_day int; -- null indicates this player was not born on a superstar day

alter table data.player_paradigm_shifts
    add column season int not null,
    add column day_type bigint references taxa.day_type, -- null for unrecognized day types
    add column day int, -- null indicates this is not a regular season day
    add column superstar_day int; -- null indicates this player was not born on a superstar day

alter table data.player_recompositions
    add column season int not null,
    add column day_type bigint references taxa.day_type, -- null for unrecognized day types
    add column day int, -- null indicates this is not a regular season day
    add column superstar_day int, -- null indicates this player was not born on a superstar day
    add column player_name_before text not null,
    add column player_name_after text not null;

alter table data.player_versions
    add column num_modifications int not null;

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
      and pv.num_modifications is not distinct from NEW.num_modifications;

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


create index close_extra_equipment_effect_versions on data.player_equipment_effect_versions (mmolb_player_id, equipment_slot, effect_index)
    where valid_until is null;

create index close_extra_modification_versions on data.player_modification_versions (mmolb_player_id, modification_order)
    where valid_until is null;


-- i know i truncated most of these tables already but i want to make super sure this is a full reset
truncate table
    info.event_ingest_log,
    info.raw_events,
    data.event_fielders,
    data.event_baserunners,
    data.events,
    data.weather,
    data.games,
    data.player_versions,
    data.player_modification_versions,
    data.modifications,
    data.player_augments,
    data.player_recompositions,
    data.player_reports,
    data.player_paradigm_shifts,
    data.player_feed_versions,
    data.player_equipment_versions,
    data.player_equipment_effect_versions,
    info.ingest_timings,
    info.ingests;