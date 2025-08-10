-- this update requires player reingest
truncate table
    data.modifications,
    data.player_versions,
    data.player_modification_versions,
    data.modifications,
    data.player_attribute_augments,
    data.player_recompositions,
    data.player_report_attributes,
    data.player_paradigm_shifts,
    data.player_feed_versions,
    data.player_equipment_versions,
    data.player_equipment_effect_versions;

-- change player reports to
-- 1. have a table per report in addition to one per attribute. this will store
--    the seasonday as well as the quote, which wasn't previously stored
-- 2. use valid_from and valid_until instead of observed
-- 3. allow nullable seasonday to also capture pre-seasonday reports

-- easier to drop and remake it
drop trigger on_insert_player_report_attribute_trigger on data.player_report_attributes;
drop function data.on_insert_player_report_attribute;
drop table data.player_report_attributes;

-- TODO Add this and any other new *_versions tables to the get-cursor query
create table data.player_report_versions (
    -- bookkeeping
    id bigserial primary key not null,
    mmolb_player_id text not null,
    category bigint references taxa.attribute_category not null,
    valid_from timestamp without time zone not null,
    valid_until timestamp without time zone default null,
    -- data
    season int, -- null for reports that were created before season was a thing
    day_type bigint references taxa.day_type, -- null for unrecognized day types
    day int, -- null indicates this report is not from a regular season day
    superstar_day int, -- null indicates this report is not from a superstar day
    quote text not null,
    -- this references taxa.attribute but postgres doesn't support foreign key constraints on array elements
    included_attributes bigint[] not null, -- used in closeout logic

    unique (mmolb_player_id, category, valid_from),
    unique nulls not distinct (mmolb_player_id, category, valid_until)
);

create function data.on_insert_player_report_version()
    returns trigger as $$
begin
    -- check if the currently-valid version is exactly identical to the new version
    -- the list of columns must exactly match the ones in data.player_report_versions or
    -- we'll miss changes
    perform 1
    from data.player_report_versions prv
    where prv.mmolb_player_id = NEW.mmolb_player_id
      and prv.category = NEW.category
      and prv.valid_until is null
      -- note: "is not distinct from" is like "=" except for how it treats nulls.
      -- in postgres, NULL = NULL is false but NULL is not distinct from NULL is true
      and prv.season is not distinct from NEW.season
      and prv.day_type is not distinct from NEW.day_type
      and prv.day is not distinct from NEW.day
      and prv.superstar_day is not distinct from NEW.superstar_day
      and prv.quote is not distinct from NEW.quote
      and prv.included_attributes is not distinct from NEW.included_attributes;

    -- if there was an exact match, suppress this insert
    if FOUND then
        return null;
    end if;

    -- otherwise, close out the currently-valid version...
    update data.player_report_versions
    set valid_until = NEW.valid_from
    where mmolb_player_id = NEW.mmolb_player_id
      and category = NEW.category
      and valid_until is null;

    -- ...and close out any attributes that are no longer included...
    update data.player_report_attribute_versions
    set valid_until = NEW.valid_until
    where mmolb_player_id = NEW.mmolb_player_id
      and category = NEW.category
      and not attribute = ANY(NEW.included_attributes)
      and valid_until is null;

    -- ...and return the new row so it gets inserted as normal
    return NEW;
end;
$$ language plpgsql;

create trigger on_insert_player_report_version_trigger
    before insert on data.player_report_versions
    for each row
execute function data.on_insert_player_report_version();

-- TODO Add this and any other new *_versions tables to the get-cursor query
create table data.player_report_attribute_versions (
    -- bookkeeping
    id bigserial primary key not null,
    mmolb_player_id text not null,
    category bigint references taxa.attribute_category not null,
    attribute bigint references taxa.attribute not null,
    valid_from timestamp without time zone not null,
    valid_until timestamp without time zone default null,
    -- data
    stars int not null, -- null indicates this report is not from a superstar day

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
      and prav.stars is not distinct from NEW.stars;

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

-- start tracking what reports there are so we can close them out when needed
alter table data.player_versions
    -- this references taxa.attribute_category but postgres doesn't support foreign key constraints on array elements
    add column included_report_categories bigint[] not null;

-- add yet more things to close out: report versions and report attribute versions
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
    set valid_until = NEW.valid_until
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
    set valid_until = NEW.valid_until
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

create index close_not_included_report_versions on data.player_report_versions (mmolb_player_id, category)
    where valid_until is null;

-- this should handle both non-included categories and non-included attributes
create index close_not_included_report_attribute_versions on data.player_report_attribute_versions (mmolb_player_id, category, attribute)
    where valid_until is null;
