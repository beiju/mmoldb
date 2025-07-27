-- speeds up player ingest enormously
create index versions_cursor_index on data.versions (kind, valid_from, entity_id);

create table taxa.handedness (
    id bigserial primary key not null,
    name text not null,
    unique (name)
);

create table taxa.day_type (
    id bigserial primary key not null,
    name text not null,
    display_name text not null,
    unique (name)
);

create table data.modifications (
    id bigserial primary key not null,
    name text not null,
    emoji text not null,
    description text not null,
    unique (name, emoji, description)
);

create table data.player_modification_versions (
    -- bookkeeping
    id bigserial primary key not null,
    mmolb_player_id text not null,
    modification_order int not null, -- refers to the order it appears in the list
    -- using "without time zone" because that's what the datablase did
    valid_from timestamp without time zone not null,
    valid_until timestamp without time zone, -- null means that it is currently valid
    duplicates int not null default 0,

    -- WARNING: When you add a new value here, also add it to the perform statement in
    -- on_insert_player_version().
    modification_id bigint references data.modifications not null,

    unique (mmolb_player_id, modification_order, valid_from),
    unique nulls not distinct (mmolb_player_id, modification_order, valid_until)
);

-- TODO This currently doesn't close out any player_modification_versions
--   properly when the player's list of modifications shrinks. The last N
--   modifications in the list will remain "valid". This could be solved
--   by closing out _all_ modifications for a given (valid_from,
--   mmolb_player_id) when any new version for that player id is inserted.
--     actually wait, I think that doesn't quite work. if the 3rd mod in
--     the list changes, it'll close out the 1st and 2nd, but their inserts
--     will already have happened so nothing will add a new, open version.
--     this probably needs a separate query from the ingest app: close out
--     all versions whose modification_order is higher than the new max
--     modification order
create function data.on_insert_player_modification_version()
    returns trigger as $$
begin
    -- check if the currently-valid version is exactly identical to the new version
    -- the list of columns must exactly match the ones in data.player_modifications or
    -- we'll miss changes
    perform 1
    from data.player_modification_versions pmv
    where pmv.mmolb_player_id = NEW.mmolb_player_id
      and pmv.valid_until is null
      -- note: "is not distinct from" is like "=" except for how it treats nulls.
      -- in postgres, NULL = NULL is false but NULL is not distinct from NULL is true
      and pmv.modification_id is not distinct from NEW.modification_id
      and pmv.modification_order is not distinct from NEW.modification_order;

    -- if there was an exact match, suppress this insert
    if FOUND then
        update data.player_modification_versions
        set duplicates = duplicates + 1
        where mmolb_player_id = NEW.mmolb_player_id
          and modification_order = NEW.modification_order
          and valid_until is null;

        return null;
    end if;

    -- otherwise, close out the currently-valid version...
    update data.player_modification_versions
    set valid_until = NEW.valid_from
    where mmolb_player_id = NEW.mmolb_player_id
      and modification_order = NEW.modification_order
      and valid_until is null;

    -- ...and return the new row so it gets inserted as normal
    return NEW;
end;
$$ language plpgsql;

create trigger on_insert_player_modification_version_trigger
    before insert on data.player_modification_versions
    for each row
    execute function data.on_insert_player_modification_version();

create table data.player_versions (
    -- bookkeeping
    id bigserial primary key not null,
    mmolb_player_id text not null,
    -- using "without time zone" because that's what the datablase did
    valid_from timestamp without time zone not null,
    valid_until timestamp without time zone, -- null means that it is currently valid
    duplicates int not null default 0,

    -- data
    -- WARNING: When you add a new value here, also add it to the perform statement in
    -- on_insert_player_version().
    first_name text not null,
    last_name text not null,
    batting_handedness bigint references taxa.handedness, -- null means this handedness was not recognized
    pitching_handedness bigint references taxa.handedness, -- null means this handedness was not recognized
    home text not null, -- birth location
    birthseason int not null,
    birthday_type bigint references taxa.day_type, -- null for unrecognized birthday types
    birthday_day int, -- null indicates this player was not born on a regular season day
    birthday_superstar_day int, -- null indicates this player was not born on a superstar day
    likes text not null, -- flavor
    dislikes text not null, -- flavor
    number int not null, -- AFAWK this does nothing
    mmolb_team_id text, -- null indicates this player is no longer on a team (e.g. relegated)
    slot bigint references taxa.slot, -- null if slot is unrecognized
    durability double precision not null, -- changes often -- may be extracted into its own table
    greater_boon bigint references data.modifications, -- null means this player does not have a greater boon
    lesser_boon bigint references data.modifications, -- null means this player does not have a lesser boon

    unique (mmolb_player_id, valid_from),
    unique nulls not distinct (mmolb_player_id, valid_until)
);

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

-- this is "Batting", "Pitching", "Defense", and "Baserunning"
create table taxa.attribute_category (
    id bigserial primary key not null,
    name text not null,
    unique (name)
);

-- this is things like "Contact", "Selflessness", "Greed", etc.
create table taxa.attribute (
    id bigserial primary key not null,
    name text not null,
    category bigint references taxa.attribute_category, -- null for Luck and Priority (sometimes also called "Misc" attributes)
    -- might add adjectives eventually"
    unique (name)
);

create table data.player_augments (
    -- bookkeeping
    id bigserial primary key not null,
    mmolb_player_id text not null,
    feed_event_index int not null,
    unique (mmolb_player_id, feed_event_index),

    -- data
    time timestamp without time zone not null,
    attribute bigint references taxa.attribute not null,
    value int not null -- stored exactly as MMOLB gives it. common values are 5, 10, 15, 30, and 50
);

create function data.on_insert_player_augment()
    returns trigger as $$
begin
    perform 1
    from data.player_augments pa
    where pa.mmolb_player_id = NEW.mmolb_player_id
      -- note: "is not distinct from" is like "=" except for how it treats nulls.
      -- in postgres, NULL = NULL is false but NULL is not distinct from NULL is true
      and pa.feed_event_index is not distinct from NEW.feed_event_index
      and pa.time is not distinct from NEW.time
      and pa.attribute is not distinct from NEW.attribute
      and pa.value is not distinct from NEW.value;

    -- if there was an exact match, suppress this insert
    if FOUND then
        return null;
    end if;

    -- otherwise, return the new row so it gets inserted as normal
    return NEW;
end;
$$ language plpgsql;

create trigger on_insert_player_augment_trigger
    before insert on data.player_augments
    for each row
execute function data.on_insert_player_augment();

create table data.player_paradigm_shifts (
    -- bookkeeping
    id bigserial primary key not null,
    mmolb_player_id text not null,
    feed_event_index int not null,
    unique (mmolb_player_id, feed_event_index),

    -- data
    time timestamp without time zone not null,
    attribute bigint references taxa.attribute not null
);

create function data.on_insert_player_paradigm_shift()
    returns trigger as $$
begin
    perform 1
    from data.player_paradigm_shifts pa
    where pa.mmolb_player_id = NEW.mmolb_player_id
      -- note: "is not distinct from" is like "=" except for how it treats nulls.
      -- in postgres, NULL = NULL is false but NULL is not distinct from NULL is true
      and pa.feed_event_index is not distinct from NEW.feed_event_index
      and pa.time is not distinct from NEW.time
      and pa.attribute is not distinct from NEW.attribute;

    -- if there was an exact match, suppress this insert
    if FOUND then
        return null;
    end if;

    -- otherwise, return the new row so it gets inserted as normal
    return NEW;
end;
$$ language plpgsql;

create trigger on_insert_player_paradigm_shift_trigger
    before insert on data.player_paradigm_shifts
    for each row
execute function data.on_insert_player_paradigm_shift();


create table data.player_recompositions (
    -- bookkeeping
    id bigserial primary key not null,
    mmolb_player_id text not null,
    feed_event_index int not null,
    unique (mmolb_player_id, feed_event_index),

    -- data
    time timestamp without time zone not null
);

create function data.on_insert_player_recomposition()
    returns trigger as $$
begin
    perform 1
    from data.player_recompositions pa
    where pa.mmolb_player_id = NEW.mmolb_player_id
      -- note: "is not distinct from" is like "=" except for how it treats nulls.
      -- in postgres, NULL = NULL is false but NULL is not distinct from NULL is true
      and pa.feed_event_index is not distinct from NEW.feed_event_index
      and pa.time is not distinct from NEW.time;

    -- if there was an exact match, suppress this insert
    if FOUND then
        return null;
    end if;

    -- otherwise, return the new row so it gets inserted as normal
    return NEW;
end;
$$ language plpgsql;

create trigger on_insert_player_recomposition_trigger
    before insert on data.player_recompositions
    for each row
execute function data.on_insert_player_recomposition();


create table data.player_reports (
    -- bookkeeping
    id bigserial primary key not null,
    mmolb_player_id text not null,
    season int not null,
    day_type bigint references taxa.day_type, -- null for unrecognized day types
    day int, -- null indicates this report is not from a regular season day
    superstar_day int, -- null indicates this report is not from a superstar day
    unique (mmolb_player_id, season, day_type, day, superstar_day, attribute),

    -- data
    observed timestamp with time zone not null, -- the valid_from of the first entity with this report
    attribute bigint references taxa.attribute not null,
    stars int not null
);

create function data.on_insert_player_report()
    returns trigger as $$
begin
    perform 1
    from data.player_reports pr
    where pr.mmolb_player_id = NEW.mmolb_player_id
      -- note: "is not distinct from" is like "=" except for how it treats nulls.
      -- in postgres, NULL = NULL is false but NULL is not distinct from NULL is true
      and pr.day_type is not distinct from NEW.day_type
      and pr.day is not distinct from NEW.day
      and pr.superstar_day is not distinct from NEW.superstar_day
      -- NOTE: `observed` should NOT be in the conditions. Each attempted insert
      -- will have a new `observed`, but we only want to keep the first one.

      -- attribute and stars are not necessary for correctness. i'm keeping them for
      -- now to help me catch bugs
      and pr.attribute is not distinct from NEW.attribute
      and pr.stars is not distinct from NEW.stars;

    -- if there was an exact match, suppress this insert
    if FOUND then
        return null;
    end if;

    -- otherwise, return the new row so it gets inserted as normal
    return NEW;
end;
$$ language plpgsql;

create trigger on_insert_player_report_trigger
    before insert on data.player_reports
    for each row
execute function data.on_insert_player_report();
