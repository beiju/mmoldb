-- Your SQL goes here
create table data.time_versions (
    -- bookkeeping
    id bigserial primary key not null,
    -- using "without time zone" because that's what the datablase did
    valid_from timestamp without time zone not null,
    valid_until timestamp without time zone, -- null means that it is currently valid
    duplicates int not null default 0,

    season integer not null,
    day_type bigint references taxa.day_type, -- null = unknown day type (this is an ingest error)
    day integer,
    superstar_day integer
);

create function data.on_insert_time_version() returns trigger
    language plpgsql
as
$$
begin
    -- check if the currently-valid version is exactly identical to the new version
    -- the list of columns must exactly match the ones in data.time_versions or
    -- we'll miss changes
    perform 1
    from data.time_versions tv
      where tv.valid_until is null
      -- note: "is not distinct from" is like "=" except for how it treats nulls.
      -- in postgres, NULL = NULL is false but NULL is not distinct from NULL is true
      and tv.season is not distinct from NEW.season
      and tv.day is not distinct from NEW.day
      and tv.superstar_day is not distinct from NEW.superstar_day;

    -- if there was an exact match, suppress this insert
    if FOUND then
        update data.time_versions
        set duplicates = duplicates + 1
        where valid_until is null;

        return null;
    end if;

    -- otherwise, close out the currently-valid version...
    update data.time_versions
    set valid_until = NEW.valid_from
    where valid_until is null;

    -- ...and return the new row so it gets inserted as normal
    return NEW;
end;
$$;

create trigger on_insert_time_version_trigger
    before insert on data.time_versions
    for each row
execute function data.on_insert_time_version();