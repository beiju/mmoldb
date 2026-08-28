drop trigger on_insert_time_version_trigger on data.time_versions;
drop function data.on_insert_time_version();
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
      and tv.season is not distinct from NEW.season
      and tv.day_type is not distinct from NEW.day_type
      and tv.superstar_day is not distinct from NEW.superstar_day
      and tv.season_status is not distinct from NEW.season_status;

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

alter table data.time_versions
    drop column pollen_count;
