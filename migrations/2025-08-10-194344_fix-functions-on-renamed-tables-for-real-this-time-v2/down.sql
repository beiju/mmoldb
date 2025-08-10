drop trigger on_insert_player_modification_version_trigger on data.player_modification_versions;
drop function data.on_insert_player_modification_version;
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
