drop trigger on_insert_team_player_version_trigger on data.team_player_versions;
drop function data.on_insert_team_player_version();

create function data.on_insert_team_player_version()
    returns trigger as $$
begin
    -- check if the currently-valid version is exactly identical to the new version
    -- the list of columns must exactly match the ones in data.team_player_versions or
    -- we'll miss changes
    perform 1
    from data.team_player_versions tpv
    where tpv.mmolb_team_id = NEW.mmolb_player_id
      and tpv.team_player_index = NEW.team_player_index
      and tpv.valid_until is null
      -- note: "is not distinct from" is like "=" except for how it treats nulls.
      -- in postgres, NULL = NULL is false but NULL is not distinct from NULL is true
      and tpv.first_name is not distinct from NEW.first_name
      and tpv.last_name is not distinct from NEW.last_name
      and tpv.number is not distinct from NEW.number
      and tpv.slot is not distinct from NEW.slot
      and tpv.mmolb_player_id is not distinct from NEW.mmolb_player_id;

    -- if there was an exact match, suppress this insert
    if FOUND then
        update data.team_player_versions
        set duplicates = duplicates + 1
        where mmolb_team_id = NEW.mmolb_team_id
          and team_player_index = NEW.team_player_index
          and valid_until is null;

        return null;
    end if;

    -- otherwise, close out the currently-valid version...
    update data.team_player_versions
    set valid_until = NEW.valid_from
    where mmolb_team_id = NEW.mmolb_team_id
      and team_player_index = NEW.team_player_index
      and valid_until is null;

    -- ...and return the new row so it gets inserted as normal
    return NEW;
end;
$$ language plpgsql;

create trigger on_insert_team_player_version_trigger
    before insert on data.team_player_versions
    for each row
execute function data.on_insert_team_player_version();