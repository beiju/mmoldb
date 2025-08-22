-- remove references to about-to-be-deleted fields, and also add closeout
-- of past-the-end player versions
drop trigger on_insert_team_version_trigger on data.team_versions;
drop function data.on_insert_team_version;
create function data.on_insert_team_version()
    returns trigger as $$
begin
    -- check if the currently-valid version is exactly identical to the new version
    -- the list of columns must exactly match the ones in data.team_versions or
    -- we'll miss changes
    perform 1
    from data.team_versions tv
    where tv.mmolb_team_id = NEW.mmolb_team_id
      and tv.valid_until is null
      -- note: "is not distinct from" is like "=" except for how it treats nulls.
      -- in postgres, NULL = NULL is false but NULL is not distinct from NULL is true
      and tv.name is not distinct from NEW.name
      and tv.emoji is not distinct from NEW.emoji
      and tv.color is not distinct from NEW.color
      and tv.location is not distinct from NEW.location
      and tv.full_location is not distinct from NEW.full_location
      and tv.abbreviation is not distinct from NEW.abbreviation
      and tv.championships is not distinct from NEW.championships
      and tv.mmolb_league_id is not distinct from NEW.mmolb_league_id
      and tv.ballpark_name is not distinct from NEW.ballpark_name
      and tv.num_players is not distinct from NEW.num_players;

    -- if there was an exact match, suppress this insert
    if FOUND then
        update data.team_versions
        set duplicates = duplicates + 1
        where mmolb_team_id = NEW.mmolb_team_id and valid_until is null;

        return null;
    end if;

    -- otherwise, close out the currently-valid version...
    update data.team_versions
    set valid_until = NEW.valid_from
    where mmolb_team_id = NEW.mmolb_team_id and valid_until is null;

    -- ...and close out any players that are now past the end...
    update data.team_player_versions
    set valid_until = NEW.valid_from
    where mmolb_team_id = NEW.mmolb_team_id
      and team_player_index >= NEW.num_players
      and valid_until is null;

    -- ...and return the new row so it gets inserted as normal
    return NEW;
end;
$$ language plpgsql;

create trigger on_insert_team_version_trigger
    before insert on data.team_versions
    for each row
execute function data.on_insert_team_version();

alter table data.team_versions
    -- this was removed, and I don't think historical values are useful
    drop column eligible,
    -- ditto
    drop column motes_used,
    -- this was probably exposed accidentally, since the info is available
    -- in ballpark_name
    drop column ballpark_word_1,
    drop column ballpark_word_2,
    drop column ballpark_suffix,
    drop column ballpark_use_city;

-- not sure if these indices do anything but they can't hurt, right
create index close_extra_team_player_versions on data.team_player_versions (mmolb_team_id, team_player_index)
    where valid_until is null;
