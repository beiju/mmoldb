drop trigger on_insert_team_player_version_trigger on data.team_player_versions;
drop function data.on_insert_team_player_version;

drop table data.team_player_versions;

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
    where tv.mmolb_team_id = NEW.mmolb_player_id
      and tv.valid_until is null
      -- note: "is not distinct from" is like "=" except for how it treats nulls.
      -- in postgres, NULL = NULL is false but NULL is not distinct from NULL is true
      and tv.name is not distinct from NEW.name
      and tv.emoji is not distinct from NEW.emoji
      and tv.color is not distinct from NEW.color
      and tv.location is not distinct from NEW.location
      and tv.full_location is not distinct from NEW.full_location
      and tv.abbreviation is not distinct from NEW.abbreviation
      and tv.motto is not distinct from NEW.motto
      and tv.active is not distinct from NEW.active
      and tv.eligible is not distinct from NEW.eligible
      and tv.augments is not distinct from NEW.augments
      and tv.championships is not distinct from NEW.championships
      and tv.motes_used is not distinct from NEW.motes_used
      and tv.mmolb_league_id is not distinct from NEW.mmolb_league_id
      and tv.ballpark_name is not distinct from NEW.ballpark_name
      and tv.ballpark_word_1 is not distinct from NEW.ballpark_word_1
      and tv.ballpark_word_2 is not distinct from NEW.ballpark_word_2
      and tv.ballpark_suffix is not distinct from NEW.ballpark_suffix
      and tv.ballpark_use_city is not distinct from NEW.ballpark_use_city;

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

    -- ...and return the new row so it gets inserted as normal
    return NEW;
end;
$$ language plpgsql;

create trigger on_insert_team_version_trigger
    before insert on data.team_versions
    for each row
execute function data.on_insert_team_version();

alter table data.team_versions
    drop column num_players;