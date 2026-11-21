create table taxa.playing_surface (
    id bigserial primary key not null,
    name text not null,
    display_name text not null,
    unique (name)
);

create table data.ballpark_config_versions (
    -- bookkeeping
    id bigserial primary key not null,
    mmolb_team_id text not null, -- ballparks are identified by the team they belong to
    -- using "without time zone" because that's what the datablase did
    valid_from timestamp without time zone not null,
    valid_until timestamp without time zone, -- null means that it is currently valid
    duplicates int not null default 0,

    -- data
    basepath_length integer not null,
    center_field_depth integer not null,
    foul_territory_line integer not null,
    left_field_depth integer not null,
    lighting integer not null,
    playing_surface bigint references taxa.playing_surface not null,
    right_field_depth integer not null,
    wall_height integer not null,

    unique (mmolb_team_id, valid_from),
    unique nulls not distinct (mmolb_team_id, valid_until)
);

create function data.on_insert_ballpark_config_version()
    returns trigger as $$
begin
    -- check if the currently-valid version is exactly identical to the new version
    -- the list of columns must exactly match the ones in data.ballpark_config_versions or
    -- we'll miss changes
    perform 1
    from data.ballpark_config_versions bpv
    where bpv.mmolb_team_id = NEW.mmolb_team_id
      and bpv.valid_until is null
      -- note: "is not distinct from" is like "=" except for how it treats nulls.
      -- in postgres, NULL = NULL is false but NULL is not distinct from NULL is true
      and bpv.basepath_length is not distinct from NEW.basepath_length
      and bpv.center_field_depth is not distinct from NEW.center_field_depth
      and bpv.foul_territory_line is not distinct from NEW.foul_territory_line
      and bpv.left_field_depth is not distinct from NEW.left_field_depth
      and bpv.lighting is not distinct from NEW.lighting
      and bpv.playing_surface is not distinct from NEW.playing_surface
      and bpv.right_field_depth is not distinct from NEW.right_field_depth
      and bpv.wall_height is not distinct from NEW.wall_height;

    -- if there was an exact match, suppress this insert
    if FOUND then
        update data.ballpark_config_versions
        set duplicates = duplicates + 1
        where mmolb_team_id = NEW.mmolb_team_id and valid_until is null;

        return null;
    end if;

    -- otherwise, close out the currently-valid version...
    update data.ballpark_config_versions
    set valid_until = NEW.valid_from
    where mmolb_team_id = NEW.mmolb_team_id and valid_until is null;

    -- ...and return the new row so it gets inserted as normal
    return NEW;
end;
$$ language plpgsql;

create trigger on_insert_ballpark_config_version_trigger
    before insert on data.ballpark_config_versions
    for each row
execute function data.on_insert_ballpark_config_version();

-- I'm not doing a forced truncate on this -- it will need to be done
-- manually for running instances
alter table data.team_versions
    add column has_configured_ballpark boolean not null default false;

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
      and tv.manager_name is not distinct from NEW.manager_name
      and tv.num_players is not distinct from NEW.num_players
      and tv.has_configured_ballpark is not distinct from NEW.has_configured_ballpark;

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

    -- ...and close out any ballpark config, if we no longer have one ...
    update data.ballpark_config_versions
    set valid_until = NEW.valid_from
    where mmolb_team_id = NEW.mmolb_team_id
      and not NEW.has_configured_ballpark
      and valid_until is null;

    -- ...and return the new row so it gets inserted as normal
    return NEW;
end;
$$ language plpgsql;

create trigger on_insert_team_version_trigger
    before insert on data.team_versions
    for each row
execute function data.on_insert_team_version();