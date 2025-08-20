truncate table data.team_versions;

alter table data.team_versions
    add column num_players integer not null;

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
      and tv.ballpark_use_city is not distinct from NEW.ballpark_use_city
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

    -- ...and return the new row so it gets inserted as normal
    return NEW;
end;
$$ language plpgsql;

create trigger on_insert_team_version_trigger
    before insert on data.team_versions
    for each row
execute function data.on_insert_team_version();

create table data.team_player_versions (
    -- bookkeeping
    id bigserial primary key not null,
    mmolb_team_id text not null,
    team_player_index integer not null,
    -- using "without time zone" because that's what the datablase did
    valid_from timestamp without time zone not null,
    valid_until timestamp without time zone, -- null means that it is currently valid
    duplicates int not null default 0,

    -- data
    first_name text not null,
    last_name text not null,
    number integer not null,
    -- note: if this isn't nullable i have to filter entries in team_player_versions, and
    -- closing out team_player_versions that go from not-filtered-out to filtered-out is
    -- nontrivial
    slot bigint references taxa.slot, -- null = not recognized (maybe including non-drafted players?)
    mmolb_player_id text not null,

    unique (mmolb_team_id, team_player_index, valid_from),
    unique nulls not distinct (mmolb_team_id, team_player_index, valid_until)
);

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