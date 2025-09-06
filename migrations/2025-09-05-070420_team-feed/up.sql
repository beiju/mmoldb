create table data.team_feed_versions (
    id bigserial primary key,
    mmolb_team_id text not null,
    valid_from timestamp not null,
    valid_until timestamp,
    duplicates integer default 0 not null,
    num_entries integer not null,
    unique (mmolb_team_id, valid_from),
    unique nulls not distinct (mmolb_team_id, valid_until),
    constraint dates_coherent
        check ((valid_until IS NULL) OR (valid_until > valid_from))
);

create function data.on_insert_team_feed_version()
    returns trigger as $$
begin
    -- check if the currently-valid version is exactly identical to the new version
    -- the list of columns must exactly match the ones in data.team_feed_versions or
    -- we'll miss changes
    perform 1
    from data.team_feed_versions tfv
    where tfv.mmolb_team_id = NEW.mmolb_team_id
      and tfv.valid_until is null
      -- note: "is not distinct from" is like "=" except for how it treats nulls.
      -- in postgres, NULL = NULL is false but NULL is not distinct from NULL is true
      and tfv.num_entries is not distinct from NEW.num_entries;

    -- if there was an exact match, suppress this insert
    if FOUND then
        update data.team_feed_versions
        set duplicates = duplicates + 1
        where mmolb_team_id = NEW.mmolb_team_id and valid_until is null;

        return null;
    end if;

    -- otherwise, close out the currently-valid version...
    update data.team_feed_versions
    set valid_until = NEW.valid_from
    where mmolb_team_id = NEW.mmolb_team_id and valid_until is null;

    -- ...and return the new row so it gets inserted as normal
    return NEW;
end;
$$ language plpgsql;

create trigger on_insert_team_feed_version_trigger
    before insert on data.team_feed_versions
    for each row
execute function data.on_insert_team_feed_version();

create table data.team_games_played (
    -- bookkeeping
    id bigserial primary key not null,
    mmolb_team_id text not null,
    feed_event_index int not null,
    unique (mmolb_team_id, feed_event_index),

    -- data
    time timestamp without time zone not null,
    mmolb_game_id text not null
);

create function data.on_insert_team_games_played()
    returns trigger as $$
begin
    perform 1
    from data.team_games_played tgp
    where tgp.mmolb_team_id = NEW.mmolb_team_id
      -- note: "is not distinct from" is like "=" except for how it treats nulls.
      -- in postgres, NULL = NULL is false but NULL is not distinct from NULL is true
      and tgp.feed_event_index is not distinct from NEW.feed_event_index
      and tgp.time is not distinct from NEW.time
      and tgp.mmolb_game_id is not distinct from NEW.mmolb_game_id;

    -- if there was an exact match, suppress this insert
    if FOUND then
        return null;
    end if;

    -- otherwise, return the new row so it gets inserted as normal
    return NEW;
end;
$$ language plpgsql;

create trigger on_insert_team_games_played_trigger
    before insert on data.team_games_played
    for each row
execute function data.on_insert_team_games_played();
