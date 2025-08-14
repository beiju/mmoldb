-- this is completely unrelated to team ingest, it just happens to be added at the same time
create table info.ingest_counts (
    id bigserial primary key not null,
    ingest_id bigint references info.ingests not null,
    name text not null,
    count int not null,

    unique (ingest_id, name)  -- this constraint is used in update
);

create table info.version_ingest_log (
    id bigserial primary key not null,
    kind text not null,
    entity_id text not null,
    valid_from timestamp without time zone not null,
    log_index int not null,
    log_level int not null,
    log_text text not null
);

create table data.team_versions (
    -- bookkeeping
    id bigserial primary key not null,
    mmolb_team_id text not null,
    -- using "without time zone" because that's what the datablase did
    valid_from timestamp without time zone not null,
    valid_until timestamp without time zone, -- null means that it is currently valid
    duplicates int not null default 0,

    -- data
    name text not null,
    emoji text not null,
    color text not null,
    location text not null,
    full_location text not null,
    abbreviation text not null,
    motto text, -- null = not provided
    active bool not null,
    eligible bool, -- null = not provided
    augments integer not null,
    championships integer not null,
    motes_used int,  -- null = not provided on the object
    mmolb_league_id text, -- null = not provided on the object

    -- all ballpark attributes are null for versions before ballparks
    ballpark_name text,
    ballpark_word_1 text, -- additionally, null if this word isn't used
    ballpark_word_2 text, -- additionally, null if this word isn't used
    ballpark_suffix text,
    ballpark_use_city bool,

    -- the object also includes:
    --   - owner_id: not including because both personal and unnecessary
    --   - modifications: this is 1:N so it'll be in a child table
    --   - players: this is 1:N so it'll be in a child table
    --   - feed: this is 1:N so it'll be in a child table
    --   - record: this is 1:N so it'll probably be in a child table? undecided
    --   - season_records: changes too quickly, no plan to include it yet
    --   - inventory: no plans to include this because it was only available for a short time

    unique (mmolb_team_id, valid_from),
    unique nulls not distinct (mmolb_team_id, valid_until)
);

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