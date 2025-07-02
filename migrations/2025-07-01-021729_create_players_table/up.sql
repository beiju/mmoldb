create table taxa.handedness (
     id bigserial primary key not null,
     name text not null,
     unique (name)
);

create table taxa.day_type (
     id bigserial primary key not null,
     name text not null,
     display_name text not null,
     unique (name)
);

create table data.player_versions (
    -- bookkeeping
    id bigserial primary key not null,
    mmolb_id text not null,
    -- using "without time zone" because that's what the datablase did
    valid_from timestamp without time zone not null,
    valid_until timestamp without time zone, -- null means that it is currently valid
    duplicates int not null default 0,

    -- data
    -- WARNING: When you add a new value here, also add it to the perform statement in
    -- update_valid_until_on_new_row().
    first_name text not null,
    last_name text not null,
    batting_handedness bigint references taxa.handedness not null,
    pitching_handedness bigint references taxa.handedness not null,
    home text not null, -- birth location
    birthseason int not null,
    birthday_type bigint references taxa.day_type not null,
    birthday_day int, -- null indicates this player was not born on a regular season day
    birthday_superstar_day int, -- null indicates this player was not born on a superstar day
    likes text not null, -- flavor
    dislikes text not null, -- flavor
    number int not null, -- AFAWK this does nothing
    mmolb_team_id text, -- null indicates this player is no longer on a team (e.g. relegated)
    position bigint references taxa.position not null,
    durability double precision not null, -- changes often -- may be extracted into its own table
    -- TODO Lesser boon
    -- TODO Greater boon
    -- TODO modifications
    unique (mmolb_id, valid_from),
    unique nulls not distinct (mmolb_id, valid_until)
);

create function data.update_valid_until_on_new_row()
    returns trigger as $$
    begin
        -- check if the currently-valid version is exactly identical to the new version
        -- the list of columns must exactly match the ones in data.player_versions or
        -- we'll miss changes
        perform 1
            from data.player_versions pv
            where pv.mmolb_id = NEW.mmolb_id
                and pv.valid_until is null
                -- note: "is not distinct from" is like "=" except for how it treats nulls.
                -- in postgres, NULL = NULL is false but NULL is not distinct from NULL is true
                and pv.first_name is not distinct from NEW.first_name
                and pv.last_name is not distinct from NEW.last_name
                and pv.batting_handedness is not distinct from NEW.batting_handedness
                and pv.pitching_handedness is not distinct from NEW.pitching_handedness
                and pv.home is not distinct from NEW.home
                and pv.birthseason is not distinct from NEW.birthseason
                and pv.birthday_type is not distinct from NEW.birthday_type
                and pv.birthday_day is not distinct from NEW.birthday_day
                and pv.birthday_superstar_day is not distinct from NEW.birthday_superstar_day
                and pv.likes is not distinct from NEW.likes
                and pv.dislikes is not distinct from NEW.dislikes
                and pv.number is not distinct from NEW.number
                and pv.mmolb_team_id is not distinct from NEW.mmolb_team_id
                and pv.position is not distinct from NEW.position
                and pv.durability is not distinct from NEW.durability;

        -- if there was an exact match, suppress this insert
        if FOUND then
            update data.player_versions
                set duplicates = duplicates + 1
                where mmolb_id = NEW.mmolb_id and valid_until is null;

            return null;
        end if;

        -- otherwise, close out the currently-valid version...
        update data.player_versions
            set valid_until = NEW.valid_from
            where mmolb_id = NEW.mmolb_id and valid_until is null;

        -- ...and return the new row so it gets inserted as normal
        return NEW;
    end;
    $$ language plpgsql;


create trigger update_valid_until_on_new_row_trigger
    before insert on data.player_versions
    for each row
    execute function data.update_valid_until_on_new_row();