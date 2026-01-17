alter table data.player_report_versions
    alter column quote drop not null;

alter table data.player_versions
    add column priority float8; -- null = this player version is from before priority was available

create table data.player_pitch_type_versions (
    -- bookkeeping
    id bigserial primary key not null,
    mmolb_player_id text not null,
    pitch_type_index int not null, -- refers to the order it appears in the list
    -- using "without time zone" because that's what the datablase did
    valid_from timestamp without time zone not null,
    valid_until timestamp without time zone, -- null means that it is currently valid
    duplicates int not null default 0,

    -- WARNING: When you add a new value here, also add it to the perform statement in
    -- on_insert_player_version().
    pitch_type bigint references taxa.pitch_type not null,
    frequency float8 not null,
    expect_full_precision bool not null,

    unique (mmolb_player_id, pitch_type_index, valid_from),
    unique nulls not distinct (mmolb_player_id, pitch_type_index, valid_until)
);

create function data.on_insert_player_pitch_type_version()
    returns trigger as $$
begin
    -- check if the currently-valid version is exactly identical to the new version
    -- the list of columns must exactly match the ones in data.player_pitch_types or
    -- we'll miss changes
    perform 1
    from data.player_pitch_type_versions pptv
    where pptv.mmolb_player_id = NEW.mmolb_player_id
      and pptv.pitch_type_index = NEW.pitch_type_index
      and pptv.valid_until is null
      -- note: "is not distinct from" is like "=" except for how it treats nulls.
      -- in postgres, NULL = NULL is false but NULL is not distinct from NULL is true
      and pptv.pitch_type is not distinct from NEW.pitch_type
      and pptv.frequency is not distinct from NEW.frequency
      and pptv.expect_full_precision is not distinct from NEW.expect_full_precision;

    -- if there was an exact match, suppress this insert
    if FOUND then
        update data.player_pitch_type_versions
        set duplicates = duplicates + 1
        where mmolb_player_id = NEW.mmolb_player_id
          and pitch_type_index = NEW.pitch_type_index
          and valid_until is null;

        return null;
    end if;

    -- otherwise, close out the currently-valid version...
    update data.player_pitch_type_versions
    set valid_until = NEW.valid_from
    where mmolb_player_id = NEW.mmolb_player_id
      and pitch_type_index = NEW.pitch_type_index
      and valid_until is null;

    -- ...and return the new row so it gets inserted as normal
    return NEW;
end;
$$ language plpgsql;

create trigger on_insert_player_pitch_type_version_trigger
    before insert on data.player_pitch_type_versions
    for each row
execute function data.on_insert_player_pitch_type_version();


-- TODO:
--   - On teams: how the lineup is currently ordered. This is public on the site so must be in the API somewhere