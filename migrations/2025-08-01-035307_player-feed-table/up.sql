-- this table only exists to keep a record of which feeds we've ingested.
-- it probably won't be any use to anyone besides the ingest itself
create table data.player_feed_versions (
    -- bookkeeping
    id bigserial primary key not null,
    mmolb_player_id text not null,
    -- using "without time zone" because that's what the datablase did
    valid_from timestamp without time zone not null,
    valid_until timestamp without time zone, -- null means that it is currently valid
    duplicates int not null default 0,

    -- we need a dedup key for when the feed was inside the player/team objects.
    -- the number of entries in it should be enough -- afaik no feed entries were
    -- ever edited retroactively
    num_entries int not null
);

create function data.on_insert_player_feed_version()
    returns trigger as $$
begin
    -- check if the currently-valid version is exactly identical to the new version
    -- the list of columns must exactly match the ones in data.player_feed_versions or
    -- we'll miss changes
    perform 1
    from data.player_feed_versions pfv
    where pfv.mmolb_player_id = NEW.mmolb_player_id
      and pfv.valid_until is null
      -- note: "is not distinct from" is like "=" except for how it treats nulls.
      -- in postgres, NULL = NULL is false but NULL is not distinct from NULL is true
      and pfv.num_entries is not distinct from NEW.num_entries;

    -- if there was an exact match, suppress this insert
    if FOUND then
        update data.player_feed_versions
        set duplicates = duplicates + 1
        where mmolb_player_id = NEW.mmolb_player_id and valid_until is null;

        return null;
    end if;

    -- otherwise, close out the currently-valid version...
    update data.player_feed_versions
    set valid_until = NEW.valid_from
    where mmolb_player_id = NEW.mmolb_player_id and valid_until is null;

    -- ...and return the new row so it gets inserted as normal
    return NEW;
end;
$$ language plpgsql;

create trigger on_insert_player_feed_version_trigger
    before insert on data.player_feed_versions
    for each row
execute function data.on_insert_player_feed_version();
