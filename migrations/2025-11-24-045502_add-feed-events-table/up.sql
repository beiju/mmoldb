create table data.feed_event_versions (
    kind text not null,
    entity_id text not null,
    feed_event_index integer not null,
    valid_from timestamp without time zone not null,
    valid_until timestamp without time zone default null,
    data jsonb not null,
primary key (kind, entity_id, feed_event_index, valid_from)
);

create function data.on_insert_feed_event_version()
    returns trigger as $$
begin
    -- check if the currently-valid version is exactly identical to the new version
    -- the list of columns must exactly match the ones in data.feed_event_versions or
    -- we'll miss events
    perform 1
    from data.feed_event_versions fev
    where fev.kind = NEW.kind
      and fev.entity_id = NEW.entity_id
      and fev.feed_event_index = NEW.feed_event_index
      and fev.valid_until is null
      -- note: "is not distinct from" is like "=" except for how it treats nulls.
      -- in postgres, NULL = NULL is false but NULL is not distinct from NULL is true
      and fev.data is not distinct from NEW.data;

    -- if there was an exact match, suppress this insert
    if FOUND then
        return null;
    end if;

    -- otherwise, close out the currently-valid version...
    update data.feed_event_versions
    set valid_until = NEW.valid_from
    where kind = NEW.kind
      and entity_id = NEW.entity_id
      and feed_event_index = NEW.feed_event_index
      and valid_until is null;

    -- ...and return the new row so it gets inserted as normal
    return NEW;
end;
$$ language plpgsql;

create trigger on_insert_feed_event_version_trigger
    before insert on data.feed_event_versions
    for each row
    execute function data.on_insert_feed_event_version();

create index close_feed_event_versions_index
    on data.feed_event_versions (kind, entity_id, feed_event_index)
    where valid_until is null;

create index cursor_feed_event_versions_index
    on data.feed_event_versions (kind, valid_from);
