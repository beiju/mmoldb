-- Your SQL goes here

create table data.entities (
    kind text not null,
    entity_id text not null,
    valid_from timestamp without time zone not null,
    data jsonb not null,
    primary key (kind, entity_id)
);

create function data.on_insert_entity()
    returns trigger as $$
begin
    -- When we get a new entity, delete the old one
    delete from data.entities en
    where en.kind = NEW.kind
      and en.entity_id = NEW.entity_id;

    return NEW;
end;
$$ language plpgsql;

create trigger on_insert_entity_trigger
    before insert on data.entities
    for each row
execute function data.on_insert_entity();

create table data.versions (
    kind text not null,
    entity_id text not null,
    valid_from timestamp without time zone not null,
    valid_to timestamp without time zone default null,
    data jsonb not null,
    primary key (kind, entity_id, valid_from)
);

create function data.on_insert_version()
    returns trigger as $$
begin
    -- When we get a new version, update the old one's valid_to.
    -- This should only
    update data.versions
        set valid_to = NEW.valid_from
        where kind = NEW.kind
            and entity_id = NEW.entity_id
            and valid_to is null;

    return NEW;
end;
$$ language plpgsql;

create trigger on_insert_version_trigger
    before insert on data.versions
    for each row
execute function data.on_insert_version();