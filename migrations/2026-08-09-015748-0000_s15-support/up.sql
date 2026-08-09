create table taxa.pollen_count (
    id bigserial primary key not null,
    name text not null,
    display_name text not null,
    unique (name)
);

alter table data.games
    add column pollen_count bigint references taxa.pollen_count; -- null = not pollen weather