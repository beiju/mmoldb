create table data.modification_effects (
    modification_name text not null,
    valid_from timestamp without time zone not null,
    valid_until timestamp without time zone, -- null = still valid
    attribute bigint references taxa.attribute not null,
    bonus_type bigint references taxa.attribute_effect_type not null,
    value float8 not null,

    primary key (modification_name, valid_from, attribute, bonus_type),
    unique (modification_name, valid_until, attribute, bonus_type)
);