-- The post-Kumite reflection team doesn't have a `Championships` field
alter table data.team_versions
    alter column championships drop not null;

-- I decided to remove the speculative, never-used Multiplicative value
-- from the taxa because it's now confusing with the Multiplier value
delete from taxa.attribute_effect_type where id > 1;