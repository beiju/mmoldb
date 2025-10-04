-- The post-Kumite reflection team doesn't have a `Championships` field
alter table data.team_versions
    alter column championships drop not null;

-- I decided to remove the speculative, never-used Multiplicative value
-- from the taxa because it's now confusing with the Multiplier value
delete from taxa.attribute_effect_type where id > 1;

-- pitcher changes should have been 'on delete cascade' from the start
alter table data.pitcher_changes
    drop constraint pitcher_changes_game_id_fkey,
    add constraint pitcher_changes_game_id_fkey
        foreign key (game_id)
            references data.games(id)
            on delete cascade;