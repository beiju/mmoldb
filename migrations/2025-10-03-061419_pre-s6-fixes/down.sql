-- pitcher changes should have been 'on delete cascade' from the start
alter table data.pitcher_changes
    drop constraint pitcher_changes_game_id_fkey,
    add constraint pitcher_changes_game_id_fkey
        foreign key (game_id)
            references data.games(id);

-- Your SQL goes here
alter table data.team_versions
    alter column championships set not null;