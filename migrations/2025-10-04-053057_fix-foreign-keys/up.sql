-- pitcher changes should have been 'on delete cascade' from the start
alter table data.pitcher_changes
drop constraint pitcher_changes_game_id_fkey,
    add constraint pitcher_changes_game_id_fkey
        foreign key (game_id)
            references data.games(id)
            on delete cascade;

-- parties should have been 'on delete cascade' from the start
alter table data.parties
drop constraint parties_game_id_fkey,
    add constraint parties_game_id_fkey
        foreign key (game_id)
            references data.games(id)
            on delete cascade;