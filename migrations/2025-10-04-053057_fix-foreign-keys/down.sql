alter table data.parties
drop constraint parties_game_id_fkey,
    add constraint parties_game_id_fkey
        foreign key (game_id)
            references data.games(id);

alter table data.pitcher_changes
drop constraint pitcher_changes_game_id_fkey,
    add constraint pitcher_changes_game_id_fkey
        foreign key (game_id)
            references data.games(id);
