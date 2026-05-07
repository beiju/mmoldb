alter table data.games
    add away_manager_name text, -- null = game was from before managers were added, or game could not be parsed
    add home_manager_name text; -- null = game was from before managers were added, or game could not be parsed