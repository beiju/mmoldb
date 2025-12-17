create table data.consumption_contests (
    -- bookkeeping
    id bigserial primary key not null,
    game_id bigint references data.games not null,
    first_game_event_index integer not null,
    last_game_event_index integer not null,

    -- actual data
    food_emoji text not null,
    food text not null,

    batting_team_mmolb_id text not null,
    batting_team_emoji text not null,
    batting_team_name text not null,
    batting_team_player_name text not null,
    batting_team_score integer not null,
    batting_team_tokens integer not null,
    batting_team_prize_emoji text, -- null = batting team didn't win or tie
    batting_team_prize_name text, -- null = batting team didn't win or tie
    batting_team_prize_rare_name text, -- null = batting team didn't win or tie, or won an item without a rare name
    batting_team_prize_prefix text, -- null = batting team didn't win or tie, or won an item without a prefix
    batting_team_prize_suffix text, -- null = batting team didn't win or tie, or won an item without a suffix

    defending_team_mmolb_id text not null,
    defending_team_emoji text not null,
    defending_team_name text not null,
    defending_team_player_name text not null,
    defending_team_score integer not null,
    defending_team_tokens integer not null,
    defending_team_prize_emoji text, -- null = batting team didn't win or tie
    defending_team_prize_name text, -- null = batting team didn't win or tie
    defending_team_prize_rare_name text, -- null = batting team didn't win or tie, or won an item without a rare name
    defending_team_prize_prefix text, -- null = batting team didn't win or tie, or won an item without a prefix
    defending_team_prize_suffix text -- null = batting team didn't win or tie, or won an item without a suffix
);

create table data.consumption_contest_events (
    -- bookkeeping
    id bigserial primary key not null,
    game_id bigint references data.games not null,
    first_game_event_index integer not null, -- This is for the purpose of associating events with their contest
    game_event_index integer not null,

    -- actual data
    batting_team_consumed integer not null,
    defending_team_consumed integer not null
);