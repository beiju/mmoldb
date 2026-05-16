-- Drop these. First, because they depend on data.events_extended, which we
-- also need to drop. Second, because it seems nobody uses them, so I can
-- get rid of them.
drop materialized view data.defense_outcomes;
drop materialized view data.offense_outcomes;

-- New table for what cheer messages exist
create table data.cheers (
    id bigserial primary key not null,
    cheer text not null,
    unique (cheer)
);

-- New table for what cheer message happens on an event
create table data.event_cheers (
    id bigserial primary key not null,
    event_id bigint references data.events not null,
    cheer_id bigint references data.cheers not null
);

-- New table for what balk reasons exist
create table data.balk_reasons (
    id bigserial primary key not null,
    balk_reason text not null,
    unique (balk_reason)
);

-- New table for what balk reason happens on an event
create table data.event_balk_reasons (
    id bigserial primary key not null,
    event_id bigint references data.events not null,
    balk_reason_id bigint references data.balk_reasons not null
);

-- Drop events_extended view, because it references the cheer column
drop view data.events_extended;

-- Remove cheer column from data.events, to be replaced by this foreign key table
alter table data.events
    drop column cheer;

-- Reinstate events_extended view, cheers computed the new way
-- (also add manager names)
create view data.events_extended as
with game_end_times as (
    select min(tgp.time) as time, mmolb_game_id
    from data.team_games_played tgp
    group by mmolb_game_id
)
select
    e.*,
    ch.cheer,
    br.balk_reason,
    -- I have to enumerate all game fields except id, because if I do *
    -- it tries to include g.id which conflicts with e.id
    g.mmolb_game_id,
    g.weather,
    g.season,
    g.day,
    g.superstar_day,
    g.away_team_emoji,
    g.away_team_name,
    g.away_team_mmolb_id,
    g.away_team_final_score,
    g.home_team_emoji,
    g.home_team_name,
    g.home_team_mmolb_id,
    g.home_team_final_score,
    g.is_ongoing,
    g.from_version,
    g.stadium_name,
    g.home_team_earned_coins,
    g.away_team_earned_coins,
    g.home_team_photo_contest_top_scorer,
    g.home_team_photo_contest_score,
    g.away_team_photo_contest_top_scorer,
    g.away_team_photo_contest_score,
    g.away_manager_name,
    g.home_manager_name,
    case when e.top_of_inning then g.away_team_mmolb_id else g.home_team_mmolb_id end as batting_team_mmolb_id,
    case when e.top_of_inning then g.home_team_mmolb_id else g.away_team_mmolb_id end as defending_team_mmolb_id,
    get.time as game_end_time
from data.events e
left join data.games g on g.id=e.game_id
left join game_end_times get on get.mmolb_game_id=g.mmolb_game_id
left join data.event_cheers ec on e.id=ec.event_id
left join data.cheers ch on ch.id=ec.cheer_id
left join data.event_balk_reasons eb on e.id=eb.event_id
left join data.balk_reasons br on br.id=eb.balk_reason_id;
