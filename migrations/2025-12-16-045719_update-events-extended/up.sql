drop materialized view data.defense_outcomes;
drop materialized view data.offense_outcomes;
drop view data.events_extended;

-- this is literally unchanged, but it needs to be re-added to refresh the columns that e.* returns
create view data.events_extended as
with game_end_times as (
    select min(tgp.time) as time, mmolb_game_id
    from data.team_games_played tgp
    group by mmolb_game_id
)
select
    e.*,
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
    case when e.top_of_inning then g.away_team_mmolb_id else g.home_team_mmolb_id end as batting_team_mmolb_id,
    case when e.top_of_inning then g.home_team_mmolb_id else g.away_team_mmolb_id end as defending_team_mmolb_id,
    get.time as game_end_time
from data.events e
         left join data.games g on g.id=e.game_id
         left join game_end_times get on get.mmolb_game_id=g.mmolb_game_id;

create materialized view data.offense_outcomes as
select
    count(1) as count,
    ee.season,
    bt.mmolb_league_id,
    ee.fair_ball_direction,
    ee.event_type,
    ee.hit_base,
    ee.fielding_error_type
from data.events_extended ee
         left join data.team_versions bt on bt.mmolb_team_id=ee.batting_team_mmolb_id
    and ee.game_end_time > bt.valid_from and ee.game_end_time <= coalesce(bt.valid_until, 'infinity')
group by
    ee.season,
    bt.mmolb_league_id,
    ee.fair_ball_direction,
    ee.event_type,
    ee.hit_base,
    ee.fielding_error_type;

create materialized view data.defense_outcomes as
select
    count(1) as count,
    ee.season,
    bt.mmolb_league_id,
    ee.fair_ball_direction,
    ee.event_type,
    ee.hit_base,
    ee.fielding_error_type
from data.events_extended ee
         left join data.team_versions bt on bt.mmolb_team_id=ee.batting_team_mmolb_id
    and ee.game_end_time > bt.valid_from and ee.game_end_time <= coalesce(bt.valid_until, 'infinity')
group by
    ee.season,
    bt.mmolb_league_id,
    ee.fair_ball_direction,
    ee.event_type,
    ee.hit_base,
    ee.fielding_error_type;
