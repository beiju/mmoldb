drop materialized view data.defense_outcomes;
drop materialized view data.offense_outcomes;
drop view data.events_extended;

-- have to drop home_run_distance or it'll cause errors reverting further back
create view data.events_extended as
with game_end_times as (
    select min(tgp.time) as time, mmolb_game_id
    from data.team_games_played tgp
    group by mmolb_game_id
)
select
    e.id,
    e.game_id,
    e.game_event_index,
    e.fair_ball_event_index,
    e.inning,
    e.top_of_inning,
    e.event_type,
    e.hit_base,
    e.fair_ball_type,
    e.fair_ball_direction,
    e.fielding_error_type,
    e.pitch_type,
    e.pitch_speed,
    e.pitch_zone,
    e.described_as_sacrifice,
    e.is_toasty,
    e.balls_before,
    e.strikes_before,
    e.outs_before,
    e.outs_after,
    e.away_team_score_before,
    e.away_team_score_after,
    e.home_team_score_before,
    e.home_team_score_after,
    e.pitcher_name,
    e.pitcher_count,
    e.batter_name,
    e.batter_count,
    e.batter_subcount,
    e.errors_before,
    e.errors_after,
    e.cheer,
    e.fair_ball_fielder_name,
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
