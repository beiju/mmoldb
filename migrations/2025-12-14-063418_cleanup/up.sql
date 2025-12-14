alter table data.wither rename player_position to player_slot;
alter table data.wither rename struggle_game_event_index to attempt_game_event_index;

create materialized view info.entities_count as (
    select 'game' as kind, count(1) as count from data.games
    union
    select 'player' as kind, count(1) as count from data.player_versions
    union
    select 'team' as kind, count(1) as count from data.team_versions
    union
    select kind, count(1) as count from data.feed_events_processed group by kind
);

drop table data.player_feed_versions;
drop table data.team_feed_versions;