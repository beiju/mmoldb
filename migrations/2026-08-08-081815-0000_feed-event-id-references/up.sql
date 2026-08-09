truncate table data.team_games_played;
-- I am deleting this trigger and not re-adding it, because the architecture
-- should never create duplicate inserts now
drop trigger on_insert_team_games_played_trigger on data.team_games_played;
drop function data.on_insert_team_games_played();
alter table data.team_games_played
    drop constraint team_games_played_mmolb_team_id_feed_event_index_key,
    drop column feed_event_index,
    add column feed_event_id text not null,
    add constraint team_games_played_mmolb_team_id_feed_event_id_key unique (mmolb_team_id, feed_event_id);

truncate table data.player_attribute_augments;
-- I am deleting this trigger and not re-adding it, because the architecture
-- should never create duplicate inserts now
drop trigger on_insert_player_attribute_augment_trigger on data.player_attribute_augments;
drop function data.on_insert_player_attribute_augment();
alter table data.player_attribute_augments
    drop constraint player_augments_mmolb_player_id_feed_event_index_key,
    drop column feed_event_index,
    add column feed_event_id text not null,
    add column player_name text not null,
    add constraint player_attribute_augments_mmolb_player_id_feed_event_id_key unique (mmolb_player_id, feed_event_id);

truncate table data.player_paradigm_shifts;
-- I am deleting this trigger and not re-adding it, because the architecture
-- should never create duplicate inserts now
drop trigger on_insert_player_paradigm_shift_trigger on data.player_paradigm_shifts;
drop function data.on_insert_player_paradigm_shift();
alter table data.player_paradigm_shifts
    drop constraint player_paradigm_shifts_mmolb_player_id_feed_event_index_key,
    drop column feed_event_index,
    add column feed_event_id text not null,
    add column player_name text not null,
    add constraint player_paradigm_shifts_mmolb_player_id_feed_event_id_key unique (mmolb_player_id, feed_event_id);

truncate table data.player_recompositions;
-- I am deleting this trigger and not re-adding it, because the architecture
-- should never create duplicate inserts now
drop trigger on_insert_player_recomposition_trigger on data.player_recompositions;
drop function data.on_insert_player_recomposition();
alter table data.player_recompositions
    drop constraint unique_player_id_feed_event_index_inferred_event_index,
    drop column feed_event_index,
    add column feed_event_id text not null,
    add constraint player_recompositions_mmolb_player_id_feed_event_id_key unique (mmolb_player_id, feed_event_id);

truncate table data.feed_events_processed;