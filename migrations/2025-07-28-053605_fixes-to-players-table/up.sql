-- This migration exists to (1) recover from me mistakenly editing a trigger in
-- the previous migration after it was deployed and (2) drop all derived player
-- data after fixes to player ingest_games

truncate table
    data.player_versions,
    data.player_modification_versions,
    data.modifications,
    data.player_augments,
    data.player_recompositions,
    data.player_reports,
    data.player_paradigm_shifts;

drop trigger on_insert_player_recomposition_trigger on data.player_recompositions;
drop function data.on_insert_player_recomposition;

create function data.on_insert_player_recomposition()
    returns trigger as $$
begin
    perform 1
    from data.player_recompositions pa
    where pa.mmolb_player_id = NEW.mmolb_player_id
      -- note: "is not distinct from" is like "=" except for how it treats nulls.
      -- in postgres, NULL = NULL is false but NULL is not distinct from NULL is true
      and pa.feed_event_index is not distinct from NEW.feed_event_index
      and pa.time is not distinct from NEW.time;

    -- if there was an exact match, suppress this insert
    if FOUND then
        return null;
    end if;

    -- otherwise, return the new row so it gets inserted as normal
    return NEW;
end;
$$ language plpgsql;

create trigger on_insert_player_recomposition_trigger
    before insert on data.player_recompositions
    for each row
execute function data.on_insert_player_recomposition();
