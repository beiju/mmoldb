-- enforce full reingest
truncate table
    info.event_ingest_log,
    info.raw_events,
    data.event_fielders,
    data.event_baserunners,
    data.events,
    data.weather,
    data.games,
    data.player_versions,
    data.player_modification_versions,
    data.modifications,
    data.player_augments,
    data.player_recompositions,
    data.player_reports,
    data.player_paradigm_shifts,
    data.player_feed_versions,
    data.player_equipment_versions,
    data.player_equipment_effect_versions,
    info.ingest_timings,
    info.ingests;

alter table data.player_recompositions
    drop constraint player_recompositions_mmolb_player_id_feed_event_index_key,
    add column inferred_event_index int, -- null = this event was not inferred
    add constraint unique_player_id_feed_event_index_inferred_event_index unique (mmolb_player_id, feed_event_index, inferred_event_index);

drop trigger on_insert_player_recomposition_trigger on data.player_recompositions;
drop function data.on_insert_player_recomposition;
create function data.on_insert_player_recomposition()
    returns trigger as $$
begin
    perform 1
    from data.player_recompositions pr
    where pr.mmolb_player_id = NEW.mmolb_player_id
      -- note: "is not distinct from" is like "=" except for how it treats nulls.
      -- in postgres, NULL = NULL is false but NULL is not distinct from NULL is true
      and pr.feed_event_index is not distinct from NEW.feed_event_index
      and pr.time is not distinct from NEW.time
      and pr.inferred_event_index is not distinct from NEW.inferred_event_index;

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
