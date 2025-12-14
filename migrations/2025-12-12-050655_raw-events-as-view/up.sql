alter table info.event_ingest_log
    drop constraint event_ingest_log_game_id_game_event_index_fkey;

drop index info.raw_events_game_id;
drop table info.raw_events;

create view info.raw_events as select
    e.valid_from,
    e.entity_id as mmolb_game_id,
    ev.game_event_index,
    ev.event_raw->>'message' as event_text,
    e.data as game_raw,
    ev.event_raw
from data.entities e
         cross join lateral jsonb_array_elements(e.data->'EventLog') with ordinality as ev(event_raw, game_event_index)
where e.kind='game';

alter table info.ingest_timings
    drop column db_insert_insert_raw_events_duration;