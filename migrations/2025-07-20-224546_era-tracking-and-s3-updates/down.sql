alter table data.event_baserunners
    drop column source_event_index,
    drop column is_earned;

alter table data.events
    drop column errors_before,
    drop column errors_after,
    drop column cheer;

alter table data.games
    drop column stadium_name;

alter table taxa.event_type
    drop column is_error;