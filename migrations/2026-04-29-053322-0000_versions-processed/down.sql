alter table data.feed_events_processed
    drop column skipped,
    drop column fatal_error;

drop table data.versions_processed;