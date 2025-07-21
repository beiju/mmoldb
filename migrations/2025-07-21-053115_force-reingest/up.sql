-- This migration only exists to force a reingest because the parser
-- has been updated
truncate table
    info.event_ingest_log,
    info.raw_events,
    data.event_fielders,
    data.event_baserunners,
    data.events,
    data.games;
