begin;
truncate table
    info.event_ingest_log
    , data.games
    -- child tables for kind=game
    , data.events
    , data.event_baserunners
    , data.event_fielders
    , data.event_balk_reasons
    , data.aurora_photos
    , data.consumption_contest_events
    , data.consumption_contests
    , data.door_prizes
    , data.door_prize_items
    , data.efflorescence
    , data.efflorescence_growth
    , data.ejections
    , data.failed_ejections
    , data.wither
    , data.parties
    , data.pitcher_changes
    , data.event_cheers
    -- auxiliary tables for kind=game
    , data.weather
    , data.cheers
    , data.balk_reasons
;
end;