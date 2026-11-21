begin;
truncate table
	-- base table for kind=game
	data.games
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
	, info.event_ingest_log
	-- auxiliary tables for kind=game
	, data.weather
	, data.cheers
	, data.balk_reasons
	;
-- refresh materialized views for kind=game
refresh materialized view info.entities_count;
refresh materialized view info.entities_with_issues_count;
end;
