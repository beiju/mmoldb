Checking team
Checking player
Checking game
begin;
truncate table
	-- version-derived tables for kind=team
	data.team_versions
	data.team_player_versions
	-- auxiliary tables for kind=team
	data.modifications
	-- feed-derived tables for kind=team
	data.team_games_played
	-- version-derived tables for kind=player
	data.player_versions
	data.player_modification_versions
	data.player_equipment_versions
	data.player_equipment_effect_versions
	data.player_report_versions
	data.player_report_attribute_versions
	data.player_pitch_type_versions
	data.player_pitch_type_bonus_versions
	data.player_pitch_category_bonus_versions
	-- feed-derived tables for kind=player
	data.player_recompositions
	data.player_attribute_augments
	data.player_paradigm_shifts
	-- base table for kind=game
	data.games
	-- child tables for kind=game
	data.events
	data.event_baserunners
	data.event_fielders
	data.event_balk_reasons
	data.aurora_photos
	data.consumption_contest_events
	data.consumption_contests
	data.door_prizes
	data.door_prize_items
	data.efflorescence
	data.efflorescence_growth
	data.ejections
	data.failed_ejections
	data.wither
	data.parties
	data.pitcher_changes
	data.event_cheers
	-- auxiliary tables for kind=game
	data.weather
	data.cheers
	data.balk_reasons
;
-- refresh materialized views for kind=game
refresh materialized view data.events_extended
end;
