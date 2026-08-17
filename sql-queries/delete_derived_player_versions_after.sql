begin;
	-- deleting from version-style table data.player
	-- deleting version-derived tables for kind=player
	delete from data.player_versions where valid_from >= '2026-01-11 11:31:33.496952';
	update data.player_versions set valid_until=null where valid_until >= '2026-01-11 11:31:33.496952';
	delete from data.player_modification_versions where valid_from >= '2026-01-11 11:31:33.496952';
	update data.player_modification_versions set valid_until=null where valid_until >= '2026-01-11 11:31:33.496952';
	delete from data.player_equipment_versions where valid_from >= '2026-01-11 11:31:33.496952';
	update data.player_equipment_versions set valid_until=null where valid_until >= '2026-01-11 11:31:33.496952';
	delete from data.player_equipment_effect_versions where valid_from >= '2026-01-11 11:31:33.496952';
	update data.player_equipment_effect_versions set valid_until=null where valid_until >= '2026-01-11 11:31:33.496952';
	delete from data.player_report_versions where valid_from >= '2026-01-11 11:31:33.496952';
	update data.player_report_versions set valid_until=null where valid_until >= '2026-01-11 11:31:33.496952';
	delete from data.player_report_attribute_versions where valid_from >= '2026-01-11 11:31:33.496952';
	update data.player_report_attribute_versions set valid_until=null where valid_until >= '2026-01-11 11:31:33.496952';
	delete from data.player_pitch_type_versions where valid_from >= '2026-01-11 11:31:33.496952';
	update data.player_pitch_type_versions set valid_until=null where valid_until >= '2026-01-11 11:31:33.496952';
	delete from data.player_pitch_type_bonus_versions where valid_from >= '2026-01-11 11:31:33.496952';
	update data.player_pitch_type_bonus_versions set valid_until=null where valid_until >= '2026-01-11 11:31:33.496952';
	delete from data.player_pitch_category_bonus_versions where valid_from >= '2026-01-11 11:31:33.496952';
	update data.player_pitch_category_bonus_versions set valid_until=null where valid_until >= '2026-01-11 11:31:33.496952';
	delete from data.versions_processed where kind = 'player' and valid_from >= '2026-01-11 11:31:33.496952';
	delete from info.version_ingest_log where kind = 'player' and valid_from >= '2026-01-11 11:31:33.496952';
	-- skipping non-selected kind game
	-- skipping non-selected kind team

	refresh materialized view info.entities_count;
	refresh materialized view info.entities_with_issues_count;
end;
