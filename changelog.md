Changelog
=========

Upcoming
--------

- Added changelog.
- Added `abbreviation` to `taxa.pitch_type`.
- (Breaking) Deleted `taxa.hit_type`. Changed `data.events` column `hit_type` 
  to reference `taxa.base` instead and renamed it to `hit_base`.
- Change `taxa.base` column `bases_achieved` from bigint to int. It never 
  should have been a bigint in the first place.
- Added documentation for `taxa` and `data` tables. `info` coming soon.
- (Breaking) Renamed `home_team_id` and `away_team_id` to `home_team_mmolb_id` 
  and `away_team_mmolb_id` in `data.games`. 
- (Breaking) Removed `count_strikes` and `count_balls`, which used to store the 
  count after the event finished, which also means that it stored 0-0 for every
  PA-ending event.
- Added `strikes_before` and `balls_before`, which store the count at the
  beginning of the event. The count at the end of the event can be easily
  computed, and examples are included in the documentation.
- Added an index that will hopefully speed up queries that group by plate 
  appearance.
- Sprinkled a few more Postgres constraints into the db.
- Reordered `away_*` and `home_*` fields to put away first.
- Added the changelog to the index page.
