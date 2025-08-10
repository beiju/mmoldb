Changelog
=========

2025-08-10
----------

- Season 4 support! This includes aurora photos and ejections, both of which 
  have new tables. Thanks to WoofyJack for help on parsing. 
- Player ingest! Thanks to Centride on the MMOLB discord for data entry 
  assistance, and as usual big thanks to WoofyJack for work on parsing. Players
  have the following new tables, all documented in the docs page:
  - `data.modifications`
  - `data.player_versions`
  - `data.player_modification_versions`
  - `data.player_equipment_versions`
  - `data.player_equipment_effect_versions`
  - `data.player_feed_versions`
  - `data.player_attribute_augments`
  - `data.player_recompositions`
  - `data.player_paradigm_shifts`
  - `data.player_report_attributes`
- Fix HitByPitch erroneously being labeled as a ball in play

2025-07-20
----------

- Fix pagination on games-with-issues page
- Fix some incorrect metadata on taxa.event_type (thanks to Bagyilisk in the 
  MMOLB discord for pointing them out)
- Replace ingest. This shouldn't change anything for users of the public 
  database. The instructions for local copies will be wrong for the time being 
  though.
- Update mmolb-parsing to fix some game parsing issues. Thanks as always to 
  WoofyJack.

2025-07-18
----------

- Track runner attribution and earned runs for use in ERA queries

2025-07-18
----------

- Handle stadium names. Stadium name is now available on `data.games`.
- Handle cheers. Cheers are currently stored as text in `data.events`, but we
  plan to move to storing them in a child table like Weather.
- Handle the bug where NowBatting events were skipped after a mound visit at
  the beginning of Season 3.
- Handle the bug with duplicate NowBatting events on s3d5
- Handle prosperity weather messages

Known issues:
- Home run challenges are not parsed
- Walkoff balks are being left as an error until the MMOLB bug is confirmed
  resolved

2025-07-14
----------

- Fixed `hit_base` for home runs being `Third` for some reason
- Fixed foul ball handling relating to new `strikes_before` column
- Change `taxa.base` column `bases_achieved` from bigint to int again? I swear
  I did that in the last big update.
- Handle FallingStar events. We don't do anything with infusions, but we now 
  properly handle retirements.
- When the batter name parsed from the event doesn't match the stored batter 
  name, use the one from the event
- Don't use the batter name from the event metadata because it's wrong when a
  player has Retired
- Remove an unnecessary warning about the automatic runner that had lots of
  false positives
- Cheeky little CSS and HTML update after the deploy

2025-07-12 Hotfix 1
----------

- Added missing file to production build

2025-07-12
----------

- Added changelog.
- Added `abbreviation` to `taxa.pitch_type`.
- (Breaking) Deleted `taxa.hit_type`. Changed `data.events` column `hit_type` 
  to reference `taxa.base` instead and renamed it to `hit_base`.
- Change `taxa.base` column `bases_achieved` from bigint to int. It never 
  should have been a bigint in the first place.
- Added documentation for `taxa`, `data`, and `info` schemata.
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
