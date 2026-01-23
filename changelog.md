Changelog
=========

Huge thanks to Astrid for Chron, without which none of this would be possible.

Another huge thanks to WoofyJack for `mmolb_parsing`, without which MMOLDB 
updates would be much slower. 

Contributors (project lifetime): WoofyJack, Ifhbiff, Centritide.

Upcoming
--------
- Fixes issues with closing out versions of boons and pitch types. These issues
  only ever existed on the staging server, so if you only use the connection 
  info from beiju.mmoldb.me you were unaffected.
- Adds `level` to `data.player_versions`, along with several columns that are
  meant for MMOLDB internal use. See docs for details (TODO docs).
- Adds `data.player_pitch_type_bonus_versions` and 
  `data.player_pitch_category_bonus_versions`, which contain the information 
  you would think. 
- Adds `taxa.pitch_category`.
- Adds `category` column to `taxa.pitch_type`.

2025-01-19 (staging only)
----------

- Season 10 support! Season 10 is a major change to MMOLB and incited the 
  following changes in MMOLB:
  - Adds `priority`, `xp`, and `name_suffix` columns to `data.player_versions`.
    See docs for details. **In particular, note that building a player's full 
    name from their `player_versions` row has changed.** This will affect any
    queries that correlate player versions with game events.
  - Columns `greater_boon` and `lesser_boon` are removed from 
    `data.player_versions`, as you can now have multiple of each (at least 
    according to the API). 
  - Boons are now treated as a special category of modification (even more 
    than they already were). They show in `data.player_modification_versions`,
    which has gained a new column, `modification_type`, to indicate whether 
    this modification is a greater boon, a lesser boon, or neither.
  - Adds `taxa.modification_type` to support the above change.
  - Items earned from door prizes and consumption contests can now have 
    multiple of the same type of affix (i.e. multiple prefixes or multiple 
    suffixes). The following columns have been renamed to be plural and
    changed from a nullable text column to an array:
    - In `data.door_prize_items`: `prefix`, `suffix`, `discarded_item_prefix`,
      and `discarded_item_suffix`.
    - In `data.consumption_contests`: `batting_team_prize_prefix`, 
      `batting_team_prize_suffix`, `defending_team_prize_prefix`, and 
      `defending_team_prize_suffix`.
  - The Clubhouse is gone from MMOLB, and with it went quotes. The `quote` 
    column of `data.player_report_versions` is now nullable, and will be `null`
    for any player versions in s10 and later. The overall structure of player
    reports remains the same, even though "reports" aren't a discrete concept 
    any more, for backwards compatibility.
  - Adds new table `data.player_pitch_type_versions` with information on the
    player's pitch types and frequency. Pitch type and pitch category bonuses
    are not yet available. Let us know in the MMOLDB discord channel if you 
    want to use these bonuses so we know to prioritize that work.
  - Adds `durability`, `prefix_position_type`, and `specialized` columns to 
    `data.player_equipment_versions`. See docs for details.
  - Adds `tier` column to `data.player_equipment_effect_versions`. See docs 
    for details.
- Fix many deserialization and parse errors, huge thanks once more to 
  WoofyJack.

2025-12-31
----------
- Consumption contest support. See documentation for details.
- Fixed a bunch of parse errors. Thanks WoofyJack for the help!

2025-12-15 Hotfix
-----------------
- Poke `data.events_extended` until it realizes that `home_run_distance` exists

2025-12-13
----------
- Docs are updated to catch up with a bunch of undocumented changes. Sorry this
  took so long!
- New tables: `data.wither`, `data.efflorescence`, `data.efflorescence_growth`, 
  and `data.failed_ejections`. See docs. (`data.wither` was added a while ago, 
  but wasn't added to the changelog at the time.)
  - In `data.wither`, `player_position` was renamed to `player_slot` and 
    `struggle_event_index` was renamed to `attempt_event_index`.
- Add `home_run_distance` to `data.event`. 
- `info.raw_events` is now a view into `data.entities`. As a consequence, 
  `game_id` is no longer available. Use `mmolb_game_id` to join on instead.
  `data.games` provides the mapping from `game_id` to `mmolb_game_id`.
- Rename `stars` in `data.player_report_attribute_versions` to `base_stars` and
  add `base_total`, `modified_stars`, and `modified_total`. See the docs for 
  details. (This change is from a while ago, but wasn't added to the changelog
  at the time.)
- `full_location` and `abbreviation` have been removed from the MMOLB API. 
  Those columns are now nullable in `data.team_versions` and will be `null` for
  all versions going forward. (This change is from a while ago, but wasn't added 
  to the changelog at the time.)
- Dramatically improve ingest speed and storage required. There are still 
  high memory usage issues, particularly with team feed ingest.
  - This change also deletes the `data.team_feed_versions` and 
    `data.player_feed_versions` tables. Those were for internal use and didn't
    have anything useful in them anyway.
- Improve speed of status page (again), and make the with-issues count update
  during ingest.
- Fix almost all ingest errors

2025-10-05 Hotfix 1
-------------------
- Speed up status page so it doesn't time out

2025-10-05
----------
- Implement a temporary solution to allow ingesting games and player versions
  while Chron is frozen
- Support tense changes in parsing (thanks WoofyJack!)
- The speculative `taxa.attribute_effect_type` value `Additive` (short for 
  "additive multiplier") has been renamed to its now-known name `Multiplier`. 
  The speculative `Multiplicative` value (short for "multiplicative 
  multiplier") has been removed until its actual name is known.
- Remove an incorrect filter in `data.player_versions_extended`

2025-09-30
----------
- Add Records page

2025-09-24
----------
- Add the materialized view `data.player_versions_extended` and its 
  documentation. Big thanks to Ifhbiff for a doing a lot of work creating and 
  documenting this view.
- Raise statement timeout to 30 minutes

2025-09-15
----------
- Add recomposes to the player API
- Add reports to the player API
- Remove a lot of erroneous inferred recomposes (probably false positives 
  caused by the player feed moving outside player objects)
- Fix `data.player_report_attribute_versions` and 
  `data.player_equipment_effect_versions` getting corrupted when the equipment
  slot is cleared / when the report is removed.
- Clean up the code

2025-09-07
----------
- Add `data.team_games_played` with the feed events for games ending. This is
  primarily useful for the timestamp. It can be used to find the proper team 
  and player versions to use for a given game. It is not necessarily guaranteed
  that every game has an entry in `team_games_played`.
- Add `data.events_extended` view with a some useful fields for events, 
  including `game_end_timestamp` derived from `team_games_played`. 
- Add `data.defense_outcomes` and `data.offense_outcomes` materialized views,
  with a count of occurrences of each event type broken down by season, league,
  and fielding position. I would also like to break it down by day type 
  (Regular season/Postseason/Superstar/Special event/Kumite/Offseason), but
  MMOLB has been inconsistent about how days are notated so that will require 
  some special care. These are currently not documented on the Docs page.
- Speed up player pages
- Fix ingest to not crash when it aborts, and to report the error on the status
  page.

2025-09-01
----------
- Add `fair_ball_fielder_name` to `data.events`
- Add more info to the player pages. This also makes them load very slowly, 
  for now. I have ideas on how to speed this up.
- Add season filtering to the player page. Append `?season=`season to the URL.

2025-08-30
----------
- Home run challenge games no longer show as errors (they still aren't 
  supported)
- Add special handling for [the bugged walkoff balk game][balkoff], so it also
  no longer shows as an error
- Fix parsing errors for players with last name Jr. (thanks WoofyJack)
- Stop incorrectly adding an automatic runner in the superstar game

[balkoff]: https://mmolb.com/watch/686662b85f5db4ab9490048d?event=402

2025-08-23
----------
- Party support
- Fix for games with players named "Jr." (thanks WoofyJack)
- Documentation for door prizes
- Documentation for team ingest
- Add tablefunc (permanently)

2025-08-20
----------
- Support for door prizes in season 5 (not yet documented). Party support 
  coming soon.
- Team ingest (not yet documented).

2025-08-11
----------

- Remove `data.player_report_attributes` and replace it with 
  `data.player_report_versions` and `data.player_report_attribute_versions`.
  These use the same `valid_from` and `valid_until` system that other 
  `_versions` tables do, reflecting the fact that player reports are now 
  live-updating. This also adds recording of clubhouse talk quotes, which were
  previously not in the database.
- Add tracking of coins earned (in both Prosperity and Geomagnetic Storms 
  weathers) and photo contest outcomes. See the docs on `data.games` for info.

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
