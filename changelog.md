Changelog
=========

Upcoming
--------

- Added changelog
- (Breaking) Deleted `taxa.hit_type`. Changed `data.events` column `hit_type` 
  to reference `taxa.base` instead and renamed it to `hit_base`.
- Change `taxa.base` column `bases_achieved` from bigint to int. It never 
  should have been a bigint in the first place.