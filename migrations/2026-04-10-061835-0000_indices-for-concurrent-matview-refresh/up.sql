-- These indices are needed for concurrent refreshes on their respective materialized view.
-- I want this so that we don't block out users while they're running

-- This table maintains a count, split by all the other columns. All the other columns are listed here.
create unique index offense_outcomes_unique on data.offense_outcomes(
    season,
    mmolb_league_id,
    fair_ball_direction,
    event_type,
    hit_base,
    fielding_error_type
);

-- This table maintains a count, split by all the other columns. All the other columns are listed here.
create unique index defense_outcomes_unique on data.defense_outcomes(
    season,
    mmolb_league_id,
    fair_ball_direction,
    event_type,
    hit_base,
    fielding_error_type
);

-- This table should be unique on player version, aka player id + valid_from
create unique index player_versions_extended_unique on data.player_versions_extended(
    mmolb_player_id,
    valid_from
);

-- While debugging the above index I found a problem with the on_insert_player_report_version_trigger
drop trigger on_insert_player_report_version_trigger on data.player_report_versions;
drop function data.on_insert_player_report_version;
create function data.on_insert_player_report_version()
    returns trigger as $$
begin
    -- check if the currently-valid version is exactly identical to the new version
    -- the list of columns must exactly match the ones in data.player_report_versions or
    -- we'll miss changes
    perform 1
    from data.player_report_versions prv
    where prv.mmolb_player_id = NEW.mmolb_player_id
      and prv.category = NEW.category
      and prv.valid_until is null
      -- note: "is not distinct from" is like "=" except for how it treats nulls.
      -- in postgres, NULL = NULL is false but NULL is not distinct from NULL is true
      and prv.season is not distinct from NEW.season
      and prv.day_type is not distinct from NEW.day_type
      and prv.day is not distinct from NEW.day
      and prv.superstar_day is not distinct from NEW.superstar_day
      and prv.quote is not distinct from NEW.quote
      and prv.included_attributes is not distinct from NEW.included_attributes;

    -- if there was an exact match, suppress this insert
    if FOUND then
        return null;
    end if;

    -- otherwise, close out the currently-valid version...
    update data.player_report_versions
    set valid_until = NEW.valid_from
    where mmolb_player_id = NEW.mmolb_player_id
      and category = NEW.category
      and valid_until is null;

    -- ...and close out any attributes that are no longer included...
    update data.player_report_attribute_versions
    set valid_until = NEW.valid_from
    where mmolb_player_id = NEW.mmolb_player_id
      and category = NEW.category
      and not attribute = ANY(NEW.included_attributes)
      and valid_until is null;

    -- ...and return the new row so it gets inserted as normal
    return NEW;
end;
$$ language plpgsql;

create trigger on_insert_player_report_version_trigger
    before insert on data.player_report_versions
    for each row
execute function data.on_insert_player_report_version();
