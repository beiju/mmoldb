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
    set valid_until = NEW.valid_until
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

drop index data.player_versions_extended_unique;
drop index data.defense_outcomes_unique;
drop index data.offense_outcomes_unique;