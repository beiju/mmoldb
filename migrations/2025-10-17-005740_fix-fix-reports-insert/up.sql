drop trigger on_insert_player_report_attribute_version_trigger on data.player_report_attribute_versions;
drop function data.on_insert_player_report_attribute_version;
create function data.on_insert_player_report_attribute_version()
    returns trigger as $$
begin
    -- check if the currently-valid version is exactly identical to the new version
    -- the list of columns must exactly match the ones in data.player_report_attribute_versions or
    -- we'll miss changes
    perform 1
    from data.player_report_attribute_versions prav
    where prav.mmolb_player_id = NEW.mmolb_player_id
      and prav.category = NEW.category
      and prav.attribute = NEW.attribute
      and prav.valid_until is null
      -- note: "is not distinct from" is like "=" except for how it treats nulls.
      -- in postgres, NULL = NULL is false but NULL is not distinct from NULL is true
      and prav.base_stars is not distinct from NEW.base_stars
      and prav.base_total is not distinct from NEW.base_total
      and prav.modified_stars is not distinct from NEW.modified_stars
      and prav.modified_total is not distinct from NEW.modified_total;

    -- if there was an exact match, suppress this insert
    if FOUND then
        return null;
    end if;

    -- otherwise, close out the currently-valid version...
    update data.player_report_attribute_versions
    set valid_until = NEW.valid_from
    where mmolb_player_id = NEW.mmolb_player_id
      and category = NEW.category
      and attribute = NEW.attribute
      and valid_until is null;

    -- ...and return the new row so it gets inserted as normal
    return NEW;
    end;
$$ language plpgsql;

create trigger on_insert_player_report_attribute_version_trigger
    before insert on data.player_report_attribute_versions
    for each row
    execute function data.on_insert_player_report_attribute_version();
