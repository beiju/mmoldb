-- update this for the new table name and the new rows
drop trigger on_insert_player_augment_trigger on data.player_attribute_augments;
drop function data.on_insert_player_augment;
create function data.on_insert_player_attribute_augment()
    returns trigger as $$
begin
    perform 1
    from data.player_attribute_augments paa
    where paa.mmolb_player_id = NEW.mmolb_player_id
      -- note: "is not distinct from" is like "=" except for how it treats nulls.
      -- in postgres, NULL = NULL is false but NULL is not distinct from NULL is true
      and paa.feed_event_index is not distinct from NEW.feed_event_index
      and paa.time is not distinct from NEW.time
      and paa.season is not distinct from NEW.season
      and paa.day_type is not distinct from NEW.day_type
      and paa.day is not distinct from NEW.day
      and paa.superstar_day is not distinct from NEW.superstar_day
      and paa.attribute is not distinct from NEW.attribute
      and paa.value is not distinct from NEW.value;

    -- if there was an exact match, suppress this insert
    if FOUND then
        return null;
    end if;

    -- otherwise, return the new row so it gets inserted as normal
    return NEW;
end;
$$ language plpgsql;

create trigger on_insert_player_attribute_augment_trigger
    before insert on data.player_attribute_augments
    for each row
execute function data.on_insert_player_attribute_augment();

-- this one just needs to be updated for the new rows
drop trigger on_insert_player_paradigm_shift_trigger on data.player_paradigm_shifts;
drop function data.on_insert_player_paradigm_shift;

create function data.on_insert_player_paradigm_shift()
    returns trigger as $$
begin
    perform 1
    from data.player_paradigm_shifts pps
    where pps.mmolb_player_id = NEW.mmolb_player_id
      -- note: "is not distinct from" is like "=" except for how it treats nulls.
      -- in postgres, NULL = NULL is false but NULL is not distinct from NULL is true
      and pps.feed_event_index is not distinct from NEW.feed_event_index
      and pps.time is not distinct from NEW.time
      and pps.season is not distinct from NEW.season
      and pps.day_type is not distinct from NEW.day_type
      and pps.day is not distinct from NEW.day
      and pps.superstar_day is not distinct from NEW.superstar_day
      and pps.attribute is not distinct from NEW.attribute;

    -- if there was an exact match, suppress this insert
    if FOUND then
        return null;
    end if;

    -- otherwise, return the new row so it gets inserted as normal
    return NEW;
end;
$$ language plpgsql;

create trigger on_insert_player_paradigm_shift_trigger
    before insert on data.player_paradigm_shifts
    for each row
execute function data.on_insert_player_paradigm_shift();

-- i think this also just needed the new columns
drop trigger on_insert_player_recomposition_trigger on data.player_recompositions;
drop function data.on_insert_player_recomposition;
create function data.on_insert_player_recomposition()
    returns trigger as $$
begin
    perform 1
    from data.player_recompositions pr
    where pr.mmolb_player_id = NEW.mmolb_player_id
      -- note: "is not distinct from" is like "=" except for how it treats nulls.
      -- in postgres, NULL = NULL is false but NULL is not distinct from NULL is true
      and pr.feed_event_index is not distinct from NEW.feed_event_index
      and pr.inferred_event_index is not distinct from NEW.inferred_event_index
      and pr.time is not distinct from NEW.time
      and pr.season is not distinct from NEW.season
      and pr.day_type is not distinct from NEW.day_type
      and pr.day is not distinct from NEW.day
      and pr.player_name_before is not distinct from NEW.player_name_before
      and pr.player_name_after is not distinct from NEW.player_name_after
      and pr.reverts_recomposition is not distinct from NEW.reverts_recomposition;

    -- if there was an exact match, suppress this insert
    if FOUND then
        return null;
    end if;

    -- otherwise, return the new row so it gets inserted as normal
    return NEW;
end;
$$ language plpgsql;

create trigger on_insert_player_recomposition_trigger
    before insert on data.player_recompositions
    for each row
execute function data.on_insert_player_recomposition();

-- this needs to be renamed and `season` added because apparently i forgot to do that
drop trigger on_insert_player_report_trigger on data.player_report_attributes;
drop function data.on_insert_player_report;
create function data.on_insert_player_report_attribute()
    returns trigger as $$
begin
    perform 1
    from data.player_report_attributes pr
    where pr.mmolb_player_id = NEW.mmolb_player_id
      -- note: "is not distinct from" is like "=" except for how it treats nulls.
      -- in postgres, NULL = NULL is false but NULL is not distinct from NULL is true
      and pr.season is not distinct from NEW.season
      and pr.day_type is not distinct from NEW.day_type
      and pr.day is not distinct from NEW.day
      and pr.superstar_day is not distinct from NEW.superstar_day
      -- NOTE: `observed` should NOT be in the conditions. Each attempted insert
      -- will have a new `observed`, but we only want to keep the first one.

      -- attribute and stars are not necessary for correctness. i'm keeping them for
      -- now to help me catch bugs
      and pr.attribute is not distinct from NEW.attribute
      and pr.stars is not distinct from NEW.stars;

    -- if there was an exact match, suppress this insert
    if FOUND then
        return null;
    end if;

    -- otherwise, return the new row so it gets inserted as normal
    return NEW;
end;
$$ language plpgsql;

create trigger on_insert_player_report_attribute_trigger
    before insert on data.player_report_attributes
    for each row
execute function data.on_insert_player_report_attribute();
