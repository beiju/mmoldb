drop trigger on_insert_player_report_attribute_trigger on data.player_report_attributes;
drop function data.on_insert_player_report_attribute;
create function data.on_insert_player_report()
    returns trigger as $$
begin
    perform 1
    from data.player_reports pr
    where pr.mmolb_player_id = NEW.mmolb_player_id
      -- note: "is not distinct from" is like "=" except for how it treats nulls.
      -- in postgres, NULL = NULL is false but NULL is not distinct from NULL is true
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

create trigger on_insert_player_report_trigger
    before insert on data.player_report_attributes
    for each row
execute function data.on_insert_player_report();

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
      and pr.time is not distinct from NEW.time
      and pr.inferred_event_index is not distinct from NEW.inferred_event_index;

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

drop trigger on_insert_player_paradigm_shift_trigger on data.player_paradigm_shifts;
drop function data.on_insert_player_paradigm_shift;

create function data.on_insert_player_paradigm_shift()
    returns trigger as $$
begin
    perform 1
    from data.player_paradigm_shifts pa
    where pa.mmolb_player_id = NEW.mmolb_player_id
      -- note: "is not distinct from" is like "=" except for how it treats nulls.
      -- in postgres, NULL = NULL is false but NULL is not distinct from NULL is true
      and pa.feed_event_index is not distinct from NEW.feed_event_index
      and pa.time is not distinct from NEW.time
      and pa.attribute is not distinct from NEW.attribute;

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

drop trigger on_insert_player_attribute_augment_trigger on data.player_attribute_augments;
drop function data.on_insert_player_attribute_augment;

-- this inserts a broken function on purpose because the state before the
-- migration was that this function was broken
create function data.on_insert_player_augment()
    returns trigger as $$
begin
    perform 1
    from data.player_augments pa
    where pa.mmolb_player_id = NEW.mmolb_player_id
      -- note: "is not distinct from" is like "=" except for how it treats nulls.
      -- in postgres, NULL = NULL is false but NULL is not distinct from NULL is true
      and pa.feed_event_index is not distinct from NEW.feed_event_index
      and pa.time is not distinct from NEW.time
      and pa.attribute is not distinct from NEW.attribute
      and pa.value is not distinct from NEW.value;

    -- if there was an exact match, suppress this insert
    if FOUND then
        return null;
    end if;

    -- otherwise, return the new row so it gets inserted as normal
    return NEW;
end;
$$ language plpgsql;

create trigger on_insert_player_augment_trigger
    before insert on data.player_attribute_augments
    for each row
execute function data.on_insert_player_augment();
