truncate table data.player_recompositions;
alter table data.player_recompositions
    drop constraint player_recompositions_mmolb_player_id_feed_event_id_key,
    drop column feed_event_id,
    add column feed_event_index integer not null,
    add constraint unique_player_id_feed_event_index_inferred_event_index unique (mmolb_player_id, feed_event_index);

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

truncate table data.player_paradigm_shifts;
alter table data.player_paradigm_shifts
    drop constraint player_paradigm_shifts_mmolb_player_id_feed_event_id_key,
    drop column player_name,
    drop column feed_event_id,
    add column feed_event_index integer not null,
    add constraint player_paradigm_shifts_mmolb_player_id_feed_event_index_key unique (mmolb_player_id, feed_event_index);

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

truncate table data.player_attribute_augments;
alter table data.player_attribute_augments
    drop constraint player_attribute_augments_mmolb_player_id_feed_event_id_key,
    drop column player_name,
    drop column feed_event_id,
    add column feed_event_index integer not null,
    add constraint player_augments_mmolb_player_id_feed_event_index_key unique (mmolb_player_id, feed_event_index);

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

truncate table data.team_games_played;
alter table data.team_games_played
    drop constraint team_games_played_mmolb_team_id_feed_event_id_key,
    drop column feed_event_id,
    add column feed_event_index integer not null,
    add constraint team_games_played_mmolb_team_id_feed_event_index_key unique (mmolb_team_id, feed_event_index);

create function data.on_insert_team_games_played()
    returns trigger as $$
begin
    perform 1
    from data.team_games_played tgp
    where tgp.mmolb_team_id = NEW.mmolb_team_id
      -- note: "is not distinct from" is like "=" except for how it treats nulls.
      -- in postgres, NULL = NULL is false but NULL is not distinct from NULL is true
      and tgp.feed_event_index is not distinct from NEW.feed_event_index
      and tgp.time is not distinct from NEW.time
      and tgp.mmolb_game_id is not distinct from NEW.mmolb_game_id;

    -- if there was an exact match, suppress this insert
    if FOUND then
        return null;
    end if;

    -- otherwise, return the new row so it gets inserted as normal
    return NEW;
end;
$$ language plpgsql;

create trigger on_insert_team_games_played_trigger
    before insert on data.team_games_played
    for each row
execute function data.on_insert_team_games_played();

truncate table data.feed_events_processed;