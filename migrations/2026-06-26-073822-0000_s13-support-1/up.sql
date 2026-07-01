alter table data.events
    add column is_surprise_strike boolean; -- null = this event type can never be a surprise strike

alter table data.event_fielders
    -- I chose to name this "WAS double trouble" (in past tense) to emphasize that being double trouble is a thing that
    -- can happen to a player, rather than a thing that a player always is 
    add column was_double_trouble boolean, -- null = fielders for this event cannot be double trouble
    add column used_jetpack boolean; -- null = this event type can never be jetpack

alter table data.event_baserunners
    add column assassinated_by text; -- null = assassin name not known -- either not an assassination or a silent assassination

drop function data.strikes_after(ev data.events, et taxa.event_type);
create function data.strikes_after(ev data.events, et taxa.event_type) returns int as
$$
begin
    return case
        when et.name = 'FoulBall' and ev.strikes_before = 2 then ev.strikes_before
        else ev.strikes_before + et.is_strike::int + coalesce(ev.is_surprise_strike::int, 0)
    end;
end;
$$ LANGUAGE plpgsql;

create index event_cheers_event_id_index on data.event_cheers (event_id);
create index event_balk_reasons_event_id_index on data.event_balk_reasons (event_id);
