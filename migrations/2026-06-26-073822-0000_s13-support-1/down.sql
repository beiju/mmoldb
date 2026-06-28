drop function data.strikes_after(ev data.events, et taxa.event_type);
create function data.strikes_after(ev data.events, et taxa.event_type) returns int as
$$
begin
    return case when et.name = 'FoulBall' and ev.strikes_before = 2 then ev.strikes_before else ev.strikes_before + et.is_strike::int end;
end;
$$ LANGUAGE plpgsql;

alter table data.event_fielders
    drop column was_double_trouble;

alter table data.events
    drop column is_surprise_strike;