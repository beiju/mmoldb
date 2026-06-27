alter table data.events
    add column is_surprise_strike boolean; -- null = this event type can never be a surprise strike

alter table data.event_fielders
    -- I chose to name this "WAS double trouble" (in past tense) to emphasize that being double trouble is a thing that
    -- can happen to a player, rather than a thing that a player always is 
    add column was_double_trouble boolean; -- null = fielders for this event cannot be double trouble