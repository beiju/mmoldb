alter table data.door_prize_items
    add column prefix text,
    add column suffix text,
    add column discarded_item_prefix text,
    add column discarded_item_suffix text;

-- noinspection SqlWithoutWhere
update data.door_prize_items
set
    prefix = prefixes[1],
    suffix = suffixes[1],
    discarded_item_prefix = discarded_item_prefixes[1],
    discarded_item_suffix = discarded_item_suffixes[1];

alter table data.door_prize_items
    drop column prefixes,
    drop column suffixes,
    drop column discarded_item_prefixes,
    drop column discarded_item_suffixes;

alter table data.consumption_contests
    add column batting_team_prize_prefix text,
    add column batting_team_prize_suffix text,
    add column defending_team_prize_prefix text,
    add column defending_team_prize_suffix text;

-- noinspection SqlWithoutWhere
update data.consumption_contests
set
    batting_team_prize_prefix = batting_team_prize_prefixes[1],
    batting_team_prize_suffix = batting_team_prize_suffixes[1],
    defending_team_prize_prefix = defending_team_prize_prefixes[1],
    defending_team_prize_suffix = defending_team_prize_suffixes[1];

alter table data.consumption_contests
    drop column batting_team_prize_prefixes,
    drop column batting_team_prize_suffixes,
    drop column defending_team_prize_prefixes,
    drop column defending_team_prize_suffixes;
