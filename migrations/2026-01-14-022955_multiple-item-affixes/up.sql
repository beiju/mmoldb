alter table data.door_prize_items
    add column prefixes text[] not null default '{}'::text[],
    add column suffixes text[] not null default '{}'::text[],
    add column discarded_item_prefixes text[] not null default '{}'::text[],
    add column discarded_item_suffixes text[] not null default '{}'::text[];

-- noinspection SqlWithoutWhere
update data.door_prize_items
set
    prefixes = case when prefix is not null then array[prefix] else '{}'::text[] end,
    suffixes = case when suffix is not null then array[suffix] else '{}'::text[] end,
    discarded_item_prefixes = case when discarded_item_prefix is not null then array[discarded_item_prefix] else '{}'::text[] end,
    discarded_item_suffixes = case when discarded_item_suffix is not null then array[discarded_item_suffix] else '{}'::text[] end;

alter table data.door_prize_items
    drop column prefix,
    drop column suffix,
    drop column discarded_item_prefix,
    drop column discarded_item_suffix;


alter table data.consumption_contests
    add column batting_team_prize_prefixes text[] not null default '{}'::text[],
    add column batting_team_prize_suffixes text[] not null default '{}'::text[],
    add column defending_team_prize_prefixes text[] not null default '{}'::text[],
    add column defending_team_prize_suffixes text[] not null default '{}'::text[];

-- noinspection SqlWithoutWhere
update data.consumption_contests
set
    batting_team_prize_prefixes = case when batting_team_prize_prefix is not null then array[batting_team_prize_prefix] else '{}'::text[] end,
    batting_team_prize_suffixes = case when batting_team_prize_suffix is not null then array[batting_team_prize_suffix] else '{}'::text[] end,
    defending_team_prize_prefixes = case when defending_team_prize_prefix is not null then array[defending_team_prize_prefix] else '{}'::text[] end,
    defending_team_prize_suffixes = case when defending_team_prize_suffix is not null then array[defending_team_prize_suffix] else '{}'::text[] end;

alter table data.consumption_contests
    drop column batting_team_prize_prefix,
    drop column batting_team_prize_suffix,
    drop column defending_team_prize_prefix,
    drop column defending_team_prize_suffix;