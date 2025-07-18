-- This file should undo anything in `up.sql`

alter table data.games drop column stadium_name;
alter table data.events drop cheer;
