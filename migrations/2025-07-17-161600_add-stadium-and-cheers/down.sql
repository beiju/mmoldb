-- This file should undo anything in `up.sql`

alter table data.games drop column stadium;
alter table data.events drop cheer;
