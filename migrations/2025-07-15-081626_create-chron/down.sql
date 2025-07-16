-- This file should undo anything in `up.sql`
drop table data.versions;
drop function data.on_insert_version;
drop table data.entities;
drop function data.on_insert_entity;
