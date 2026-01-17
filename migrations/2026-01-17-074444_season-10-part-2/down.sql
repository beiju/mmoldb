alter table data.player_versions
    drop column priority;

alter table data.player_report_versions
    alter column quote set not null;