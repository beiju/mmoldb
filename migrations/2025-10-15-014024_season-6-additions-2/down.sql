drop table data.wither;
alter table data.player_report_attribute_versions
    alter column base_stars set not null,
    drop column modified_stars,
    drop column modified_total;
alter table data.player_report_attribute_versions rename column base_total to total;
alter table data.player_report_attribute_versions rename column base_stars to stars;
