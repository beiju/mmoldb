alter table data.player_report_attribute_versions
    add column total double precision; -- null == not provided (this was added to the API late)