alter table data.player_report_versions
    alter column quote drop not null;

-- TODO:
--   - Priority, within BaseAttributes
--   - Pitch types and their probabilities, within BaseAttributes
--   - On teams: how the lineup is currently ordered. This is public on the site so must be in the API somewhere