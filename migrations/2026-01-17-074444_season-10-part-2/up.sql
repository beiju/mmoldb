alter table data.player_report_versions
    alter column quote drop not null;

alter table data.player_versions
    add column priority float8; -- null = this player version is from before priority was available

-- TODO:
--   - Priority, within BaseAttributes
--   - Pitch types and their probabilities, within BaseAttributes
--   - On teams: how the lineup is currently ordered. This is public on the site so must be in the API somewhere