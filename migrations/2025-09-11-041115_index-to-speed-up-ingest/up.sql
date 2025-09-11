create index if not exists close_versions_index on data.versions (kind, entity_id) where valid_to is null;
