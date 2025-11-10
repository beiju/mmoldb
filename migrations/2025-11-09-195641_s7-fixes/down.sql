alter table data.parties
    drop column durability_loss;

alter table data.wither
    drop column source_player_name,
    drop column contain_attempted,
    drop column contain_replacement_player_name;
