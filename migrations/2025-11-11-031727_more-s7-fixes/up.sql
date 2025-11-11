alter table data.door_prize_items
    add column prize_discarded boolean; -- null -> this item went to inventory and thus didn't have the opportunity to be discarded