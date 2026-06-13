drop function data.modified_attribute_total;
create function data.modified_attribute_total(
    player_id text,
    attribute_id bigint,
    at_time timestamp without time zone
) returns float8
as $$
select
    greatest(
            0.0,
            (
                prav.base_total
                    + coalesce((select sum(peev.value)
                                from data.player_equipment_effect_versions peev
                                where peev.mmolb_player_id = player_id
                                  and peev.attribute = attribute_id
                                  and peev.valid_from <= at_time
                                  and at_time < coalesce(peev.valid_until, 'infinity')
                                  and peev.effect_type = any (select id from taxa.attribute_effect_type where name = 'Flat')
                                  and peev.zone is null
                                  and peev.phase is null),
                               0.0)
                    + coalesce((select sum(me.value)
                                from data.player_modification_versions pmv
                                         left join data.modifications m on m.id = pmv.modification_id
                                         left join data.modification_effects me on me.modification_name = m.name
                                where pmv.mmolb_player_id = player_id
                                  and me.attribute = attribute_id
                                  and pmv.valid_from <= at_time
                                  and at_time < coalesce(pmv.valid_until, 'infinity')
                                  and me.valid_from <= at_time
                                  and at_time < coalesce(me.valid_until, 'infinity')
                                  and me.effect_type = any (select id from taxa.attribute_effect_type where name = 'Flat')),
                               0.0)
                ) *
            (
                1.0
                    + coalesce((select sum(peev.value)
                                from data.player_equipment_effect_versions peev
                                where peev.mmolb_player_id = player_id
                                  and peev.attribute = attribute_id
                                  and peev.valid_from <= at_time
                                  and at_time < coalesce(peev.valid_until, 'infinity')
                                  and peev.effect_type = any(select id from taxa.attribute_effect_type where name = 'Multiplier')
                                  and peev.zone is null
                                  and peev.phase is null), 0.0)
                    + coalesce((select sum(me.value)
                                from data.player_modification_versions pmv
                                         left join data.modifications m on m.id = pmv.modification_id
                                         left join data.modification_effects me on me.modification_name = m.name
                                where pmv.mmolb_player_id = player_id
                                  and me.attribute = attribute_id
                                  and pmv.valid_from <= at_time
                                  and at_time < coalesce(pmv.valid_until, 'infinity')
                                  and me.valid_from <= at_time
                                  and at_time < coalesce(me.valid_until, 'infinity')
                                  and me.effect_type = any
                                      (select id from taxa.attribute_effect_type where name = 'Multiplier')), 0.0)
                )
    )
from data.player_versions pv
         left join data.player_report_attribute_versions prav
                   on prav.mmolb_player_id=pv.mmolb_player_id
where pv.mmolb_player_id=player_id
  and pv.valid_from <= at_time and at_time < coalesce(pv.valid_until, 'infinity')
  and prav.attribute=attribute_id
  and prav.valid_from <= at_time and at_time < coalesce(prav.valid_until, 'infinity')
    $$ language SQL;