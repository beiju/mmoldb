-- credit to ifhbiff for the view
create materialized view data.player_versions_extended as
WITH
--Get all timestamps for a player
boundaries AS (
    SELECT mmolb_player_id, valid_from AS ts FROM data.player_equipment_versions
    UNION ALL
    SELECT mmolb_player_id, valid_until
    FROM data.player_equipment_versions
    UNION ALL
    SELECT mmolb_player_id, valid_from FROM data.player_equipment_effect_versions
    UNION ALL
    SELECT mmolb_player_id, valid_until
    FROM data.player_equipment_effect_versions
    UNION ALL
    SELECT mmolb_player_id, valid_from FROM data.player_versions
    UNION ALL
    SELECT mmolb_player_id, valid_until
    FROM data.player_versions
    UNION ALL
    SELECT mmolb_player_id, valid_from FROM data.player_report_attribute_versions
    UNION ALL
    SELECT mmolb_player_id, valid_until
    FROM data.player_report_attribute_versions
    UNION ALL
    SELECT mmolb_player_id, TIME - INTERVAL '1 microsecond' FROM DATA.player_attribute_augments
        ),
--Group up timestamps within a player
        ordered AS (
        SELECT mmolb_player_id, ts
        FROM boundaries
        WHERE ts IS NOT NULL
        GROUP BY mmolb_player_id, ts
        ),
--Use lead() to create time segments
        segments AS (
        SELECT mmolb_player_id,
        ts AS valid_from,
        LEAD(ts) OVER (PARTITION BY mmolb_player_id ORDER BY ts) AS valid_until
        FROM ordered
        )

--Start actual SELECT
SELECT DISTINCT
    row_number() over (ORDER BY s.mmolb_player_id, s.valid_from) AS id,
    s.mmolb_player_id,

    --Timespan
    s.valid_from,
    s.valid_until,

    --Player data
    b.first_name, b.last_name, b.mmolb_team_id,
    b.durability::DECIMAL(10,2), xsl.abbreviation AS position,
	xhb.name AS batting_handedness, xhp.name AS pitching_handedness,
	b.home, b.birthseason, COALESCE(b.birthday_day, b.birthday_superstar_day, null) AS birthday,
	xdt.display_name AS birthday_type,
	mg.id AS greater_boon_id, mg.name AS greater_boon,
	ml.id AS lesser_boon_id, ml.name AS lesser_boon,
	coalesce(pmod.modification_ids, '{}') as modification_ids,
	coalesce(pmod.modifications, '{}') as modifications,

	--Augment
	xaa.name AS attribute_augmented, c.value AS augmented_amount,

	--Equipment
	trim(coalesce(eva.rare_name || ' ' || eva.name, coalesce(eva.prefixes[1],'') || ' ' || eva.name || ' ' || coalesce(eva.suffixes[1],''))) AS accessory_equip_name,
	accessory_attributes, accessory_effect_types, accessory_values,
	/*
	accessory_attribute1, accessory_effect_type1, accessory_value1,
	accessory_attribute2, accessory_effect_type2, accessory_value2,
	accessory_attribute3, accessory_effect_type3, accessory_value3,
	accessory_attribute4, accessory_effect_type4, accessory_value4,
	*/

	trim(coalesce(evb.rare_name || ' ' || evb.name, coalesce(evb.prefixes[1],'') || ' ' || evb.name || ' ' || coalesce(evb.suffixes[1],''))) AS body_equip_name,
	body_attributes, body_effect_types, body_values,

	trim(coalesce(evf.rare_name || ' ' || evf.name, coalesce(evf.prefixes[1],'') || ' ' || evf.name || ' ' || coalesce(evf.suffixes[1],''))) AS feet_equip_name,
	feet_attributes, feet_effect_types, feet_values,

	trim(coalesce(evg.rare_name || ' ' || evg.name, coalesce(evg.prefixes[1],'') || ' ' || evg.name || ' ' || coalesce(evg.suffixes[1],''))) AS hands_equip_name,
	hands_attributes, hands_effect_types, hands_values,

	trim(coalesce(evh.rare_name || ' ' || evh.name, coalesce(evb.prefixes[1],'') || ' ' || evh.name || ' ' || coalesce(evh.suffixes[1],''))) AS head_equip_name,
	head_attributes, head_effect_types, head_values,

	--Attributes
	luck.stars AS luck_stars,

	aiming.stars as aiming_stars,
	contact.stars as contact_stars,
	cunning.stars as cunning_stars,
	determination.stars as determination_stars,
	discipline.stars as discipline_stars,
	insight.stars as insight_stars,
	intimidation.stars as intimidation_stars,
	lift.stars as lift_stars,
	muscle.stars as muscle_stars,
	selflessness.stars as selflessness_stars,
	vision.stars as vision_stars,
	wisdom.stars as wisdom_stars,

	accuracy.stars as accuracy_stars,
	control.stars as control_stars,
	defiance.stars as defiance_stars,
	guts.stars as guts_stars,
	presence.stars as presence_stars,
	persuasion.stars as persuasion_stars,
	rotation.stars as rotation_stars,
	stamina.stars as stamina_stars,
	stuff.stars as stuff_stars,
	velocity.stars as velocity_stars,

	acrobatics.stars as acrobatics_stars,
	agility.stars as agility_stars,
	arm.stars as arm_stars,
	awareness.stars as awareness_stars,
	composure.stars as composure_stars,
	dexterity.stars as dexterity_stars,
	patience.stars as patience_stars,
	reaction.stars as reaction_stars,

	greed.stars as greed_stars,
	performance.stars as performance_stars,
	speed.stars as speed_stars,
	stealth.stars as stealth_stars

FROM segments s

--SELECT player data
    JOIN data.player_versions b
ON b.mmolb_player_id = s.mmolb_player_id
    AND b.valid_from <= s.valid_from
    AND (b.valid_until IS NULL OR b.valid_until > s.valid_from)

    JOIN taxa.handedness xhb
    ON b.batting_handedness = xhb.id
    JOIN taxa.handedness xhp
    ON b.pitching_handedness = xhp.id

    --SELECT Attributes
--For some reason, the 35 separate left joins are always faster than a consolidated CTE
    LEFT JOIN
    (
    SELECT * from
    DATA.player_report_attribute_versions
    WHERE attribute = 2
    ) luck
    ON luck.mmolb_player_id = s.mmolb_player_id
    AND luck.valid_from <= s.valid_from
    AND (luck.valid_until IS NULL OR luck.valid_until > s.valid_from)

    LEFT JOIN
    (
    SELECT * from
    DATA.player_report_attribute_versions
    WHERE attribute = 3
    ) aiming
    ON aiming.mmolb_player_id = s.mmolb_player_id
    AND aiming.valid_from <= s.valid_from
    AND (aiming.valid_until IS NULL OR aiming.valid_until > s.valid_from)

    LEFT JOIN
    (
    SELECT * from
    DATA.player_report_attribute_versions
    WHERE attribute = 4
    ) contact
    ON contact.mmolb_player_id = s.mmolb_player_id
    AND contact.valid_from <= s.valid_from
    AND (contact.valid_until IS NULL OR contact.valid_until > s.valid_from)

    LEFT JOIN
    (
    SELECT * from
    DATA.player_report_attribute_versions
    WHERE attribute = 5
    ) cunning
    ON cunning.mmolb_player_id = s.mmolb_player_id
    AND cunning.valid_from <= s.valid_from
    AND (cunning.valid_until IS NULL OR cunning.valid_until > s.valid_from)

    LEFT JOIN
    (
    SELECT * from
    DATA.player_report_attribute_versions
    WHERE attribute = 6
    ) discipline
    ON discipline.mmolb_player_id = s.mmolb_player_id
    AND discipline.valid_from <= s.valid_from
    AND (discipline.valid_until IS NULL OR discipline.valid_until > s.valid_from)

    LEFT JOIN
    (
    SELECT * from
    DATA.player_report_attribute_versions
    WHERE attribute = 7
    ) insight
    ON insight.mmolb_player_id = s.mmolb_player_id
    AND insight.valid_from <= s.valid_from
    AND (insight.valid_until IS NULL OR insight.valid_until > s.valid_from)

    LEFT JOIN
    (
    SELECT * from
    DATA.player_report_attribute_versions
    WHERE attribute = 8
    ) intimidation
    ON intimidation.mmolb_player_id = s.mmolb_player_id
    AND intimidation.valid_from <= s.valid_from
    AND (intimidation.valid_until IS NULL OR intimidation.valid_until > s.valid_from)

    LEFT JOIN
    (
    SELECT * from
    DATA.player_report_attribute_versions
    WHERE attribute = 9
    ) lift
    ON lift.mmolb_player_id = s.mmolb_player_id
    AND lift.valid_from <= s.valid_from
    AND (lift.valid_until IS NULL OR lift.valid_until > s.valid_from)

    LEFT JOIN
    (
    SELECT * from
    DATA.player_report_attribute_versions
    WHERE attribute = 10
    ) vision
    ON vision.mmolb_player_id = s.mmolb_player_id
    AND vision.valid_from <= s.valid_from
    AND (vision.valid_until IS NULL OR vision.valid_until > s.valid_from)

    LEFT JOIN
    (
    SELECT * from
    DATA.player_report_attribute_versions
    WHERE attribute = 11
    ) determination
    ON determination.mmolb_player_id = s.mmolb_player_id
    AND determination.valid_from <= s.valid_from
    AND (determination.valid_until IS NULL OR determination.valid_until > s.valid_from)

    LEFT JOIN
    (
    SELECT * from
    DATA.player_report_attribute_versions
    WHERE attribute = 12
    ) wisdom
    ON wisdom.mmolb_player_id = s.mmolb_player_id
    AND wisdom.valid_from <= s.valid_from
    AND (wisdom.valid_until IS NULL OR wisdom.valid_until > s.valid_from)

    LEFT JOIN
    (
    SELECT * from
    DATA.player_report_attribute_versions
    WHERE attribute = 13
    ) muscle
    ON muscle.mmolb_player_id = s.mmolb_player_id
    AND muscle.valid_from <= s.valid_from
    AND (muscle.valid_until IS NULL OR muscle.valid_until > s.valid_from)

    LEFT JOIN
    (
    SELECT * from
    DATA.player_report_attribute_versions
    WHERE attribute = 14
    ) selflessness
    ON selflessness.mmolb_player_id = s.mmolb_player_id
    AND selflessness.valid_from <= s.valid_from
    AND (selflessness.valid_until IS NULL OR selflessness.valid_until > s.valid_from)

    LEFT JOIN
    (
    SELECT * from
    DATA.player_report_attribute_versions
    WHERE attribute = 15
    ) accuracy
    ON accuracy.mmolb_player_id = s.mmolb_player_id
    AND accuracy.valid_from <= s.valid_from
    AND (accuracy.valid_until IS NULL OR accuracy.valid_until > s.valid_from)

    LEFT JOIN
    (
    SELECT * from
    DATA.player_report_attribute_versions
    WHERE attribute = 16
    ) rotation
    ON rotation.mmolb_player_id = s.mmolb_player_id
    AND rotation.valid_from <= s.valid_from
    AND (rotation.valid_until IS NULL OR rotation.valid_until > s.valid_from)

    LEFT JOIN
    (
    SELECT * from
    DATA.player_report_attribute_versions
    WHERE attribute = 17
    ) presence
    ON presence.mmolb_player_id = s.mmolb_player_id
    AND presence.valid_from <= s.valid_from
    AND (presence.valid_until IS NULL OR presence.valid_until > s.valid_from)

    LEFT JOIN
    (
    SELECT * from
    DATA.player_report_attribute_versions
    WHERE attribute = 18
    ) persuasion
    ON persuasion.mmolb_player_id = s.mmolb_player_id
    AND persuasion.valid_from <= s.valid_from
    AND (persuasion.valid_until IS NULL OR persuasion.valid_until > s.valid_from)

    LEFT JOIN
    (
    SELECT * from
    DATA.player_report_attribute_versions
    WHERE attribute = 19
    ) stamina
    ON stamina.mmolb_player_id = s.mmolb_player_id
    AND stamina.valid_from <= s.valid_from
    AND (stamina.valid_until IS NULL OR stamina.valid_until > s.valid_from)

    LEFT JOIN
    (
    SELECT * from
    DATA.player_report_attribute_versions
    WHERE attribute = 20
    ) velocity
    ON velocity.mmolb_player_id = s.mmolb_player_id
    AND velocity.valid_from <= s.valid_from
    AND (velocity.valid_until IS NULL OR velocity.valid_until > s.valid_from)

    LEFT JOIN
    (
    SELECT * from
    DATA.player_report_attribute_versions
    WHERE attribute = 21
    ) control
    ON control.mmolb_player_id = s.mmolb_player_id
    AND control.valid_from <= s.valid_from
    AND (control.valid_until IS NULL OR control.valid_until > s.valid_from)

    LEFT JOIN
    (
    SELECT * from
    DATA.player_report_attribute_versions
    WHERE attribute = 22
    ) stuff
    ON stuff.mmolb_player_id = s.mmolb_player_id
    AND stuff.valid_from <= s.valid_from
    AND (stuff.valid_until IS NULL OR stuff.valid_until > s.valid_from)

    LEFT JOIN
    (
    SELECT * from
    DATA.player_report_attribute_versions
    WHERE attribute = 23
    ) defiance
    ON defiance.mmolb_player_id = s.mmolb_player_id
    AND defiance.valid_from <= s.valid_from
    AND (defiance.valid_until IS NULL OR defiance.valid_until > s.valid_from)

    LEFT JOIN
    (
    SELECT * from
    DATA.player_report_attribute_versions
    WHERE attribute = 24
    ) acrobatics
    ON acrobatics.mmolb_player_id = s.mmolb_player_id
    AND acrobatics.valid_from <= s.valid_from
    AND (acrobatics.valid_until IS NULL OR acrobatics.valid_until > s.valid_from)

    LEFT JOIN
    (
    SELECT * from
    DATA.player_report_attribute_versions
    WHERE attribute = 25
    ) agility
    ON agility.mmolb_player_id = s.mmolb_player_id
    AND agility.valid_from <= s.valid_from
    AND (agility.valid_until IS NULL OR agility.valid_until > s.valid_from)

    LEFT JOIN
    (
    SELECT * from
    DATA.player_report_attribute_versions
    WHERE attribute = 26
    ) arm
    ON arm.mmolb_player_id = s.mmolb_player_id
    AND arm.valid_from <= s.valid_from
    AND (arm.valid_until IS NULL OR arm.valid_until > s.valid_from)

    LEFT JOIN
    (
    SELECT * from
    DATA.player_report_attribute_versions
    WHERE attribute = 27
    ) awareness
    ON awareness.mmolb_player_id = s.mmolb_player_id
    AND awareness.valid_from <= s.valid_from
    AND (awareness.valid_until IS NULL OR awareness.valid_until > s.valid_from)

    LEFT JOIN
    (
    SELECT * from
    DATA.player_report_attribute_versions
    WHERE attribute = 28
    ) composure
    ON composure.mmolb_player_id = s.mmolb_player_id
    AND composure.valid_from <= s.valid_from
    AND (composure.valid_until IS NULL OR composure.valid_until > s.valid_from)

    LEFT JOIN
    (
    SELECT * from
    DATA.player_report_attribute_versions
    WHERE attribute = 29
    ) dexterity
    ON dexterity.mmolb_player_id = s.mmolb_player_id
    AND dexterity.valid_from <= s.valid_from
    AND (dexterity.valid_until IS NULL OR dexterity.valid_until > s.valid_from)

    LEFT JOIN
    (
    SELECT * from
    DATA.player_report_attribute_versions
    WHERE attribute = 30
    ) patience
    ON patience.mmolb_player_id = s.mmolb_player_id
    AND patience.valid_from <= s.valid_from
    AND (patience.valid_until IS NULL OR patience.valid_until > s.valid_from)

    LEFT JOIN
    (
    SELECT * from
    DATA.player_report_attribute_versions
    WHERE attribute = 31
    ) reaction
    ON reaction.mmolb_player_id = s.mmolb_player_id
    AND reaction.valid_from <= s.valid_from
    AND (reaction.valid_until IS NULL OR reaction.valid_until > s.valid_from)

    LEFT JOIN
    (
    SELECT * from
    DATA.player_report_attribute_versions
    WHERE attribute = 32
    ) greed
    ON greed.mmolb_player_id = s.mmolb_player_id
    AND greed.valid_from <= s.valid_from
    AND (greed.valid_until IS NULL OR greed.valid_until > s.valid_from)

    LEFT JOIN
    (
    SELECT * from
    DATA.player_report_attribute_versions
    WHERE attribute = 33
    ) performance
    ON performance.mmolb_player_id = s.mmolb_player_id
    AND performance.valid_from <= s.valid_from
    AND (performance.valid_until IS NULL OR performance.valid_until > s.valid_from)

    LEFT JOIN
    (
    SELECT * from
    DATA.player_report_attribute_versions
    WHERE attribute = 34
    ) speed
    ON speed.mmolb_player_id = s.mmolb_player_id
    AND speed.valid_from <= s.valid_from
    AND (speed.valid_until IS NULL OR speed.valid_until > s.valid_from)

    LEFT JOIN
    (
    SELECT * from
    DATA.player_report_attribute_versions
    WHERE attribute = 35
    ) stealth
    ON stealth.mmolb_player_id = s.mmolb_player_id
    AND stealth.valid_from <= s.valid_from
    AND (stealth.valid_until IS NULL OR stealth.valid_until > s.valid_from)

    LEFT JOIN
    (
    SELECT * from
    DATA.player_report_attribute_versions
    WHERE attribute = 36
    ) guts
    ON guts.mmolb_player_id = s.mmolb_player_id
    AND guts.valid_from <= s.valid_from
    AND (guts.valid_until IS NULL OR guts.valid_until > s.valid_from)

    --SELECT Equipment
--Similar to above, multiple left joins run much faster
    LEFT JOIN
    (
    SELECT ev.*,
    array_remove(array_agg(eev.attribute ORDER BY effect_index), null) AS accessory_attributes,
    array_remove(array_agg(eev.effect_type ORDER BY effect_index), null) AS accessory_effect_types,
    array_remove(array_agg(eev.value ORDER BY effect_index), null) AS accessory_values
    FROM DATA.player_equipment_versions ev

    LEFT JOIN DATA.player_equipment_effect_versions eev
    ON ev.mmolb_player_id = eev.mmolb_player_id
    AND ev.valid_from = eev.valid_from
    AND ev.equipment_slot = eev.equipment_slot

    WHERE ev.equipment_slot = 'Accessory'
    GROUP BY ev.id
    ) eva
    ON eva.mmolb_player_id = s.mmolb_player_id
    AND eva.valid_from <= s.valid_from
    AND (eva.valid_until IS NULL OR eva.valid_until > s.valid_from)

    LEFT JOIN
    (
    SELECT ev.*,
    array_remove(array_agg(eev.attribute ORDER BY effect_index), null) AS body_attributes,
    array_remove(array_agg(eev.effect_type ORDER BY effect_index), null) AS body_effect_types,
    array_remove(array_agg(eev.value ORDER BY effect_index), null) AS body_values
    FROM DATA.player_equipment_versions ev

    LEFT JOIN DATA.player_equipment_effect_versions eev
    ON ev.mmolb_player_id = eev.mmolb_player_id
    AND ev.valid_from = eev.valid_from
    AND ev.equipment_slot = eev.equipment_slot

    WHERE ev.equipment_slot = 'Body'
    GROUP BY ev.id
    ) evb
    ON evb.mmolb_player_id = s.mmolb_player_id
    AND evb.valid_from <= s.valid_from
    AND (evb.valid_until IS NULL OR evb.valid_until > s.valid_from)

    LEFT JOIN
    (
    SELECT ev.*,
    array_remove(array_agg(eev.attribute ORDER BY effect_index), null) AS feet_attributes,
    array_remove(array_agg(eev.effect_type ORDER BY effect_index), null) AS feet_effect_types,
    array_remove(array_agg(eev.value ORDER BY effect_index), null) AS feet_values
    FROM DATA.player_equipment_versions ev

    LEFT JOIN DATA.player_equipment_effect_versions eev
    ON ev.mmolb_player_id = eev.mmolb_player_id
    AND ev.valid_from = eev.valid_from
    AND ev.equipment_slot = eev.equipment_slot

    WHERE ev.equipment_slot = 'Feet'
    GROUP BY ev.id
    ) evf
    ON evf.mmolb_player_id = s.mmolb_player_id
    AND evf.valid_from <= s.valid_from
    AND (evf.valid_until IS NULL OR evf.valid_until > s.valid_from)

    LEFT JOIN
    (
    SELECT ev.*,
    array_remove(array_agg(eev.attribute ORDER BY effect_index), null) AS hands_attributes,
    array_remove(array_agg(eev.effect_type ORDER BY effect_index), null) AS hands_effect_types,
    array_remove(array_agg(eev.value ORDER BY effect_index), null) AS hands_values
    FROM DATA.player_equipment_versions ev

    LEFT JOIN DATA.player_equipment_effect_versions eev
    ON ev.mmolb_player_id = eev.mmolb_player_id
    AND ev.valid_from = eev.valid_from
    AND ev.equipment_slot = eev.equipment_slot

    WHERE ev.equipment_slot = 'Hands'
    GROUP BY ev.id
    ) evg
    ON evg.mmolb_player_id = s.mmolb_player_id
    AND evg.valid_from <= s.valid_from
    AND (evg.valid_until IS NULL OR evg.valid_until > s.valid_from)

    LEFT JOIN
    (
    SELECT ev.*,
    array_remove(array_agg(eev.attribute ORDER BY effect_index), null) AS head_attributes,
    array_remove(array_agg(eev.effect_type ORDER BY effect_index), null) AS head_effect_types,
    array_remove(array_agg(eev.value ORDER BY effect_index), null) AS head_values
    FROM DATA.player_equipment_versions ev

    LEFT JOIN DATA.player_equipment_effect_versions eev
    ON ev.mmolb_player_id = eev.mmolb_player_id
    AND ev.valid_from = eev.valid_from
    AND ev.equipment_slot = eev.equipment_slot

    WHERE ev.equipment_slot = 'Head'
    GROUP BY ev.id
    ) evh
    ON evh.mmolb_player_id = s.mmolb_player_id
    AND evh.valid_from <= s.valid_from
    AND (evh.valid_until IS NULL OR evh.valid_until > s.valid_from)

--SELECT Augments
    LEFT JOIN DATA.player_attribute_augments c
    ON c.mmolb_player_id = s.mmolb_player_id
    AND c.time BETWEEN s.valid_from AND s.valid_until

    --SELECT Modifications
--Because the from/untils overlap, need to do the entire segments separately
    LEFT JOIN
    (
    WITH
    --Get all timestamps for a player
    boundaries AS (
    SELECT mmolb_player_id, valid_from AS ts FROM data.player_modification_versions
    UNION ALL
    SELECT mmolb_player_id, coalesce(valid_until, timezone('utc', NOW())) FROM data.player_modification_versions
    ),
    --Group up timestamps within a player
    ordered AS (
    SELECT mmolb_player_id, ts
    FROM boundaries
    WHERE ts IS NOT NULL
    GROUP BY mmolb_player_id, ts
    ),
    --Use lead() to create time segments
    segments AS (
    SELECT mmolb_player_id,
    ts AS valid_from,
    LEAD(ts) OVER (PARTITION BY mmolb_player_id ORDER BY ts) AS valid_until
    FROM ordered
    )
    SELECT distinct s.mmolb_player_id, s.valid_from,
    case
    when s.valid_until = NOW() THEN NULL ELSE s.valid_until
    END AS valid_until,
    array_agg(mo.id ORDER BY modification_index) AS modification_ids,
    array_agg(mo.name ORDER BY modification_index) AS modifications
    FROM segments s
    JOIN DATA.player_modification_versions pm
    ON s.mmolb_player_id = pm.mmolb_player_id
    AND pm.valid_from <= s.valid_from
    AND (pm.valid_until IS NULL OR pm.valid_until > s.valid_from)
    JOIN DATA.modifications mo
    ON pm.modification_id = mo.id

    WHERE s.valid_from < NOW()
    AND s.mmolb_player_id = '686dcf38775d6c67e270a944'

    GROUP BY s.mmolb_player_id, s.valid_from,
    case
    when s.valid_until = NOW() THEN NULL ELSE s.valid_until
    END
    ) pmod
    ON pmod.mmolb_player_id = s.mmolb_player_id
    AND pmod.valid_from <= s.valid_from
    AND (pmod.valid_until IS NULL OR pmod.valid_until > s.valid_from)

--Misc taxas
    LEFT JOIN taxa.attribute xaa
    ON c.attribute = xaa.id

    LEFT JOIN taxa.day_type xdt
    ON b.birthday_type = xdt.id

    LEFT JOIN DATA.modifications ml
    ON b.lesser_boon = ml.id

    LEFT JOIN DATA.modifications mg
    ON b.greater_boon = mg.id

    LEFT JOIN taxa.slot xsl
    ON b.slot = xsl.id

--This is needed to prevent creating an extra row where valid_from and valid_until are both NULL
WHERE s.valid_from IS NOT NULL

--Current records only
--AND s.valid_until IS NULL
--AND b.mmolb_team_id IS NOT NULL

--Testing purposes bring in specific player
--AND b.mmolb_player_id = '6843188688056169e0078f07'

ORDER BY s.mmolb_player_id, valid_from, valid_until;