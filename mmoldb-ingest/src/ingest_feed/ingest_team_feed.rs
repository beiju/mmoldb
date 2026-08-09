use crate::ingest::VersionIngestLogs;
use chron::ChronFeedEvent;
use itertools::Itertools;
use mmolb_parsing::enums::LinkType;
use mmolb_parsing::team_feed::ParsedTeamFeedEventText;
use mmoldb_db::models::NewTeamGamePlayed;

pub fn chron_team_feed_as_new<'a>(
    feed_event: &'a ChronFeedEvent<mmolb_parsing::feed_event::FeedEvent>,
    ingest_logs: &mut VersionIngestLogs<'a>,
) -> Option<NewTeamGamePlayed<'a>> {

    // There is a bug in mmolb_parsing that causes a panic when an
    // augment's text is empty
    if feed_event.data.text.is_empty() {
        return None;
    }

    let parsed_event = mmolb_parsing::team_feed::parse_team_feed_event(&feed_event.data);

    let is_game_result = if let ParsedTeamFeedEventText::GameResult { .. } = &parsed_event {
        true
    } else {
        false
    };

    let game_outcome = match parsed_event {
        ParsedTeamFeedEventText::ParseError { error, text } => {
            // I'm making this a warning because we don't care about most event types
            // (and we can handle having a game for which we don't know the end time)
            ingest_logs.warn(format!("Error parsing \"{text}\": {error}"));
            None
        }
        // Get game
        ParsedTeamFeedEventText::GameResult { .. }
        | ParsedTeamFeedEventText::Shipment { .. }
        | ParsedTeamFeedEventText::PhotoContest { .. }
        | ParsedTeamFeedEventText::SpecialDelivery { .. }
        | ParsedTeamFeedEventText::PlayerReflected { .. }
        | ParsedTeamFeedEventText::SimulacrumPayout { .. }
        // EndGameIncome doesn't have a link in the rendered text, but it does have one
        // in the metadata
        | ParsedTeamFeedEventText::EndGameIncome { .. } => {
            let game_link = feed_event.data
                .links
                .iter()
                .filter(|link| link.link_type == Ok(LinkType::Game))
                .exactly_one();

            match game_link {
                Ok(game_link) => Some(NewTeamGamePlayed {
                    mmolb_team_id: &feed_event.subject_id,
                    feed_event_id: &feed_event.event_id,
                    time: feed_event.timestamp.naive_utc(),
                    mmolb_game_id: &game_link.id,
                }),
                Err(err) => {
                    let msg = format!(
                        "Game outcome in {} feed event {} had {} game links (expected 1)",
                        feed_event.subject_id,
                        feed_event.event_id,
                        err.count()
                    );
                    if is_game_result {
                        ingest_logs.warn(msg);
                    } else {
                        ingest_logs.info(msg);
                    }
                    None
                }
            }
        }
        // Delivery is an end-of-game event but didn't have game links
        ParsedTeamFeedEventText::Delivery { .. }
        | ParsedTeamFeedEventText::ClaimedLinealBelt { .. }
        | ParsedTeamFeedEventText::LostLinealBelt { .. }
        | ParsedTeamFeedEventText::Party { .. }
        | ParsedTeamFeedEventText::DoorPrize { .. }
        | ParsedTeamFeedEventText::Prosperous { .. }
        | ParsedTeamFeedEventText::DonatedToLottery { .. }
        | ParsedTeamFeedEventText::WonLottery { .. }
        | ParsedTeamFeedEventText::Enchantment { .. }
        | ParsedTeamFeedEventText::AttributeChanges { .. }
        | ParsedTeamFeedEventText::MassAttributeEquals { .. }
        | ParsedTeamFeedEventText::TakeTheMound { .. }
        | ParsedTeamFeedEventText::TakeThePlate { .. }
        | ParsedTeamFeedEventText::SwapPlaces { .. }
        | ParsedTeamFeedEventText::Recomposed { .. }
        | ParsedTeamFeedEventText::Modification { .. }
        | ParsedTeamFeedEventText::FallingStarOutcome { .. }
        | ParsedTeamFeedEventText::CorruptedByWither { .. }
        | ParsedTeamFeedEventText::Purified { .. }
        | ParsedTeamFeedEventText::NameChanged
        | ParsedTeamFeedEventText::PlayerMoved { .. }
        | ParsedTeamFeedEventText::PlayerRelegated { .. }
        | ParsedTeamFeedEventText::PlayerPositionsSwapped { .. }
        | ParsedTeamFeedEventText::PlayerContained { .. }
        | ParsedTeamFeedEventText::PlayerGrow { .. }
        | ParsedTeamFeedEventText::Callup { .. }
        | ParsedTeamFeedEventText::GreaterAugment { .. }
        | ParsedTeamFeedEventText::Released { .. }
        | ParsedTeamFeedEventText::OldRetirement { .. }
        | ParsedTeamFeedEventText::PlayerGrewInEfflorescence { .. }
        | ParsedTeamFeedEventText::PlayerEffloresce { .. }
        | ParsedTeamFeedEventText::DeliveryDiscarded { .. }
        | ParsedTeamFeedEventText::ConsumptionContestToPlayer { .. }
        | ParsedTeamFeedEventText::ConsumptionContestToTeam { .. }
        | ParsedTeamFeedEventText::PlayersSwapped { .. }
        | ParsedTeamFeedEventText::PlayersPurified { .. }
        | ParsedTeamFeedEventText::ElectionAppliedLevelUps { .. }
        | ParsedTeamFeedEventText::Restyle { .. }
        | ParsedTeamFeedEventText::Augment { .. }
        | ParsedTeamFeedEventText::BulkImmunized { .. }
        | ParsedTeamFeedEventText::GildedUmpiresPayout { .. }
        | ParsedTeamFeedEventText::GoldenPlayerReplacementFailed { .. }
        | ParsedTeamFeedEventText::ResumedHolidayProcessingReplacement { .. }
        | ParsedTeamFeedEventText::GoldenPlayerEmerged { .. }
        | ParsedTeamFeedEventText::GainedModificationFromGreaterAugment { .. }
        | ParsedTeamFeedEventText::PlayersBecameFriends { .. }
        | ParsedTeamFeedEventText::PlayerTrained { .. }
        | ParsedTeamFeedEventText::ManagerReplaced { .. }
        | ParsedTeamFeedEventText::NewRetirement { .. } => None,
    };

    game_outcome
}
