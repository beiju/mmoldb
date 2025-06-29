use paste::paste;

use diesel::QueryResult;
use enum_map::EnumMap;
use diesel::{PgConnection, RunQueryDsl};
use std::collections::HashSet;
use super::taxa_macro::*;
use rocket_sync_db_pools::diesel::prelude::*;

taxa! {
    #[
        schema = crate::taxa_schema::taxa::event_type,
        table = crate::taxa_schema::taxa::event_type::dsl::event_type,
        id_column = crate::taxa_schema::taxa::event_type::dsl::id,
    ]
    pub enum TaxaEventType {
        #[display_name: str = "ball"]
        Ball = 0,
        #[display_name: str = "called strike"]
        CalledStrike = 1,
        #[display_name: str = "swinging strike"]
        SwingingStrike = 2,
        #[display_name: str = "foul tip"]
        FoulTip = 3,
        #[display_name: str = "foul ball"]
        FoulBall = 4,
        #[display_name: str = "hit"]
        Hit = 5,
        #[display_name: str = "force out"]
        ForceOut = 6,
        #[display_name: str = "caught out"]
        CaughtOut = 7,
        #[display_name: str = "grounded out"]
        GroundedOut = 8,
        #[display_name: str = "walk"]
        Walk = 9,
        #[display_name: str = "home run"]
        HomeRun = 10,
        #[display_name: str = "fielding error"]
        FieldingError = 11,
        #[display_name: str = "hit by pitch"]
        HitByPitch = 12,
        #[display_name: str = "double play"]
        DoublePlay = 13,
        #[display_name: str = "fielder's choice"]
        FieldersChoice = 14,
        #[display_name: str = "error on fielder's choice"]
        ErrorOnFieldersChoice = 15,
    }
}

taxa! {
    #[
        schema = crate::taxa_schema::taxa::hit_type,
        table = crate::taxa_schema::taxa::hit_type::dsl::hit_type,
        id_column = crate::taxa_schema::taxa::hit_type::dsl::id,
    ]
    pub enum TaxaHitType {
        #[base_number: i64 = 1]
        Single = 1,
        #[base_number: i64 = 2]
        Double = 2,
        #[base_number: i64 = 3]
        Triple = 3,
        #[base_number: i64 = 4]
        HomeRun = 4,
    }
}

impl Into<mmolb_parsing::enums::Distance> for TaxaHitType {
    fn into(self) -> mmolb_parsing::enums::Distance {
        match self {
            Self::Single => mmolb_parsing::enums::Distance::Single,
            Self::Double => mmolb_parsing::enums::Distance::Double,
            Self::Triple => mmolb_parsing::enums::Distance::Triple,
            Self::HomeRun => mmolb_parsing::enums::Distance::HomeRun,     
        }
    }
}

impl From<mmolb_parsing::enums::Distance> for TaxaHitType {
    fn from(other: mmolb_parsing::enums::Distance) -> Self {
        match other {
            mmolb_parsing::enums::Distance::Single => Self::Single,
            mmolb_parsing::enums::Distance::Double => Self::Double,
            mmolb_parsing::enums::Distance::Triple => Self::Triple,
            mmolb_parsing::enums::Distance::HomeRun => Self::HomeRun,
        }
    }
}

taxa! {
    #[
        schema = crate::taxa_schema::taxa::leagues,
        table = crate::taxa_schema::taxa::leagues::dsl::leagues,
        id_column = crate::taxa_schema::taxa::leagues::dsl::id,
    ]
    pub enum TaxaLeagues {
        #[mmolb_league_id: str = "6805db0cac48194de3cd3fe4", parent_team_id: str = "6805db0dac48194de3cd4257", emoji: str = "☘️", color: str = "39993a", league_type: str = "Greater"]
        Clover = 1,
        #[mmolb_league_id: str = "6805db0cac48194de3cd3fe5", parent_team_id: str = "6805db0dac48194de3cd4258", emoji: str = "🍍", color: str = "feea63", league_type: str = "Greater"]
        Pineapple = 2,
        #[mmolb_league_id: str = "6805db0cac48194de3cd3fe7", parent_team_id: str = "6805db0cac48194de3cd3ff7", emoji: str = "⚾", color: str = "47678e", league_type: str = "Lesser"]
        Baseball = 3,
        #[mmolb_league_id: str = "6805db0cac48194de3cd3fe8", parent_team_id: str = "6805db0cac48194de3cd400a", emoji: str = "🎯", color: str = "507d45", league_type: str = "Lesser"] 
        Precision = 4,
        #[mmolb_league_id: str = "6805db0cac48194de3cd3fe9", parent_team_id: str = "6805db0cac48194de3cd401d", emoji: str = "🔺", color: str = "7c65a3", league_type: str = "Lesser"]
        Isosceles = 5,
        #[mmolb_league_id: str = "6805db0cac48194de3cd3fea", parent_team_id: str = "6805db0cac48194de3cd4030", emoji: str = "🗽", color: str = "2e768d", league_type: str = "Lesser"]
        Liberty = 6,
        #[mmolb_league_id: str = "6805db0cac48194de3cd3feb", parent_team_id: str = "6805db0cac48194de3cd4043", emoji: str = "🍁", color: str = "a13e33", league_type: str = "Lesser"]
        Maple = 7,
        #[mmolb_league_id: str = "6805db0cac48194de3cd3fec", parent_team_id: str = "6805db0cac48194de3cd4056", emoji: str = "🦗", color: str = "4a8546", league_type: str = "Lesser"]
        Cricket = 8,
        #[mmolb_league_id: str = "6805db0cac48194de3cd3fed", parent_team_id: str = "6805db0cac48194de3cd4069", emoji: str = "🌪️", color: str = "5a5e6e", league_type: str = "Lesser"]
        Tornado = 9,
        #[mmolb_league_id: str = "6805db0cac48194de3cd3fee", parent_team_id: str = "6805db0cac48194de3cd407c", emoji: str = "🪲", color: str = "3f624d", league_type: str = "Lesser"]
        Coleoptera = 10,
        #[mmolb_league_id: str = "6805db0cac48194de3cd3fef", parent_team_id: str = "6805db0cac48194de3cd408f", emoji: str = "🧼", color: str = "88b9ba", league_type: str = "Lesser"]
        Clean = 11,
        #[mmolb_league_id: str = "6805db0cac48194de3cd3ff0", parent_team_id: str = "6805db0cac48194de3cd40a2", emoji: str = "✨", color: str = "e0d95a", league_type: str = "Lesser"]
        Shiny = 12,
        #[mmolb_league_id: str = "6805db0cac48194de3cd3ff1", parent_team_id: str = "6805db0cac48194de3cd40b5", emoji: str = "🔮", color: str = "734d92", league_type: str = "Lesser"]
        Psychic = 13,
        #[mmolb_league_id: str = "6805db0cac48194de3cd3ff2", parent_team_id: str = "6805db0cac48194de3cd40c8", emoji: str = "❓", color: str = "6c6c6c", league_type: str = "Lesser"]
        Unidentified = 14,
        #[mmolb_league_id: str = "6805db0cac48194de3cd3ff3", parent_team_id: str = "6805db0cac48194de3cd40db", emoji: str = "👻", color: str = "5b4b62", league_type: str = "Lesser"]
        Ghastly = 15,
        #[mmolb_league_id: str = "6805db0cac48194de3cd3ff4", parent_team_id: str = "6805db0cac48194de3cd40ee", emoji: str = "🐸", color: str = "5b9340", league_type: str = "Lesser"]
        Amphibian = 16,
        #[mmolb_league_id: str = "6805db0cac48194de3cd3ff5", parent_team_id: str = "6805db0cac48194de3cd4101", emoji: str = "🌊", color: str = "1a3a4f", league_type: str = "Lesser"]
        Deep = 17,
        #[mmolb_league_id: str = "6805db0cac48194de3cd3ff6", parent_team_id: str = "6805db0cac48194de3cd4114", emoji: str = "🎵", color: str = "659b87", league_type: str = "Lesser"]
        Harmony = 18,
    }
}

taxa! {
    #[
        schema = crate::taxa_schema::taxa::position,
        table = crate::taxa_schema::taxa::position::dsl::position,
        id_column = crate::taxa_schema::taxa::position::dsl::id,
    ]
    pub enum TaxaPosition {
        #[display_name: str = "Pitcher", abbreviation: str = "P", position_area: str = "Infield"]
        Pitcher = 1,
        #[display_name: str = "Catcher", abbreviation: str = "C", position_area: str = "Infield"]
        Catcher = 2,
        #[display_name: str = "First base", abbreviation: str = "1B", position_area: str = "Infield"]
        FirstBase = 3,
        #[display_name: str = "Second base", abbreviation: str = "2B", position_area: str = "Infield"]
        SecondBase = 4,
        #[display_name: str = "Third base", abbreviation: str = "3B", position_area: str = "Infield"]
        ThirdBase = 5,
        #[display_name: str = "Shortstop", abbreviation: str = "SS", position_area: str = "Infield"]
        Shortstop = 6,
        #[display_name: str = "Left fielder", abbreviation: str = "LF", position_area: str = "Outfield"]
        LeftField = 7,
        #[display_name: str = "Center fielder", abbreviation: str = "CF", position_area: str = "Outfield"]
        CenterField = 8,
        #[display_name: str = "Right fielder", abbreviation: str = "RF", position_area: str = "Outfield"]
        RightField = 9,
        // TODO The following are roles, not positions
        #[display_name: str = "Starting pitcher", abbreviation: str = "SP", position_area: str = "Infield"]
        StartingPitcher = 10,
        #[display_name: str = "Relief pitcher", abbreviation: str = "RP", position_area: str = "Infield"]
        ReliefPitcher = 11,
        #[display_name: str = "Closer", abbreviation: str = "CL", position_area: str = "Infield"]
        Closer = 12,
        #[display_name: str = "Designated hitter", abbreviation: str = "DH", position_area: str = "None"]
        DesignatedHitter = 13,
    }
}

impl Into<mmolb_parsing::enums::Position> for TaxaPosition {
    fn into(self) -> mmolb_parsing::enums::Position {
        match self {
            TaxaPosition::Pitcher => mmolb_parsing::enums::Position::Pitcher,
            TaxaPosition::Catcher => mmolb_parsing::enums::Position::Catcher,
            TaxaPosition::FirstBase => mmolb_parsing::enums::Position::FirstBaseman,
            TaxaPosition::SecondBase => mmolb_parsing::enums::Position::SecondBaseman,
            TaxaPosition::ThirdBase => mmolb_parsing::enums::Position::ThirdBaseman,
            TaxaPosition::Shortstop => mmolb_parsing::enums::Position::ShortStop,
            TaxaPosition::LeftField => mmolb_parsing::enums::Position::LeftField,
            TaxaPosition::CenterField => mmolb_parsing::enums::Position::CenterField,
            TaxaPosition::RightField => mmolb_parsing::enums::Position::RightField,
            TaxaPosition::StartingPitcher => mmolb_parsing::enums::Position::StartingPitcher,
            TaxaPosition::ReliefPitcher => mmolb_parsing::enums::Position::ReliefPitcher,
            TaxaPosition::Closer => mmolb_parsing::enums::Position::Closer,
            TaxaPosition::DesignatedHitter => mmolb_parsing::enums::Position::DesignatedHitter,
        }
    }
}

impl From<mmolb_parsing::enums::Position> for TaxaPosition {
    fn from(other: mmolb_parsing::enums::Position) -> Self {
        match other {
            mmolb_parsing::enums::Position::Pitcher => TaxaPosition::Pitcher,
            mmolb_parsing::enums::Position::Catcher => TaxaPosition::Catcher,
            mmolb_parsing::enums::Position::FirstBaseman => TaxaPosition::FirstBase,
            mmolb_parsing::enums::Position::SecondBaseman => TaxaPosition::SecondBase,
            mmolb_parsing::enums::Position::ThirdBaseman => TaxaPosition::ThirdBase,
            mmolb_parsing::enums::Position::ShortStop => TaxaPosition::Shortstop,
            mmolb_parsing::enums::Position::LeftField => TaxaPosition::LeftField,
            mmolb_parsing::enums::Position::CenterField => TaxaPosition::CenterField,
            mmolb_parsing::enums::Position::RightField => TaxaPosition::RightField,
            mmolb_parsing::enums::Position::StartingPitcher => TaxaPosition::StartingPitcher,
            mmolb_parsing::enums::Position::ReliefPitcher => TaxaPosition::ReliefPitcher,
            mmolb_parsing::enums::Position::Closer => TaxaPosition::Closer,
            mmolb_parsing::enums::Position::DesignatedHitter => TaxaPosition::DesignatedHitter,
        }
    }
}

impl TryInto<mmolb_parsing::enums::FairBallDestination> for TaxaPosition {
    type Error = Self;

    fn try_into(self) -> Result<mmolb_parsing::enums::FairBallDestination, Self::Error> {
        match self {
            TaxaPosition::Pitcher => Ok(mmolb_parsing::enums::FairBallDestination::Pitcher),
            TaxaPosition::Catcher => Ok(mmolb_parsing::enums::FairBallDestination::Catcher),
            TaxaPosition::FirstBase => Ok(mmolb_parsing::enums::FairBallDestination::FirstBase),
            TaxaPosition::SecondBase => Ok(mmolb_parsing::enums::FairBallDestination::SecondBase),
            TaxaPosition::ThirdBase => Ok(mmolb_parsing::enums::FairBallDestination::ThirdBase),
            TaxaPosition::Shortstop => Ok(mmolb_parsing::enums::FairBallDestination::ShortStop),
            TaxaPosition::LeftField => Ok(mmolb_parsing::enums::FairBallDestination::LeftField),
            TaxaPosition::CenterField => Ok(mmolb_parsing::enums::FairBallDestination::CenterField),
            TaxaPosition::RightField => Ok(mmolb_parsing::enums::FairBallDestination::RightField),
            TaxaPosition::StartingPitcher => Err(TaxaPosition::StartingPitcher),
            TaxaPosition::ReliefPitcher => Err(TaxaPosition::ReliefPitcher),
            TaxaPosition::Closer => Err(TaxaPosition::Closer),
            TaxaPosition::DesignatedHitter => Err(TaxaPosition::DesignatedHitter),
        }
    }
}

impl From<mmolb_parsing::enums::FairBallDestination> for TaxaPosition {
    fn from(other: mmolb_parsing::enums::FairBallDestination) -> Self {
        match other {
            mmolb_parsing::enums::FairBallDestination::Pitcher => TaxaPosition::Pitcher,
            mmolb_parsing::enums::FairBallDestination::Catcher => TaxaPosition::Catcher,
            mmolb_parsing::enums::FairBallDestination::FirstBase => TaxaPosition::FirstBase,
            mmolb_parsing::enums::FairBallDestination::SecondBase => TaxaPosition::SecondBase,
            mmolb_parsing::enums::FairBallDestination::ThirdBase => TaxaPosition::ThirdBase,
            mmolb_parsing::enums::FairBallDestination::ShortStop => TaxaPosition::Shortstop,
            mmolb_parsing::enums::FairBallDestination::LeftField => TaxaPosition::LeftField,
            mmolb_parsing::enums::FairBallDestination::CenterField => TaxaPosition::CenterField,
            mmolb_parsing::enums::FairBallDestination::RightField => TaxaPosition::RightField,
        }
    }
}

taxa! {
    #[
        schema = crate::taxa_schema::taxa::fair_ball_type,
        table = crate::taxa_schema::taxa::fair_ball_type::dsl::fair_ball_type,
        id_column = crate::taxa_schema::taxa::fair_ball_type::dsl::id,
    ]
    pub enum TaxaFairBallType {
        #[display_name: str = "Ground ball"]
        GroundBall = 1,
        #[display_name: str = "Fly ball"]
        FlyBall = 2,
        #[display_name: str = "Line drive"]
        LineDrive = 3,
        #[display_name: str = "Popup"]
        Popup = 4,
    }
}

impl Into<mmolb_parsing::enums::FairBallType> for TaxaFairBallType {
    fn into(self) -> mmolb_parsing::enums::FairBallType {
        match self {
            Self::GroundBall => mmolb_parsing::enums::FairBallType::GroundBall,
            Self::FlyBall => mmolb_parsing::enums::FairBallType::FlyBall,
            Self::LineDrive => mmolb_parsing::enums::FairBallType::LineDrive,
            Self::Popup => mmolb_parsing::enums::FairBallType::Popup,
        }
    }
}

impl From<mmolb_parsing::enums::FairBallType> for TaxaFairBallType {
    fn from(value: mmolb_parsing::enums::FairBallType) -> Self {
        match value {
            mmolb_parsing::enums::FairBallType::GroundBall => Self::GroundBall,
            mmolb_parsing::enums::FairBallType::FlyBall => Self::FlyBall,
            mmolb_parsing::enums::FairBallType::LineDrive => Self::LineDrive,
            mmolb_parsing::enums::FairBallType::Popup => Self::Popup,
        }
    }
}

taxa! {
    #[
        schema = crate::taxa_schema::taxa::base,
        table = crate::taxa_schema::taxa::base::dsl::base,
        id_column = crate::taxa_schema::taxa::base::dsl::id,
        derive = (PartialOrd,)
    ]
    pub enum TaxaBase {
        #[bases_achieved: i64 = 4]
        Home = 0,
        #[bases_achieved: i64 = 1]
        First = 1,
        #[bases_achieved: i64 = 2]
        Second = 2,
        #[bases_achieved: i64 = 3]
        Third = 3,
    }
}

impl TaxaBase {
    pub fn next_base(self) -> TaxaBase {
        match self {
            TaxaBase::Home => TaxaBase::First,
            TaxaBase::First => TaxaBase::Second,
            TaxaBase::Second => TaxaBase::Third,
            TaxaBase::Third => TaxaBase::Home,
        }
    }
}

impl Into<mmolb_parsing::enums::Base> for TaxaBase {
    fn into(self) -> mmolb_parsing::enums::Base {
        match self {
            Self::Home => mmolb_parsing::enums::Base::Home,
            Self::First => mmolb_parsing::enums::Base::First,
            Self::Second => mmolb_parsing::enums::Base::Second,
            Self::Third => mmolb_parsing::enums::Base::Third,
        }
    }
}

impl From<mmolb_parsing::enums::Base> for TaxaBase {
    fn from(value: mmolb_parsing::enums::Base) -> Self {
        match value {
            mmolb_parsing::enums::Base::Home => Self::Home,
            mmolb_parsing::enums::Base::First => Self::First,
            mmolb_parsing::enums::Base::Second => Self::Second,
            mmolb_parsing::enums::Base::Third => Self::Third,
        }
    }
}

impl From<mmolb_parsing::enums::Distance> for TaxaBase {
    fn from(value: mmolb_parsing::enums::Distance) -> Self {
        match value {
            mmolb_parsing::enums::Distance::Single => Self::First,
            mmolb_parsing::enums::Distance::Double => Self::Second,
            mmolb_parsing::enums::Distance::Triple => Self::Third,
        }
    }
}

impl From<mmolb_parsing::enums::BaseNameVariant> for TaxaBase {
    fn from(value: mmolb_parsing::enums::BaseNameVariant) -> Self {
        match value {
            mmolb_parsing::enums::BaseNameVariant::First => TaxaBase::First,
            mmolb_parsing::enums::BaseNameVariant::FirstBase => TaxaBase::First,
            mmolb_parsing::enums::BaseNameVariant::OneB => TaxaBase::First,
            mmolb_parsing::enums::BaseNameVariant::Second => TaxaBase::Second,
            mmolb_parsing::enums::BaseNameVariant::SecondBase => TaxaBase::Second,
            mmolb_parsing::enums::BaseNameVariant::TwoB => TaxaBase::Second,
            mmolb_parsing::enums::BaseNameVariant::ThirdBase => TaxaBase::Third,
            mmolb_parsing::enums::BaseNameVariant::Third => TaxaBase::Third,
            mmolb_parsing::enums::BaseNameVariant::ThreeB => TaxaBase::Third,
            mmolb_parsing::enums::BaseNameVariant::Home => TaxaBase::Home,
        }
    }
}

taxa! {
    #[
        schema = crate::taxa_schema::taxa::base_description_format,
        table = crate::taxa_schema::taxa::base_description_format::dsl::base_description_format,
        id_column = crate::taxa_schema::taxa::base_description_format::dsl::id,
    ]
    pub enum TaxaBaseDescriptionFormat {
        NumberB = 1,  // e.g. "1B"
        Name = 2,     // e.g. "first"
        NameBase = 3, // e.g. "first base"
    }
}

// Newtype just to hang a From impl on
pub struct TaxaBaseWithDescriptionFormat(pub TaxaBase, pub TaxaBaseDescriptionFormat);

impl Into<mmolb_parsing::enums::BaseNameVariant> for TaxaBaseWithDescriptionFormat {
    fn into(self) -> mmolb_parsing::enums::BaseNameVariant {
        match (self.0, self.1) {
            (TaxaBase::First, TaxaBaseDescriptionFormat::NumberB) => {
                mmolb_parsing::enums::BaseNameVariant::OneB
            }
            (TaxaBase::First, TaxaBaseDescriptionFormat::Name) => {
                mmolb_parsing::enums::BaseNameVariant::First
            }
            (TaxaBase::First, TaxaBaseDescriptionFormat::NameBase) => {
                mmolb_parsing::enums::BaseNameVariant::FirstBase
            }
            (TaxaBase::Second, TaxaBaseDescriptionFormat::NumberB) => {
                mmolb_parsing::enums::BaseNameVariant::TwoB
            }
            (TaxaBase::Second, TaxaBaseDescriptionFormat::Name) => {
                mmolb_parsing::enums::BaseNameVariant::Second
            }
            (TaxaBase::Second, TaxaBaseDescriptionFormat::NameBase) => {
                mmolb_parsing::enums::BaseNameVariant::SecondBase
            }
            (TaxaBase::Third, TaxaBaseDescriptionFormat::NumberB) => {
                mmolb_parsing::enums::BaseNameVariant::ThreeB
            }
            (TaxaBase::Third, TaxaBaseDescriptionFormat::Name) => {
                mmolb_parsing::enums::BaseNameVariant::Third
            }
            (TaxaBase::Third, TaxaBaseDescriptionFormat::NameBase) => {
                mmolb_parsing::enums::BaseNameVariant::ThirdBase
            }
            (TaxaBase::Home, _) => mmolb_parsing::enums::BaseNameVariant::Home,
        }
    }
}

impl From<mmolb_parsing::enums::BaseNameVariant> for TaxaBaseDescriptionFormat {
    fn from(value: mmolb_parsing::enums::BaseNameVariant) -> Self {
        match value {
            mmolb_parsing::enums::BaseNameVariant::First => TaxaBaseDescriptionFormat::Name,
            mmolb_parsing::enums::BaseNameVariant::FirstBase => TaxaBaseDescriptionFormat::NameBase,
            mmolb_parsing::enums::BaseNameVariant::OneB => TaxaBaseDescriptionFormat::NumberB,
            mmolb_parsing::enums::BaseNameVariant::Second => TaxaBaseDescriptionFormat::Name,
            mmolb_parsing::enums::BaseNameVariant::SecondBase => {
                TaxaBaseDescriptionFormat::NameBase
            }
            mmolb_parsing::enums::BaseNameVariant::TwoB => TaxaBaseDescriptionFormat::NumberB,
            mmolb_parsing::enums::BaseNameVariant::ThirdBase => TaxaBaseDescriptionFormat::NameBase,
            mmolb_parsing::enums::BaseNameVariant::Third => TaxaBaseDescriptionFormat::Name,
            mmolb_parsing::enums::BaseNameVariant::ThreeB => TaxaBaseDescriptionFormat::NumberB,
            mmolb_parsing::enums::BaseNameVariant::Home => TaxaBaseDescriptionFormat::Name,
        }
    }
}

taxa! {
    #[
        schema = crate::taxa_schema::taxa::fielding_error_type,
        table = crate::taxa_schema::taxa::fielding_error_type::dsl::fielding_error_type,
        id_column = crate::taxa_schema::taxa::fielding_error_type::dsl::id,
    ]
    pub enum TaxaFieldingErrorType {
        Fielding = 1,
        Throwing = 2,
    }
}

impl Into<mmolb_parsing::enums::FieldingErrorType> for TaxaFieldingErrorType {
    fn into(self) -> mmolb_parsing::enums::FieldingErrorType {
        match self {
            Self::Fielding => mmolb_parsing::enums::FieldingErrorType::Fielding,
            Self::Throwing => mmolb_parsing::enums::FieldingErrorType::Throwing,
        }
    }
}

impl From<mmolb_parsing::enums::FieldingErrorType> for TaxaFieldingErrorType {
    fn from(value: mmolb_parsing::enums::FieldingErrorType) -> Self {
        match value {
            mmolb_parsing::enums::FieldingErrorType::Fielding => Self::Fielding,
            mmolb_parsing::enums::FieldingErrorType::Throwing => Self::Throwing,
        }
    }
}

taxa! {
    #[
        schema = crate::taxa_schema::taxa::pitch_type,
        table = crate::taxa_schema::taxa::pitch_type::dsl::pitch_type,
        id_column = crate::taxa_schema::taxa::pitch_type::dsl::id,
    ]
    pub enum TaxaPitchType {
        #[display_name: str = "Fastball"]
        Fastball = 1,
        #[display_name: str = "Sinker"]
        Sinker = 2,
        #[display_name: str = "Slider"]
        Slider = 3,
        #[display_name: str = "Changeup"]
        Changeup = 4,
        #[display_name: str = "Curveball"]
        Curveball = 5,
        #[display_name: str = "Cutter"]
        Cutter = 6,
        #[display_name: str = "Sweeper"]
        Sweeper = 7,
        #[display_name: str = "Knuckle curve"]
        KnuckleCurve = 8,
        #[display_name: str = "Splitter"]
        Splitter = 9,
    }
}

impl Into<mmolb_parsing::enums::PitchType> for TaxaPitchType {
    fn into(self) -> mmolb_parsing::enums::PitchType {
        match self {
            Self::Changeup => mmolb_parsing::enums::PitchType::Changeup,
            Self::Sinker => mmolb_parsing::enums::PitchType::Sinker,
            Self::Slider => mmolb_parsing::enums::PitchType::Slider,
            Self::Curveball => mmolb_parsing::enums::PitchType::Curveball,
            Self::Cutter => mmolb_parsing::enums::PitchType::Cutter,
            Self::Sweeper => mmolb_parsing::enums::PitchType::Sweeper,
            Self::KnuckleCurve => mmolb_parsing::enums::PitchType::KnuckleCurve,
            Self::Splitter => mmolb_parsing::enums::PitchType::Splitter,
            Self::Fastball => mmolb_parsing::enums::PitchType::Fastball,
        }
    }
}

impl From<mmolb_parsing::enums::PitchType> for TaxaPitchType {
    fn from(value: mmolb_parsing::enums::PitchType) -> Self {
        match value {
            mmolb_parsing::enums::PitchType::Changeup => Self::Changeup,
            mmolb_parsing::enums::PitchType::Sinker => Self::Sinker,
            mmolb_parsing::enums::PitchType::Slider => Self::Slider,
            mmolb_parsing::enums::PitchType::Curveball => Self::Curveball,
            mmolb_parsing::enums::PitchType::Cutter => Self::Cutter,
            mmolb_parsing::enums::PitchType::Sweeper => Self::Sweeper,
            mmolb_parsing::enums::PitchType::KnuckleCurve => Self::KnuckleCurve,
            mmolb_parsing::enums::PitchType::Splitter => Self::Splitter,
            mmolb_parsing::enums::PitchType::Fastball => Self::Fastball,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Taxa {
    event_type_mapping: EnumMap<TaxaEventType, i64>,
    hit_type_mapping: EnumMap<TaxaHitType, i64>,
    position_mapping: EnumMap<TaxaPosition, i64>,
    fair_ball_type_mapping: EnumMap<TaxaFairBallType, i64>,
    base_mapping: EnumMap<TaxaBase, i64>,
    base_description_format_mapping: EnumMap<TaxaBaseDescriptionFormat, i64>,
    fielding_error_type_mapping: EnumMap<TaxaFieldingErrorType, i64>,
    pitch_type_mapping: EnumMap<TaxaPitchType, i64>,
    league_mapping: EnumMap<TaxaLeagues, i64>,
}

impl Taxa {
    pub fn new(conn: &mut PgConnection) -> QueryResult<Self> {
        Ok(Self {
            event_type_mapping: TaxaEventType::make_id_mapping(conn)?,
            hit_type_mapping: TaxaHitType::make_id_mapping(conn)?,
            position_mapping: TaxaPosition::make_id_mapping(conn)?,
            fair_ball_type_mapping: TaxaFairBallType::make_id_mapping(conn)?,
            base_mapping: TaxaBase::make_id_mapping(conn)?,
            base_description_format_mapping: TaxaBaseDescriptionFormat::make_id_mapping(conn)?,
            fielding_error_type_mapping: TaxaFieldingErrorType::make_id_mapping(conn)?,
            pitch_type_mapping: TaxaPitchType::make_id_mapping(conn)?,
            league_mapping: TaxaLeagues::make_id_mapping(conn)?,
        })
    }

    pub fn event_type_id(&self, ty: TaxaEventType) -> i64 {
        self.event_type_mapping[ty]
    }

    pub fn hit_type_id(&self, ty: TaxaHitType) -> i64 {
        self.hit_type_mapping[ty]
    }

    pub fn position_id(&self, ty: TaxaPosition) -> i64 {
        self.position_mapping[ty]
    }

    pub fn fair_ball_type_id(&self, ty: TaxaFairBallType) -> i64 {
        self.fair_ball_type_mapping[ty]
    }

    pub fn base_id(&self, ty: TaxaBase) -> i64 {
        self.base_mapping[ty]
    }

    pub fn base_description_format_id(&self, ty: TaxaBaseDescriptionFormat) -> i64 {
        self.base_description_format_mapping[ty]
    }

    pub fn fielding_error_type_id(&self, ty: TaxaFieldingErrorType) -> i64 {
        self.fielding_error_type_mapping[ty]
    }

    pub fn pitch_type_id(&self, ty: TaxaPitchType) -> i64 {
        self.pitch_type_mapping[ty]
    }

    pub fn league_id(&self, ty: TaxaLeagues) -> i64 {
        self.league_mapping[ty]
    }

    pub fn event_type_from_id(&self, id: i64) -> Option<TaxaEventType> {
        self.event_type_mapping
            .iter()
            .find(|(_, ty_id)| id == **ty_id)
            .map(|(val, _)| val)
    }

    pub fn hit_type_from_id(&self, id: i64) -> TaxaHitType {
        self.hit_type_mapping
            .iter()
            .find(|(_, ty_id)| id == **ty_id)
            .expect("TODO Handle unknown hit type")
            .0
    }

    pub fn position_from_id(&self, id: i64) -> TaxaPosition {
        self.position_mapping
            .iter()
            .find(|(_, ty_id)| id == **ty_id)
            .expect("TODO Handle unknown position")
            .0
    }

    pub fn fair_ball_type_from_id(&self, id: i64) -> TaxaFairBallType {
        self.fair_ball_type_mapping
            .iter()
            .find(|(_, ty_id)| id == **ty_id)
            .expect("TODO Handle unknown fair ball type")
            .0
    }

    pub fn base_from_id(&self, id: i64) -> TaxaBase {
        self.base_mapping
            .iter()
            .find(|(_, ty_id)| id == **ty_id)
            .expect("TODO Handle unknown base type")
            .0
    }

    pub fn base_description_format_from_id(&self, id: i64) -> TaxaBaseDescriptionFormat {
        self.base_description_format_mapping
            .iter()
            .find(|(_, ty_id)| id == **ty_id)
            .expect("TODO Handle unknown base description format")
            .0
    }

    pub fn fielding_error_type_from_id(&self, id: i64) -> TaxaFieldingErrorType {
        self.fielding_error_type_mapping
            .iter()
            .find(|(_, ty_id)| id == **ty_id)
            .expect("TODO Handle unknown base description format")
            .0
    }

    pub fn pitch_type_from_id(&self, id: i64) -> TaxaPitchType {
        self.pitch_type_mapping
            .iter()
            .find(|(_, ty_id)| id == **ty_id)
            .expect("TODO Handle unknown pitch type")
            .0
    }

    pub fn league_from_id(&self, id: i64) -> TaxaLeagues {
        self.league_mapping
            .iter()
            .find(|(_, ty_id)| id == **ty_id)
            .expect("TODO Handle unknown laegue")
            .0
    }

}
