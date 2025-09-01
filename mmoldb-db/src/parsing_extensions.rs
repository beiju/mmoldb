use mmolb_parsing::enums::{Place, Position, Slot, SlotDiscriminants};
use mmolb_parsing::parsed_event::PlacedPlayer;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum BestEffortSlot {
    Slot(Slot),
    SlotType(SlotDiscriminants),
    GenericPitcher,
}

impl BestEffortSlot {
    pub fn from_slot(slot: Slot) -> Self {
        Self::Slot(slot)
    }

    pub fn from_position(position: Position) -> Self {
        match position {
            Position::Pitcher => Self::GenericPitcher,
            Position::Catcher => Self::SlotType(SlotDiscriminants::Catcher),
            Position::FirstBaseman => Self::SlotType(SlotDiscriminants::FirstBaseman),
            Position::SecondBaseman => Self::SlotType(SlotDiscriminants::SecondBaseman),
            Position::ThirdBaseman => Self::SlotType(SlotDiscriminants::ThirdBaseman),
            Position::ShortStop => Self::SlotType(SlotDiscriminants::ShortStop),
            Position::LeftField => Self::SlotType(SlotDiscriminants::LeftField),
            Position::CenterField => Self::SlotType(SlotDiscriminants::CenterField),
            Position::RightField => Self::SlotType(SlotDiscriminants::RightField),
            Position::StartingPitcher => Self::SlotType(SlotDiscriminants::StartingPitcher),
            Position::ReliefPitcher => Self::SlotType(SlotDiscriminants::ReliefPitcher),
            Position::Closer => Self::SlotType(SlotDiscriminants::Closer),
        }
    }
}

impl Display for BestEffortSlot {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            BestEffortSlot::Slot(s) => write!(f, "{}", s),
            // This might not be right...
            BestEffortSlot::SlotType(st) => write!(f, "{}", st),
            BestEffortSlot::GenericPitcher => write!(f, "P"),
        }
    }
}

impl From<Place> for BestEffortSlot {
    fn from(value: Place) -> Self {
        match value {
            Place::Pitcher => BestEffortSlot::GenericPitcher,
            Place::Catcher => BestEffortSlot::Slot(Slot::Catcher),
            Place::FirstBaseman => BestEffortSlot::Slot(Slot::FirstBaseman),
            Place::SecondBaseman => BestEffortSlot::Slot(Slot::SecondBaseman),
            Place::ThirdBaseman => BestEffortSlot::Slot(Slot::ThirdBaseman),
            Place::ShortStop => BestEffortSlot::Slot(Slot::ShortStop),
            Place::LeftField => BestEffortSlot::Slot(Slot::LeftField),
            Place::CenterField => BestEffortSlot::Slot(Slot::CenterField),
            Place::RightField => BestEffortSlot::Slot(Slot::RightField),
            Place::StartingPitcher(Some(i)) => BestEffortSlot::Slot(Slot::StartingPitcher(i)),
            Place::ReliefPitcher(Some(i)) => BestEffortSlot::Slot(Slot::ReliefPitcher(i)),
            Place::Closer => BestEffortSlot::Slot(Slot::Closer),
            Place::DesignatedHitter => BestEffortSlot::Slot(Slot::DesignatedHitter),
            Place::StartingPitcher(None) => {
                BestEffortSlot::SlotType(SlotDiscriminants::StartingPitcher)
            }
            Place::ReliefPitcher(None) => {
                BestEffortSlot::SlotType(SlotDiscriminants::ReliefPitcher)
            }
        }
    }
}

impl Into<Place> for BestEffortSlot {
    fn into(self) -> Place {
        match self {
            BestEffortSlot::Slot(s) => {
                match s {
                    Slot::Catcher => Place::Catcher,
                    Slot::FirstBaseman => Place::FirstBaseman,
                    Slot::SecondBaseman => Place::SecondBaseman,
                    Slot::ThirdBaseman => Place::ThirdBaseman,
                    Slot::ShortStop => Place::ShortStop,
                    Slot::LeftField => Place::LeftField,
                    Slot::CenterField => Place::CenterField,
                    Slot::RightField => Place::RightField,
                    Slot::StartingPitcher(n) => Place::StartingPitcher(Some(n)),
                    Slot::ReliefPitcher(n) => Place::ReliefPitcher(Some(n)),
                    Slot::Closer => Place::Closer,
                    Slot::DesignatedHitter => Place::DesignatedHitter,
                }
            }
            BestEffortSlot::SlotType(t) => {
                match t {
                    SlotDiscriminants::Catcher => Place::Catcher,
                    SlotDiscriminants::FirstBaseman => Place::FirstBaseman,
                    SlotDiscriminants::SecondBaseman => Place::SecondBaseman,
                    SlotDiscriminants::ThirdBaseman => Place::ThirdBaseman,
                    SlotDiscriminants::ShortStop => Place::ShortStop,
                    SlotDiscriminants::LeftField => Place::LeftField,
                    SlotDiscriminants::CenterField => Place::CenterField,
                    SlotDiscriminants::RightField => Place::RightField,
                    SlotDiscriminants::StartingPitcher => Place::StartingPitcher(None),
                    SlotDiscriminants::ReliefPitcher => Place::ReliefPitcher(None),
                    SlotDiscriminants::Closer => Place::Closer,
                    SlotDiscriminants::DesignatedHitter => Place::DesignatedHitter,
                }
            }
            BestEffortSlot::GenericPitcher => Place::Pitcher,
        }
    }
}

impl FromStr for BestEffortSlot {
    type Err = <Slot as FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let slot = s.parse::<Slot>()?;
        Ok(Self::Slot(slot))
    }
}

#[derive(Debug, Copy, Clone)]
pub struct BestEffortSlottedPlayer<StrT> {
    pub name: StrT,
    pub slot: BestEffortSlot,
}

impl<StrT> From<PlacedPlayer<StrT>> for BestEffortSlottedPlayer<StrT> {
    fn from(value: PlacedPlayer<StrT>) -> Self {
        BestEffortSlottedPlayer {
            name: value.name,
            slot: value.place.into(),
        }
    }
}

impl<StrT: Display> Display for BestEffortSlottedPlayer<StrT> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.slot, self.name)
    }
}
