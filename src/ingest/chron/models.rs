use chrono::{DateTime, Utc};
use mmolb_parsing::enums::{Day, Position, PositionType};
use rocket::serde::{Deserialize, Serialize};
use strum::EnumDiscriminants;

#[derive(Debug, Serialize, Deserialize)]
pub struct ChronEntities<EntityT> {
    pub items: Vec<ChronEntity<EntityT>>,
    pub next_page: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChronEntity<EntityT> {
    pub kind: String,
    pub entity_id: String,
    pub valid_from: DateTime<Utc>,
    pub valid_until: Option<DateTime<Utc>>,
    pub data: EntityT,
}

// We don't deserialize data we don't plan to store
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ChronPlayerModification {
    pub name: String,
    pub emoji: String,
    pub description: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ChronHandedness {
    #[serde(rename = "R")]
    Right,
    #[serde(rename = "L")]
    Left,
    #[serde(rename = "S")]
    Switch,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ChronPlayer {
    pub bats: ChronHandedness,
    pub home: String,
    pub likes: String,
    pub number: i32,
    #[serde(rename = "TeamID")]
    pub team_id: String,
    pub throws: ChronHandedness,
    pub augments: i32,
    pub birthday: Day,
    pub dislikes: String,
    pub last_name: String,
    pub position: Position,
    pub first_name: String,
    pub durability: f64,
    pub lesser_boon: Option<ChronPlayerModification>,
    pub birthseason: i32, // Apparently birthseason is one word, like birthday
    pub greater_boon: Option<ChronPlayerModification>,
    // Ignoring SeasonStats for now
    pub position_type: PositionType,
    pub modifications: Vec<ChronPlayerModification>,
}

pub trait GameExt {
    /// Returns true for any game which will never be updated. This includes all
    /// finished games and a set of games from s0d60 that the sim lost track of
    /// and will never be finished.
    fn is_terminal(&self) -> bool;

    /// True for any game in the "Completed" state
    fn is_completed(&self) -> bool;
}

impl GameExt for mmolb_parsing::Game {
    fn is_terminal(&self) -> bool {
        // There are some games from season 0 that aren't completed and never
        // will be.
        self.season == 0 || self.is_completed()
    }

    fn is_completed(&self) -> bool {
        self.state == "Complete"
    }
}

#[derive(Debug, Serialize, Deserialize, EnumDiscriminants)]
#[non_exhaustive]
pub enum GenericChronEntities {
    Game(ChronEntities<mmolb_parsing::Game>),
    Player(ChronEntities<ChronPlayer>),
}
