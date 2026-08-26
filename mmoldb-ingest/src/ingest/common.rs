use std::fmt::{Display, Formatter};

#[derive(Debug, Copy, Clone)]
pub enum VersionedIngestKind {
    Time,
    Team,
    Player,
}

impl VersionedIngestKind {
    pub fn as_kind(self) -> &'static str {
        match self {
            VersionedIngestKind::Time => "time",
            VersionedIngestKind::Team => "team",
            VersionedIngestKind::Player => "player",
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub enum EntityIngestKind {
    Game,
}

impl EntityIngestKind {
    pub fn as_kind(self) -> &'static str {
        match self {
            EntityIngestKind::Game => "game",
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub enum IngestKind {
    Versioned(VersionedIngestKind),
    CombinedFeed,
    Entity(EntityIngestKind),
}

impl Display for IngestKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            IngestKind::Versioned(k) => write!(f, "{}", k.as_kind()),
            IngestKind::CombinedFeed => write!(f, "feed"),
            IngestKind::Entity(k) => write!(f, "{}", k.as_kind()),
        }
    }
}