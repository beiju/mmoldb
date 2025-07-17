use mmoldb_db::taxa::AsInsertable;
use mmoldb_db::taxa::{
    TaxaBase, TaxaBaseDescriptionFormat, TaxaBaseWithDescriptionFormat, TaxaEventType,
    TaxaFairBallType, TaxaFielderLocation, TaxaFieldingErrorType, TaxaPitchType, TaxaSlot,
};
use mmoldb_db::{BestEffortSlot, BestEffortSlottedPlayer, EventDetail, EventDetailFielder, EventDetailRunner, IngestLog};
use itertools::{EitherOrBoth, Itertools, PeekingNext};
use log::warn;
use miette::Diagnostic;
use mmolb_parsing::ParsedEventMessage;
use mmolb_parsing::enums::{
    Base, BaseNameVariant, BatterStat, Day, Distance, FairBallDestination, FairBallType,
    FieldingErrorType, FoulType, GameOverMessage, HomeAway, NowBattingStats, Place, StrikeType,
    TopBottom,
};
use mmolb_parsing::game::MaybePlayer;
use mmolb_parsing::parsed_event::{
    BaseSteal, EmojiTeam, FallingStarOutcome, FieldingAttempt, KnownBug,
    ParsedEventMessageDiscriminants, PlacedPlayer, RunnerAdvance, RunnerOut, StartOfInningPitcher,
};
use std::collections::{HashMap, VecDeque};
use std::fmt::Write;
use std::fmt::{Debug, Formatter};
use strum::IntoDiscriminant;
use thiserror::Error;

#[derive(Debug, Error, Diagnostic)]
#[error("Parse error: {}", .0)]
pub struct ParseError(mmolb_parsing::parsed_event::GameEventParseError);

#[derive(Debug, Error, Diagnostic)]
pub enum SimStartupError {
    #[error("This game had no events")]
    NoEvents,

    #[error("Not enough events. Expected {expected:?} event after {previous:?}")]
    NotEnoughEvents {
        expected: &'static [ParsedEventMessageDiscriminants],
        previous: ParsedEventMessageDiscriminants,
    },

    #[error(transparent)]
    ParseError {
        #[diagnostic_source]
        source: ParseError,
    },

    #[error("Expected {expected:?} event after {previous:?}, but received {received:?}")]
    UnexpectedEventType {
        expected: &'static [ParsedEventMessageDiscriminants],
        previous: Option<ParsedEventMessageDiscriminants>,
        received: ParsedEventMessageDiscriminants,
    },

    #[error("Couldn't parse game day")]
    FailedToParseGameDay {
        source: mmolb_parsing::NotRecognized,
    },

    #[error("Couldn't parse starting pitcher \"{0}\"")]
    FailedToParseStartingPitcher(String),
}

#[derive(Debug, Error, Diagnostic)]
pub enum SimEventError {
    #[error(transparent)]
    ParseError {
        #[diagnostic_source]
        source: ParseError,
    },

    #[error("Expected {expected:?} event after {previous:?}, but received {received:?}")]
    UnexpectedEventType {
        expected: &'static [ParsedEventMessageDiscriminants],
        previous: Option<ParsedEventMessageDiscriminants>,
        received: ParsedEventMessageDiscriminants,
    },

    #[error("Expected the automatic runner to be set by inning {inning_num}")]
    MissingAutomaticRunner { inning_num: u8 },
}

// A utility to more conveniently build a Vec<IngestLog>
pub struct IngestLogs {
    game_event_index: i32,
    logs: Vec<IngestLog>,
}

impl IngestLogs {
    pub fn new(game_event_index: i32) -> Self {
        Self {
            game_event_index,
            logs: Vec::new(),
        }
    }

    pub fn critical(&mut self, s: impl Into<String>) {
        self.logs.push(IngestLog {
            game_event_index: self.game_event_index,
            log_level: 0,
            log_text: s.into(),
        });
    }

    pub fn error(&mut self, s: impl Into<String>) {
        self.logs.push(IngestLog {
            game_event_index: self.game_event_index,
            log_level: 1,
            log_text: s.into(),
        });
    }

    pub fn warn(&mut self, s: impl Into<String>) {
        self.logs.push(IngestLog {
            game_event_index: self.game_event_index,
            log_level: 2,
            log_text: s.into(),
        });
    }

    pub fn info(&mut self, s: impl Into<String>) {
        self.logs.push(IngestLog {
            game_event_index: self.game_event_index,
            log_level: 3,
            log_text: s.into(),
        });
    }

    pub fn debug(&mut self, s: impl Into<String>) {
        self.logs.push(IngestLog {
            game_event_index: self.game_event_index,
            log_level: 4,
            log_text: s.into(),
        });
    }

    #[allow(dead_code)]
    pub fn trace(&mut self, s: impl Into<String>) {
        self.logs.push(IngestLog {
            game_event_index: self.game_event_index,
            log_level: 5,
            log_text: s.into(),
        });
    }

    pub fn into_vec(self) -> Vec<IngestLog> {
        self.logs
    }
}

#[derive(Debug, Copy, Clone)]
struct Pitch {
    pitch_type: mmolb_parsing::enums::PitchType,
    pitch_speed: f32,
    pitch_zone: u8,
}

#[derive(Debug, Copy, Clone)]
struct FairBall {
    game_event_index: usize,
    fair_ball_type: FairBallType,
    fair_ball_destination: FairBallDestination,
    pitch: Option<Pitch>,
}

#[derive(Debug, Copy, Clone)]
enum ContextAfterMoundVisitOutcome<'g> {
    ExpectNowBatting,
    ExpectPitch(&'g str),
}

impl<'g> Into<EventContext<'g>> for ContextAfterMoundVisitOutcome<'g> {
    fn into(self) -> EventContext<'g> {
        match self {
            ContextAfterMoundVisitOutcome::ExpectNowBatting => EventContext::ExpectNowBatting,
            ContextAfterMoundVisitOutcome::ExpectPitch(batter_name) => {
                EventContext::ExpectPitch(batter_name)
            }
        }
    }
}

#[derive(Debug, Copy, Clone)]
enum EventContext<'g> {
    ExpectInningStart,
    ExpectNowBatting,
    ExpectPitch(&'g str),
    ExpectFairBallOutcome(&'g str, FairBall),
    ExpectFallingStarOutcome {
        falling_star_hit_player: &'g str,
        batter_name: &'g str,
    },
    ExpectInningEnd,
    ExpectMoundVisitOutcome(ContextAfterMoundVisitOutcome<'g>),
    ExpectGameEnd,
    ExpectFinalScore,
    Finished,
}

#[derive(Debug, Default)]
pub struct BatterStats {
    hits: u8,
    at_bats: u8,
    stats: Vec<()>,
}

impl BatterStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.stats.is_empty() && self.hits == 0 && self.at_bats == 0
    }
}

#[derive(Debug)]
pub struct TeamInGame<'g> {
    team_name: &'g str,
    team_emoji: &'g str,
    // I need another field to store the automatic runner because it's
    // not always the batter who most recently stepped up, in the case
    // of automatic runners after an inning-ending CS
    automatic_runner: Option<&'g str>,
    // Need to store the team's active pitcher to facilitate upgrading
    // old games' positions (e.g. "P") into the newer slots (e.g. "RP").
    // This does need to be an Option because the change that made it
    // unnecessary also made it unavailable, but take care to not set
    // it to None out of convenience, because it might lead to runtime
    // errors.
    active_pitcher: BestEffortSlottedPlayer<&'g str>,
    batter_stats: HashMap<&'g str, BatterStats>,
    pitcher_count: i32,
    batter_count: i32,
    batter_subcount: i32,
    advance_to_next_batter: bool,
    has_seen_first_batter: bool,
}

#[derive(Debug, Clone)]
struct RunnerOn<'g> {
    runner_name: &'g str,
    base: TaxaBase,
}

#[derive(Debug, Clone)]
struct GameState<'g> {
    prev_event_type: ParsedEventMessageDiscriminants,
    context: EventContext<'g>,
    home_score: u8,
    away_score: u8,
    inning_number: u8,
    inning_half: TopBottom,
    count_balls: u8,
    count_strikes: u8,
    outs: i32,
    game_finished: bool,
    runners_on: VecDeque<RunnerOn<'g>>,
}

#[derive(Debug)]
pub struct Game<'g> {
    // Does not change
    game_id: &'g str,
    season: i64,
    day: Day,

    // Aggregates
    away: TeamInGame<'g>,
    home: TeamInGame<'g>,

    // Changes all the time
    state: GameState<'g>,
}

#[derive(Debug)]
struct ParsedEventMessageIter<'g: 'a, 'a, IterT: Iterator<Item = &'a ParsedEventMessage<&'g str>>> {
    inner: &'a mut IterT,
    prev_event_type: Option<ParsedEventMessageDiscriminants>,
}

impl<'g: 'a, 'a, IterT> ParsedEventMessageIter<'g, 'a, IterT>
where
    IterT: Iterator<Item = &'a ParsedEventMessage<&'g str>>,
{
    pub fn new(iter: &'a mut IterT) -> Self {
        Self {
            prev_event_type: None,
            inner: iter,
        }
    }

    pub fn next(
        &mut self,
        expected: &'static [ParsedEventMessageDiscriminants],
    ) -> Result<&'a ParsedEventMessage<&'g str>, SimStartupError> {
        match self.inner.next() {
            Some(val) => {
                self.prev_event_type = Some(val.discriminant());
                Ok(val)
            }
            None => match self.prev_event_type {
                None => Err(SimStartupError::NoEvents),
                Some(previous) => Err(SimStartupError::NotEnoughEvents { expected, previous }),
            },
        }
    }
}

// This macro accepts an iterator over game events, and is meant for
// use in Game::new. See game_event! for the equivalent that's meant
// for use in Game::next.
macro_rules! extract_next_game_event {
    // This arm matches when there isn't a trailing comma, adds the
    // trailing comma, and forwards to the other arm
    ($iter:expr, $([$expected:expr] $p:pat => $e:expr,)+) => {
        extract_next_game_event!($iter, $([$expected] $p => $e),+)
    };
    // This arm matches when there is a trailing comma
    ($iter:expr, $([$expected:expr] $p:pat => $e:expr),+) => {{
        let previous = $iter.prev_event_type;
        let expected = &[$($expected,)*];
        match $iter.next(expected)? {
            $($p => Ok($e),)*
            ParsedEventMessage::ParseError { error, .. } => Err(SimStartupError::ParseError {
                source: ParseError(error.clone()),
            }),
            other => Err(SimStartupError::UnexpectedEventType {
                expected,
                previous,
                received: other.discriminant(),
            }),
        }
    }};
}

// This macro accepts a Game and a game event, and is meant for
// use in Game::next. See extract_next_game_event! for the equivalent
// that's meant for use in Game::new.

macro_rules! game_event {
    // This is the main arm, and matches when there is a trailing comma.
    // It needs to be first, otherwise the other two arms will be
    // infinitely mutually recursive.
    (($previous_event:expr, $event:expr), $([$expected:expr] $p:pat => $e:expr,)*) => {{
        // This is wrapped in Some because SimError::UnexpectedEventType
        // takes an Option to handle the case when the error is at the
        // first event (and therefore there is no previous event).
        // However, this macro is only used on a fully-constructed Game,
        // which must have a previous event. So prev_event_type is not
        // an Option, but previous must be.
        let previous: Option<ParsedEventMessageDiscriminants> = Some($previous_event);
        let expected: &[ParsedEventMessageDiscriminants] = &[$($expected,)*];

        match $event {
            $($p => {
                Ok($e)
            })*
            ParsedEventMessage::ParseError { error, .. } => Err(SimEventError::ParseError {
                source: ParseError(error.clone()),
            }),
            other => Err(SimEventError::UnexpectedEventType {
                expected,
                previous,
                received: other.discriminant(),
            }),
        }
    }};
    // This arm matches when there isn't a trailing comma, adds the
    // trailing comma, and forwards to the main arm
    (($previous_event:expr, $event:expr), $([$expected:expr] $p:pat => $e:expr),*) => {
        game_event!(($previous_event, $event), $([$expected] $p => $e,)*)
    };
    // This arm matches when there are no patterns provided and no
    // trailing comma (no patterns with trailing comma is captured by
    // the previous arm)
    (($previous_event:expr, $event:expr)) => {
        game_event!(($previous_event, $event),)
    };
}

fn is_matching_advance<'g>(prev_runner: &RunnerOn<'g>, advance: &RunnerAdvance<&'g str>) -> bool {
    if prev_runner.runner_name != advance.runner {
        // If it's not the same runner, no match
        false
    } else if !(prev_runner.base < advance.base.into()) {
        // If the base they advanced to isn't ahead of the base they started on, no match
        false
    } else {
        true
    }
}

fn is_matching_runner_out<'g>(prev_runner: &RunnerOn<'g>, out: &RunnerOut<&'g str>) -> bool {
    if prev_runner.runner_name != out.runner {
        // If it's not the same runner, no match
        false
    } else if !(prev_runner.base <= out.base.into()) && out.base != BaseNameVariant::Home {
        // If the base they got out at to is behind the base they started on, no match
        false
    } else {
        true
    }
}

fn is_matching_steal<'g>(prev_runner: &RunnerOn<'g>, steal: &BaseSteal<&'g str>) -> bool {
    if prev_runner.runner_name != steal.runner {
        // If it's not the same runner, no match
        false
    } else if !(prev_runner.base < steal.base.into()) && steal.base != Base::Home {
        // If the base they advanced to isn't ahead of the base they started on, no match
        // This could be restricted to the very next base but I don't think that's necessary
        false
    } else {
        true
    }
}

struct EventDetailBuilder<'g> {
    raw_event: &'g mmolb_parsing::game::Event,
    prev_game_state: GameState<'g>,
    game_event_index: usize,
    fair_ball_event_index: Option<usize>,
    pitcher: BestEffortSlottedPlayer<&'g str>,
    fair_ball_type: Option<TaxaFairBallType>,
    fair_ball_direction: Option<TaxaFielderLocation>,
    hit_base: Option<TaxaBase>,
    fielding_error_type: Option<TaxaFieldingErrorType>,
    pitch: Option<Pitch>,
    described_as_sacrifice: Option<bool>,
    is_toasty: Option<bool>,
    fielders: Vec<EventDetailFielder<&'g str>>,
    advances: Vec<RunnerAdvance<&'g str>>,
    scores: Vec<&'g str>,
    steals: Vec<BaseSteal<&'g str>>,
    runner_added: Option<(&'g str, TaxaBase)>,
    runners_out: Vec<RunnerOut<&'g str>>,
    batter_scores: bool,
}

impl<'g> EventDetailBuilder<'g> {
    fn fair_ball(mut self, fair_ball: FairBall) -> Self {
        self = self.pitch(fair_ball.pitch);
        self.fair_ball_event_index = Some(fair_ball.game_event_index);
        self.fair_ball_type = Some(fair_ball.fair_ball_type.into());
        self.fair_ball_direction = Some(fair_ball.fair_ball_destination.into());
        self
    }

    fn hit_base(mut self, base: TaxaBase) -> Self {
        self.hit_base = Some(base);
        self
    }

    fn fielding_error_type(mut self, fielding_error_type: TaxaFieldingErrorType) -> Self {
        self.fielding_error_type = Some(fielding_error_type);
        self
    }

    //noinspection RsSelfConvention
    fn described_as_sacrifice(mut self, described_as_sacrifice: bool) -> Self {
        self.described_as_sacrifice = Some(described_as_sacrifice);
        self
    }

    //noinspection RsSelfConvention
    fn is_toasty(mut self, is_toasty: bool) -> Self {
        self.is_toasty = Some(is_toasty);
        self
    }

    fn placed_player_slot(
        &self,
        player: PlacedPlayer<&'g str>,
        ingest_logs: &mut IngestLogs,
    ) -> Result<TaxaSlot, SimEventError> {
        Ok(match player.place {
            Place::Pitcher => {
                if self.pitcher.name != player.name {
                    ingest_logs.info(format!(
                        "Event pitcher name ({}) does not match our stored pitcher's name ({}). The \
                        only known cause of this mismatch is an augment firing during a game, in which \
                        case the active pitcher's position is still correct. Therefore this is not a \
                        warning.",
                        player.name, self.pitcher.name,
                    ));
                }
                self.pitcher.slot.into()
            }
            Place::Catcher => TaxaSlot::Catcher,
            Place::FirstBaseman => TaxaSlot::FirstBase,
            Place::SecondBaseman => TaxaSlot::SecondBase,
            Place::ThirdBaseman => TaxaSlot::ThirdBase,
            Place::ShortStop => TaxaSlot::Shortstop,
            Place::LeftField => TaxaSlot::LeftField,
            Place::CenterField => TaxaSlot::CenterField,
            Place::RightField => TaxaSlot::RightField,
            Place::StartingPitcher(None) => TaxaSlot::StartingPitcher,
            Place::StartingPitcher(Some(1)) => TaxaSlot::StartingPitcher1,
            Place::StartingPitcher(Some(2)) => TaxaSlot::StartingPitcher2,
            Place::StartingPitcher(Some(3)) => TaxaSlot::StartingPitcher3,
            Place::StartingPitcher(Some(4)) => TaxaSlot::StartingPitcher4,
            Place::StartingPitcher(Some(5)) => TaxaSlot::StartingPitcher5,
            Place::StartingPitcher(Some(other)) => {
                ingest_logs.warn(format!(
                    "Unexpected starting pitcher number: {other} (expected 1-5). Falling back to \
                    un-numbered starting pitcher type.",
                ));
                TaxaSlot::StartingPitcher
            }
            Place::ReliefPitcher(None) => TaxaSlot::ReliefPitcher,
            Place::ReliefPitcher(Some(1)) => TaxaSlot::ReliefPitcher1,
            Place::ReliefPitcher(Some(2)) => TaxaSlot::ReliefPitcher2,
            Place::ReliefPitcher(Some(3)) => TaxaSlot::ReliefPitcher3,
            Place::ReliefPitcher(Some(other)) => {
                ingest_logs.warn(format!(
                    "Unexpected relief pitcher number: {other} (expected 1-3). Falling back to \
                    un-numbered relief pitcher type.",
                ));
                TaxaSlot::ReliefPitcher
            }
            Place::Closer => TaxaSlot::Closer,
            Place::DesignatedHitter => TaxaSlot::DesignatedHitter,
        })
    }

    fn fielder(
        mut self,
        fielder: PlacedPlayer<&'g str>,
        ingest_logs: &mut IngestLogs,
    ) -> Result<Self, SimEventError> {
        if !self.fielders.is_empty() {
            warn!("EventDetailBuilder overwrote existing fielders");
        }

        self.fielders = vec![EventDetailFielder {
            name: fielder.name,
            slot: self.placed_player_slot(fielder, ingest_logs)?,
        }];

        Ok(self)
    }

    fn fielders(
        mut self,
        fielders: impl IntoIterator<Item = PlacedPlayer<&'g str>>,
        ingest_logs: &mut IngestLogs,
    ) -> Result<Self, SimEventError> {
        if !self.fielders.is_empty() {
            warn!("EventDetailBuilder overwrote existing fielders");
        }

        self.fielders = fielders
            .into_iter()
            .map(|f| {
                self.placed_player_slot(f, ingest_logs)
                    .map(|slot| EventDetailFielder { name: f.name, slot })
            })
            .collect::<Result<_, _>>()?;

        Ok(self)
    }

    fn set_batter_scores(mut self) -> Self {
        self.batter_scores = true;
        self
    }

    fn runner_changes(
        mut self,
        advances: Vec<RunnerAdvance<&'g str>>,
        scores: Vec<&'g str>,
    ) -> Self {
        if !self.advances.is_empty() {
            warn!("EventDetailBuilder overwrote existing advances");
        }

        if !self.scores.is_empty() {
            warn!("EventDetailBuilder overwrote existing scores");
        }

        if !self.steals.is_empty() {
            warn!("Called runner_changes() and steals() on the same EventDetailBuilder");
        }

        self.advances = advances;
        self.scores = scores;
        self
    }

    fn steals(mut self, steals: Vec<BaseSteal<&'g str>>) -> Self {
        if !self.advances.is_empty() {
            warn!("Called runner_changes() and steals() on the same EventDetailBuilder");
        }

        if !self.scores.is_empty() {
            warn!("Called runner_changes() and steals() on the same EventDetailBuilder");
        }

        if !self.steals.is_empty() {
            warn!("EventDetailBuilder overwrote existing steals");
        }

        self.steals = steals;
        self
    }

    fn add_runner(mut self, runner_name: &'g str, to_base: TaxaBase) -> Self {
        self.runner_added = Some((runner_name, to_base));
        self
    }

    fn add_out(mut self, runner_out: RunnerOut<&'g str>) -> Self {
        self.runners_out.push(runner_out);
        self
    }

    fn pitch(mut self, pitch: Option<Pitch>) -> Self {
        self.pitch = pitch;
        self
    }

    pub fn build_some(
        self,
        game: &Game<'g>,
        batter_name: &'g str,
        ingest_logs: &mut IngestLogs,
        type_detail: TaxaEventType,
    ) -> Option<EventDetail<&'g str>> {
        Some(self.build(game, ingest_logs, type_detail, batter_name))
    }

    pub fn build(
        self,
        game: &Game<'g>,
        ingest_logs: &mut IngestLogs,
        type_detail: TaxaEventType,
        // Note: this *cannot* be replaced with the batter name from the
        // RawEvent, because that name is sometimes incorrect following a
        // player Retirement. See game 687464a0336fee7c9e05b0f6 event 70.
        batter_name: &'g str,
    ) -> EventDetail<&'g str> {
        // Note: game.state.runners_on gets cleared if this event is an
        // inning-ending out. As of writing this comment the code below
        // doesn't use game.state.runners_on at all, but if it is used
        // in the future keep that in mind.
        let mut scores = self.scores.into_iter();
        let mut advances = self.advances.into_iter().peekable();
        let mut runners_out = self.runners_out.into_iter().peekable();
        let mut steals = self.steals.into_iter().peekable();

        let advances_ref = &mut advances;
        let runners_out_ref = &mut runners_out;
        let mut baserunners: Vec<_> = self
            .prev_game_state
            .runners_on
            .into_iter()
            .map(|prev_runner| {
                if let Some(steal) = steals.peeking_next(|s| is_matching_steal(&prev_runner, s)) {
                    // There can't be steals and scorers in the same event so we're safe to do this first
                    EventDetailRunner {
                        name: prev_runner.runner_name,
                        base_before: Some(prev_runner.base),
                        base_after: steal.base.into(),
                        is_out: steal.caught,
                        base_description_format: None,
                        is_steal: true,
                    }
                } else if let Some(_scorer_name) = {
                    // Tapping into the if-else chain so I can do some
                    // processing between the call to .next() and the
                    // `if let` match
                    if let Some(scorer_name) = scores.next() {
                        // If there are any scores left, they MUST be in runner order.
                        if scorer_name != prev_runner.runner_name {
                            ingest_logs.error(format!(
                                "Runner {scorer_name} scored, but the farthest runner was {}. \
                                Ignoring the score.",
                                prev_runner.runner_name,
                            ));
                            None
                        } else {
                            Some(scorer_name)
                        }
                    } else {
                        None
                    }
                } {
                    EventDetailRunner {
                        name: prev_runner.runner_name,
                        base_before: Some(prev_runner.base),
                        base_after: TaxaBase::Home,
                        is_out: false,
                        base_description_format: None,
                        is_steal: false,
                    }
                } else if let Some(advance) =
                    advances_ref.peeking_next(|a| is_matching_advance(&prev_runner, a))
                {
                    // If the runner didn't score, they may have advanced
                    EventDetailRunner {
                        name: prev_runner.runner_name,
                        base_before: Some(prev_runner.base),
                        base_after: advance.base.into(),
                        is_out: false,
                        base_description_format: None,
                        is_steal: false,
                    }
                } else if let Some(out) =
                    runners_out_ref.peeking_next(|o| is_matching_runner_out(&prev_runner, o))
                {
                    // If the runner didn't score or advance, they may have gotten out
                    EventDetailRunner {
                        name: prev_runner.runner_name,
                        base_before: Some(prev_runner.base),
                        base_after: out.base.into(),
                        is_out: true,
                        base_description_format: Some(out.base.into()),
                        is_steal: false,
                    }
                } else {
                    // If the runner didn't score, advance, or get out they just stayed on base
                    EventDetailRunner {
                        name: prev_runner.runner_name,
                        base_before: Some(prev_runner.base),
                        base_after: prev_runner.base,
                        is_out: false,
                        base_description_format: None,
                        is_steal: false,
                    }
                }
            })
            .chain(
                // Add runners who made it to base this event
                self.runner_added
                    .into_iter()
                    .map(|(name, base)| EventDetailRunner {
                        name,
                        base_before: None,
                        base_after: base,
                        is_out: false,
                        base_description_format: None,
                        is_steal: false,
                    }),
            )
            .collect();

        // Semantically I want this to be a .chain(), but it has to be .extend() for lifetime
        // reasons
        baserunners.extend(
            // The remaining runners out should be batter-runners who got out this event, but
            // who have a baserunner entry because we need to record which base they got out
            // at. This should be mutually exclusive with runners_added, so their relative
            // order doesn't matter.
            runners_out_ref.map(|out| {
                if out.runner != batter_name {
                    ingest_logs.warn(format!(
                        "Got a batter-runner entry in `baserunners` that has the wrong name \
                        ({}, expected {})",
                        out.runner, batter_name,
                    ));
                }

                EventDetailRunner {
                    name: out.runner,
                    base_before: None,
                    base_after: out.base.into(),
                    is_out: true,
                    base_description_format: Some(out.base.into()),
                    is_steal: false,
                }
            }),
        );

        // Another thing that could be a chain but for lifetimes
        baserunners.extend(
            // There can be "advances" that put a runner on base
            advances_ref.map(|advance| {
                if advance.runner != batter_name {
                    ingest_logs.warn(format!(
                        "Got a stray advance ({}) that doesn't match the batter name ({})",
                        advance.runner, batter_name,
                    ));
                }

                EventDetailRunner {
                    name: advance.runner,
                    base_before: None,
                    base_after: advance.base.into(),
                    is_out: false,
                    base_description_format: None,
                    is_steal: false,
                }
            }),
        );

        // Finally, on a home run the batter scores
        if self.batter_scores {
            baserunners.push(EventDetailRunner {
                name: batter_name,
                base_before: None,
                base_after: TaxaBase::Home,
                is_out: false,
                base_description_format: None,
                is_steal: false,
            })
        }

        // Check that we processed every change to existing runners
        let extra_steals = steals.collect::<Vec<_>>();
        if !extra_steals.is_empty() {
            ingest_logs.error(format!("Stealing runner(s) not found: {:?}", extra_steals));
        }
        let extra_scores = scores.collect::<Vec<_>>();
        if !extra_scores.is_empty() {
            ingest_logs.error(format!("Scoring runner(s) not found: {:?}", extra_scores));
        }
        let extra_advances = advances.collect::<Vec<_>>();
        if !extra_advances.is_empty() {
            ingest_logs.error(format!(
                "Advancing runner(s) not found: {:?}",
                extra_advances
            ));
        }
        let extra_runners_out = runners_out.collect::<Vec<_>>();
        if !extra_runners_out.is_empty() {
            ingest_logs.error(format!("Runner(s) out not found: {:?}", extra_runners_out));
        }

        EventDetail {
            game_event_index: self.game_event_index,
            fair_ball_event_index: self.fair_ball_event_index,
            inning: game.state.inning_number,
            top_of_inning: game.state.inning_half.is_top(),
            balls_before: self.prev_game_state.count_balls,
            strikes_before: self.prev_game_state.count_strikes,
            outs_before: self.prev_game_state.outs,
            outs_after: game.state.outs,
            away_team_score_before: self.prev_game_state.away_score,
            away_team_score_after: game.state.away_score,
            home_team_score_before: self.prev_game_state.home_score,
            home_team_score_after: game.state.home_score,
            batter_name,
            pitcher_name: if let MaybePlayer::Player(pitcher) = &self.raw_event.pitcher {
                pitcher
            } else {
                // TODO Correct error handling
                panic!("Must have a pitcher when building an EventDetail");
            },
            fielders: self.fielders,
            detail_type: type_detail,
            hit_base: self.hit_base,
            fair_ball_type: self.fair_ball_type,
            fair_ball_direction: self.fair_ball_direction,
            fielding_error_type: self.fielding_error_type,
            pitch_type: self.pitch.map(|pitch| pitch.pitch_type.into()),
            pitch_speed: self.pitch.map(|pitch| pitch.pitch_speed as f64),
            pitch_zone: self.pitch.map(|pitch| pitch.pitch_zone as i32),
            described_as_sacrifice: self.described_as_sacrifice,
            is_toasty: self.is_toasty,
            baserunners,
            pitcher_count: game.defending_team().pitcher_count,
            batter_count: game.batting_team().batter_count,
            batter_subcount: game.batting_team().batter_subcount,
        }
    }
}

// This is only used as a structured way to pass parameters into Game::update_runners
#[derive(Debug, Default)]
struct RunnerUpdate<'g, 'a> {
    pub steals: &'a [BaseSteal<&'g str>],
    pub scores: &'a [&'g str],
    pub advances: &'a [RunnerAdvance<&'g str>],
    pub runners_out: &'a [RunnerOut<&'g str>],
    pub runners_out_may_include_batter: Option<&'g str>,
    pub runner_added: Option<(&'g str, TaxaBase)>,
    pub runner_added_forces_advances: bool,
    pub runner_advances_may_include_batter: Option<&'g str>,
}

// Returns true for events that happen in the ExpectPitch context but
// don't have an associated pitch
fn is_pitchless_pitch(ty: ParsedEventMessageDiscriminants) -> bool {
    ty == ParsedEventMessageDiscriminants::MoundVisit
        || ty == ParsedEventMessageDiscriminants::Balk
        || ty == ParsedEventMessageDiscriminants::FallingStar
}

impl<'g> Game<'g> {
    pub fn new<'a, IterT>(
        game_id: &'g str,
        game_data: &'g mmolb_parsing::Game,
        events: &'a mut IterT,
    ) -> Result<(Game<'g>, Vec<Vec<IngestLog>>), SimStartupError>
    where
        'g: 'a,
        IterT: Iterator<Item = &'a ParsedEventMessage<&'g str>>,
    {
        let mut events = ParsedEventMessageIter::new(events);
        let mut ingest_logs = Vec::new();

        // TODO Double check correctness of game_event_index
        let mut game_event_index = 0;
        let (away_team_name, away_team_emoji, home_team_name, home_team_emoji) = extract_next_game_event!(
            events,
            [ParsedEventMessageDiscriminants::LiveNow]
            ParsedEventMessage::LiveNow {
                away_team,
                home_team,
            } => (
                away_team.name,
                away_team.emoji,
                home_team.name,
                home_team.emoji,
            )
        )?;

        ingest_logs.push({
            let mut logs = IngestLogs::new(game_event_index);
            logs.debug(format!(
                "Set home team to name: \"{home_team_name}\", emoji: \"{home_team_emoji}\""
            ));
            logs.debug(format!(
                "Set away team to name: \"{away_team_name}\", emoji: \"{away_team_emoji}\""
            ));
            logs.into_vec()
        });

        game_event_index += 1;
        let (
            home_pitcher_name,
            away_pitcher_name,
            away_team_name_2,
            away_team_emoji_2,
            home_team_name_2,
            home_team_emoji_2,
        ) = extract_next_game_event!(
            events,
            [ParsedEventMessageDiscriminants::PitchingMatchup]
            ParsedEventMessage::PitchingMatchup {
                home_pitcher,
                away_pitcher,
                away_team,
                home_team,
            } => (
                home_pitcher,
                away_pitcher,
                away_team.name,
                away_team.emoji,
                home_team.name,
                home_team.emoji,
            )
        )?;
        let mut event_ingest_logs = IngestLogs::new(game_event_index);
        if away_team_name_2 != away_team_name {
            event_ingest_logs.warn(format!(
                "Away team name from PitchingMatchup ({away_team_name_2}) did \
                not match the one from LiveNow ({away_team_name})"
            ));
        }
        if away_team_emoji_2 != away_team_emoji {
            event_ingest_logs.warn(format!(
                "Away team emoji from PitchingMatchup ({away_team_emoji_2}) did \
                not match the one from LiveNow ({away_team_emoji})"
            ));
        }
        if home_team_name_2 != home_team_name {
            event_ingest_logs.warn(format!(
                "Home team name from PitchingMatchup ({home_team_name_2}) did \
                not match the one from LiveNow ({home_team_name})"
            ));
        }
        if home_team_emoji_2 != home_team_emoji {
            event_ingest_logs.warn(format!(
                "Home team emoji from PitchingMatchup ({home_team_emoji_2}) did \
                not match the one from LiveNow ({home_team_emoji})"
            ));
        }
        event_ingest_logs.debug(format!(
            "Set home team pitcher to name: \"{home_pitcher_name}\""
        ));
        event_ingest_logs.debug(format!(
            "Set away team pitcher to name: \"{away_pitcher_name}\""
        ));
        ingest_logs.push(event_ingest_logs.into_vec());

        game_event_index += 1;
        let away_lineup = extract_next_game_event!(
            events,
            [ParsedEventMessageDiscriminants::Lineup]
            ParsedEventMessage::Lineup { side: HomeAway::Away, players } => players
        )?;
        ingest_logs.push({
            let mut logs = IngestLogs::new(game_event_index);
            logs.debug(format!(
                "Set away lineup to: {}",
                format_lineup(&away_lineup)
            ));
            logs.into_vec()
        });
        let away_batter_stats = away_lineup
            .iter()
            .map(|player| (player.name, BatterStats::new()))
            .collect();

        game_event_index += 1;
        let home_lineup = extract_next_game_event!(
            events,
            [ParsedEventMessageDiscriminants::Lineup]
            ParsedEventMessage::Lineup { side: HomeAway::Home, players } => players
        )?;
        ingest_logs.push({
            let mut logs = IngestLogs::new(game_event_index);
            logs.debug(format!(
                "Set home lineup to: {}",
                format_lineup(&home_lineup)
            ));
            logs.into_vec()
        });
        let home_batter_stats = home_lineup
            .iter()
            .map(|player| (player.name, BatterStats::new()))
            .collect();

        game_event_index += 1;
        extract_next_game_event!(
            events,
            [ParsedEventMessageDiscriminants::PlayBall]
            ParsedEventMessage::PlayBall => ()
        )?;
        ingest_logs.push(Vec::new());

        let game = Self {
            game_id,
            season: game_data.season.into(),
            day: match &game_data.day {
                Ok(day) => *day,
                Err(error) => {
                    return Err(SimStartupError::FailedToParseGameDay {
                        source: error.clone(),
                    });
                }
            },
            away: TeamInGame {
                team_name: away_team_name,
                team_emoji: away_team_emoji,
                automatic_runner: None,
                active_pitcher: BestEffortSlottedPlayer {
                    name: away_pitcher_name,
                    slot: game_data.away_sp.parse().map_err(|_| {
                        SimStartupError::FailedToParseStartingPitcher(game_data.away_sp.clone())
                    })?,
                },
                batter_stats: away_batter_stats,
                pitcher_count: 0,
                batter_count: 0,
                batter_subcount: 0,
                advance_to_next_batter: false,
                has_seen_first_batter: false,
            },
            home: TeamInGame {
                team_name: home_team_name,
                team_emoji: home_team_emoji,
                automatic_runner: None,
                active_pitcher: BestEffortSlottedPlayer {
                    name: home_pitcher_name,
                    slot: game_data.home_sp.parse().map_err(|_| {
                        SimStartupError::FailedToParseStartingPitcher(game_data.home_sp.clone())
                    })?,
                },
                batter_stats: home_batter_stats,
                pitcher_count: 0,
                batter_count: 0,
                batter_subcount: 0,
                advance_to_next_batter: false,
                has_seen_first_batter: false,
            },
            state: GameState {
                prev_event_type: ParsedEventMessageDiscriminants::PlayBall,
                context: EventContext::ExpectInningStart,
                home_score: 0,
                away_score: 0,
                inning_number: 0,
                inning_half: TopBottom::Bottom,
                count_balls: 0,
                count_strikes: 0,
                outs: 0,
                game_finished: false,
                runners_on: Default::default(),
            },
        };
        Ok((game, ingest_logs))
    }

    pub fn automatic_runner_rule_is_active(&self) -> bool {
        let day_threshold = if self.season == 0 {
            // s0 was 120 days of every team playing every day
            120
        } else {
            // s1 and onwards will be 240 days of teams playing every even (lesser league) or odd
            // (greater league) day
            240
        };

        // Superstar game does have the automatic runner rule as confirmed by Danny.
        // The other special types of day shouldn't have any games.
        match self.day {
            Day::SuperstarBreak => true,
            Day::Holiday => true,
            Day::Day(day) => day <= day_threshold,
            Day::SuperstarDay(_) => true,
            Day::PostseasonRound(_) => false,
            Day::PostseasonPreview => true,
            Day::Preseason => true,
            Day::Election => true,
        }
    }

    fn batting_team(&self) -> &TeamInGame<'g> {
        match self.state.inning_half {
            TopBottom::Top => &self.away,
            TopBottom::Bottom => &self.home,
        }
    }

    fn batting_team_mut(&mut self) -> &mut TeamInGame<'g> {
        match self.state.inning_half {
            TopBottom::Top => &mut self.away,
            TopBottom::Bottom => &mut self.home,
        }
    }

    fn defending_team(&self) -> &TeamInGame<'g> {
        match self.state.inning_half {
            TopBottom::Top => &self.home,
            TopBottom::Bottom => &self.away,
        }
    }

    fn defending_team_mut(&mut self) -> &mut TeamInGame<'g> {
        match self.state.inning_half {
            TopBottom::Top => &mut self.home,
            TopBottom::Bottom => &mut self.away,
        }
    }

    fn check_count(&self, (balls, strikes): (u8, u8), ingest_logs: &mut IngestLogs) {
        if self.state.count_balls != balls {
            ingest_logs.warn(format!(
                "Unexpected number of balls: expected {}, but saw {balls}",
                self.state.count_balls
            ));
        }
        if self.state.count_strikes != strikes {
            ingest_logs.warn(format!(
                "Unexpected number of strikes: expected {}, but saw {strikes}",
                self.state.count_strikes
            ));
        }
    }

    fn active_automatic_runner(&self) -> Option<&'g str> {
        match self.state.inning_half {
            TopBottom::Top => self.away.automatic_runner,
            TopBottom::Bottom => self.home.automatic_runner,
        }
    }

    fn active_automatic_runner_mut(&mut self) -> &mut Option<&'g str> {
        match self.state.inning_half {
            TopBottom::Top => &mut self.away.automatic_runner,
            TopBottom::Bottom => &mut self.home.automatic_runner,
        }
    }

    fn batter_stats(&self, batter_name: &'g str) -> Option<&BatterStats> {
        match self.state.inning_half {
            TopBottom::Top => self.away.batter_stats.get(batter_name),
            TopBottom::Bottom => self.home.batter_stats.get(batter_name),
        }
    }

    fn batter_stats_mut(&mut self, batter_name: &'g str) -> &mut BatterStats {
        let entry = match self.state.inning_half {
            TopBottom::Top => self.away.batter_stats.entry(batter_name),
            TopBottom::Bottom => self.home.batter_stats.entry(batter_name),
        };

        entry.or_default()
    }

    fn check_batter(
        &self,
        expected_batter_name: &str,
        observed_batter_name: &str,
        ingest_logs: &mut IngestLogs,
    ) {
        if expected_batter_name != observed_batter_name {
            ingest_logs.warn(format!(
                "Unexpected batter name: Expected {}, but saw {}",
                expected_batter_name, observed_batter_name,
            ));
        }
    }

    fn check_pitcher(&self, observed_pitcher_name: &str, ingest_logs: &mut IngestLogs) {
        if self.defending_team().active_pitcher.name != observed_pitcher_name {
            ingest_logs.warn(format!(
                "Unexpected pitcher name: Expected {}, but saw {}",
                self.defending_team().active_pitcher.name,
                observed_pitcher_name,
            ));
        }
    }

    fn check_fair_ball_type(
        &self,
        fair_ball_from_previous_event: &FairBall,
        fair_ball_type_from_this_event: FairBallType,
        ingest_logs: &mut IngestLogs,
    ) {
        if fair_ball_from_previous_event.fair_ball_type != fair_ball_type_from_this_event {
            ingest_logs.warn(format!(
                "Mismatched fair ball type: expected {} but saw {}",
                fair_ball_from_previous_event.fair_ball_type, fair_ball_type_from_this_event,
            ));
        }
    }

    fn check_fair_ball_destination(
        &self,
        fair_ball_from_previous_event: &FairBall,
        fair_ball_destination_from_this_event: FairBallDestination,
        ingest_logs: &mut IngestLogs,
    ) {
        if fair_ball_from_previous_event.fair_ball_destination
            != fair_ball_destination_from_this_event
        {
            ingest_logs.warn(format!(
                "Mismatched fair ball destination: expected {} but saw {}",
                fair_ball_from_previous_event.fair_ball_destination,
                fair_ball_destination_from_this_event,
            ));
        }
    }

    fn detail_builder(
        &self,
        prev_game_state: GameState<'g>,
        game_event_index: usize,
        raw_event: &'g mmolb_parsing::game::Event,
    ) -> EventDetailBuilder<'g> {
        EventDetailBuilder {
            raw_event,
            prev_game_state,
            fair_ball_event_index: None,
            game_event_index,
            pitcher: self.defending_team().active_pitcher,
            fielders: Vec::new(),
            advances: Vec::new(),
            hit_base: None,
            fair_ball_type: None,
            fair_ball_direction: None,
            fielding_error_type: None,
            pitch: None,
            described_as_sacrifice: None,
            is_toasty: None,
            scores: Vec::new(),
            steals: Vec::new(),
            runner_added: None,
            runners_out: Vec::new(),
            batter_scores: false,
        }
    }

    // Note: Must happen after all outs for this event are added
    pub fn finish_pa(&mut self, batter_name: &'g str) {
        // Automatic runner is the most recent runner to have finished a PA
        *self.active_automatic_runner_mut() = Some(batter_name);

        // Occam's razor: assume "at bats" is actually PAs until proven
        // otherwise
        self.batter_stats_mut(batter_name).at_bats += 1;

        self.state.count_strikes = 0;
        self.state.count_balls = 0;
        self.batting_team_mut().advance_to_next_batter = true;

        if self.is_walkoff() {
            // If it's the bottom of a 9th or later, and the score is
            // now in favor of the home team, it's a walk-off
            self.end_game();
        } else if self.state.outs >= 3 {
            // Otherwise, if there's 3 outs, the inning ends
            self.state.context = EventContext::ExpectInningEnd;
        } else {
            // Otherwise just go to the next batter
            self.state.context = EventContext::ExpectNowBatting;
        }
    }

    fn end_game(&mut self) {
        self.state.runners_on.clear();
        self.state.game_finished = true;
        self.state.context = EventContext::ExpectGameEnd;
    }

    fn is_walkoff(&self) -> bool {
        self.state.inning_number >= 9
            && self.state.inning_half == TopBottom::Bottom
            && self.state.home_score > self.state.away_score
    }

    pub fn add_outs(&mut self, num_outs: i32) {
        self.state.outs += num_outs;

        // This is usually redundant with finish_pa, but not in the case of inning-ending
        // caught stealing
        if self.state.outs >= 3 {
            self.state.context = EventContext::ExpectInningEnd;
            self.state.runners_on.clear();
        }
    }

    pub fn add_out(&mut self) {
        self.add_outs(1)
    }

    fn add_runs_to_batting_team(&mut self, runs: u8) {
        match self.state.inning_half {
            TopBottom::Top => {
                self.state.away_score += runs;
            }
            TopBottom::Bottom => {
                self.state.home_score += runs;
            }
        }

        if self.is_walkoff() {
            self.end_game();
        }
    }

    fn check_baserunner_consistency(
        &self,
        raw_event: &mmolb_parsing::game::Event,
        ingest_logs: &mut IngestLogs,
    ) {
        self.check_internal_baserunner_consistency(ingest_logs);

        let mut on_1b = false;
        let mut on_2b = false;
        let mut on_3b = false;

        for runner in &self.state.runners_on {
            match runner.base {
                TaxaBase::Home => {}
                TaxaBase::First => on_1b = true,
                TaxaBase::Second => on_2b = true,
                TaxaBase::Third => on_3b = true,
            }
        }

        fn test_on_base(
            log: &mut IngestLogs,
            which_base: &str,
            expected_value: bool,
            value_from_mmolb: bool,
        ) {
            if value_from_mmolb && !expected_value {
                log.error(format!(
                    "Observed a runner on {which_base} but we expected it to be empty"
                ));
            } else if !value_from_mmolb && expected_value {
                log.error(format!(
                    "Expected a runner on {which_base} but observed it to be empty"
                ));
            }
        }
        test_on_base(ingest_logs, "first", on_1b, raw_event.on_1b);
        test_on_base(ingest_logs, "second", on_2b, raw_event.on_2b);
        test_on_base(ingest_logs, "third", on_3b, raw_event.on_3b);
    }

    fn check_internal_baserunner_consistency(&self, ingest_logs: &mut IngestLogs) {
        if !self
            .state
            .runners_on
            .iter()
            .is_sorted_by(|a, b| a.base > b.base)
        {
            ingest_logs.error(format!(
                "Runners on base list was not sorted descending by base: {:?}",
                self.state.runners_on
            ));
        }

        if self.state.runners_on.iter().unique_by(|r| r.base).count() != self.state.runners_on.len()
        {
            ingest_logs.error(format!(
                "Runners on base list has multiple runners on the same base: {:?}",
                self.state.runners_on
            ));
        }

        if self
            .state
            .runners_on
            .iter()
            .any(|r| r.base == TaxaBase::Home)
        {
            ingest_logs.error(format!(
                "Runners on base list has a runner on Home: {:?}",
                self.state.runners_on
            ));
        }
    }

    fn update_runners(&mut self, updates: RunnerUpdate<'g, '_>, ingest_logs: &mut IngestLogs) {
        // For borrow checker reasons, we can't add runs as we go.
        // Instead, accumulate them here and add them at the end.
        let mut runs_to_add = 0;
        // Same applies to outs
        let mut outs_to_add = 0;

        let n_runners_on_before = self.state.runners_on.len();
        let n_caught_stealing = updates.steals.iter().filter(|steal| steal.caught).count();
        let n_stole_home = updates
            .steals
            .iter()
            .filter(|steal| !steal.caught && steal.base == Base::Home)
            .count();
        let n_scored = updates.scores.len();
        let n_runners_out = updates.runners_out.len();

        let mut scores_iter = updates.scores.iter().peekable();
        let mut steals_iter = updates.steals.iter().peekable();
        let mut advances_iter = updates.advances.iter().peekable();
        let mut runners_out_iter = updates.runners_out.iter().peekable();

        // Checking for eligible advances requires knowing which base
        // ahead of you is occupied
        let mut last_occupied_base = None;

        self.state.runners_on.retain_mut(|runner| {
            // Consistency check
            if last_occupied_base == Some(TaxaBase::Home) {
                ingest_logs.error(format!(
                    "When processing {} (on {:#?}), the previous occupied base was Home",
                    runner.runner_name, runner.base
                ));
            }

            // Runners can only score if there is no one ahead of them
            if last_occupied_base == None {
                if let Some(_) = scores_iter.peeking_next(|n| **n == runner.runner_name) {
                    // Then this is a score, and the runner should
                    // score a run and be removed from base.
                    runs_to_add += 1;
                    ingest_logs.debug(format!("{} scored", runner.runner_name));
                    return false;
                }
            }

            // Next, check if this is a steal. I don't think steals
            // ever happen on an event that has any other runner
            // updates, so the ordering doesn't matter (until it
            // does). But it's easier to reason about the logic
            // around in_scoring_phase if it's processed after
            // scores.
            if let Some(steal) = steals_iter.peeking_next(|s| {
                // A steal is eligible if the name matches and the base
                // they tried to steal is the one after the one they're
                // currently on. I tried to leave this open to a runner
                // stealing multiple bases in one attempt, in case that
                // ever gets added, but it turned out it has to be that
                // restrictive to properly handle the case when there's
                // multiple same-named baserunners on the basepaths.
                s.runner == runner.runner_name && s.base == runner.base.next_base().into()
            }) {
                return if steal.caught {
                    // Caught out: Add an out and remove the runner
                    outs_to_add += 1;
                    ingest_logs.debug(format!("{} caught stealing", runner.runner_name));
                    false
                } else if steal.base == Base::Home {
                    // Stole home: Add a run and remove the runner
                    ingest_logs.debug(format!("{} stole home", runner.runner_name));
                    runs_to_add += 1;
                    false
                } else {
                    // Stole any other base: Update the runner and
                    // retain them, also updating the last occupied
                    // base
                    ingest_logs.debug(format!("{} stole {}", runner.runner_name, steal.base));
                    runner.base = steal.base.into();
                    last_occupied_base = Some(runner.base);
                    true
                };
            }

            // Next, look for advances
            if let Some(advance) = advances_iter.peeking_next(|a| {
                // An advance is eligible if the name matches and
                // the next occupied base is later than the one
                // they advanced to and the base they're advancing
                // to is after the one they're at
                a.runner == runner.runner_name
                    && runner.base < a.base.into()
                    && last_occupied_base
                        .map_or(true, |occupied_base| occupied_base > a.base.into())
            }) {
                // For an advance, the only thing necessary is to
                // update the runner's base and the last occupied
                // base, then retain the runner
                ingest_logs.debug(format!(
                    "{} advanced from {} to {}",
                    runner.runner_name, runner.base, advance.base
                ));
                runner.base = advance.base.into();
                last_occupied_base = Some(runner.base);
                return true;
            }

            // Next, look for outs
            if let Some(out) = runners_out_iter.peeking_next(|o| {
                // A runner-out is eligible if the name matches and
                // all bases behind the one they got out at are
                // clear. The one they got out at may be occupied.
                // This translates to a >= condition compared to
                // the > condition for other tests.
                if o.runner != runner.runner_name {
                    return false;
                }

                if let Some(occupied_base) = last_occupied_base {
                    if occupied_base >= o.base.into() {
                        true
                    } else {
                        false
                    }
                } else {
                    true
                }
                // o.runner == runner.runner_name && last_occupied_base.map_or(true, |occupied_base| occupied_base >= o.base.into())
            }) {
                // Every runner out is an out (obviously), and
                // removes the runner from the bases
                ingest_logs.debug(format!("{} out at {}", runner.runner_name, out.base));
                outs_to_add += 1;
                return false;
            }

            // If none of the above applies, the runner must not have moved
            last_occupied_base = Some(runner.base);
            ingest_logs.debug(format!(
                "{} didn't move from {}",
                runner.runner_name, runner.base
            ));
            true
        });

        // If the batter may be one of the outs, try to pop it from the
        // outs iterator.
        let mut batter_out = 0;
        if let Some(batter_name) = updates.runners_out_may_include_batter {
            // TODO Clean up difference between this and the version inside retain_mut()
            if let Some(_) = runners_out_iter.peeking_next(|o| {
                // A runner-out is eligible if the name matches and
                // all bases behind the one they got out at are
                // clear. The one they got out at may be occupied.
                // This translates to a >= condition compared to
                // the > condition for other tests.
                if o.runner != batter_name {
                    return false;
                }

                if let Some(occupied_base) = last_occupied_base {
                    if occupied_base >= o.base.into() {
                        true
                    } else {
                        false
                    }
                } else {
                    true
                }
                // o.runner == runner.runner_name && last_occupied_base.map_or(true, |occupied_base| occupied_base >= o.base.into())
            }) {
                // Every runner out is an out (obviously), and
                // removes the runner from the bases
                outs_to_add += 1;
                batter_out += 1;
            }
        }

        let mut batter_added = false;
        let mut new_runners = 0;
        if let Some(batter_name) = updates.runner_advances_may_include_batter {
            if let Some(new_runner) = advances_iter.peeking_next(|a| {
                // TODO Unify with other advances_iter.peeking_next
                if a.runner != batter_name {
                    return false;
                }
                if let Some(occupied_base) = last_occupied_base {
                    if occupied_base <= a.base.into() {
                        return false;
                    }
                }
                true
            }) {
                new_runners += 1;
                batter_added = true;
                self.state.runners_on.push_back(RunnerOn {
                    runner_name: new_runner.runner,
                    base: new_runner.base.into(),
                });
            }
        }

        // Check that we processed every change to existing runners
        let extra_steals = steals_iter.collect::<Vec<_>>();
        if !extra_steals.is_empty() {
            ingest_logs.error(format!("Failed to apply steal(s): {:?}", extra_steals));
        }
        let extra_scores = scores_iter.collect::<Vec<_>>();
        if !extra_scores.is_empty() {
            ingest_logs.error(format!("Failed to apply score(s): {:?}", extra_scores));
        }
        let extra_advances = advances_iter.collect::<Vec<_>>();
        if !extra_advances.is_empty() {
            ingest_logs.error(format!("Failed to apply advance(s): {:?}", extra_advances));
        }
        let extra_runners_out = runners_out_iter.collect::<Vec<_>>();
        if !extra_runners_out.is_empty() {
            ingest_logs.error(format!(
                "Failed to apply runner(s) out: {:?}",
                extra_runners_out
            ));
        }

        // Consistency check
        let expected_n_runners_after = n_runners_on_before as isize
            - n_caught_stealing as isize
            - n_stole_home as isize
            - n_scored as isize
            - n_runners_out as isize
            // Batter out will be included in n_runners_out but doesn't
            // represent an existing runner being removed from base
            + batter_out as isize
            + new_runners as isize;
        if self.state.runners_on.len() as isize != expected_n_runners_after {
            ingest_logs.error(format!(
                "Inconsistent runner counting: With {n_runners_on_before} on to start, \
                {n_caught_stealing} caught stealing, {n_stole_home} stealing home, {n_scored} scoring\
                , and {n_runners_out} out, including {batter_out} batter outs, plus {new_runners} new \
                runners, expected {expected_n_runners_after} runners on but our records show {}",
                self.state.runners_on.len(),
            ));
        }

        if let Some((runner_name, base)) = updates.runner_added {
            if !batter_added {
                if updates.runner_added_forces_advances {
                    let mut base_to_clear = base;
                    for runner in self.state.runners_on.iter_mut().rev() {
                        if runner.base == base {
                            base_to_clear = base_to_clear.next_base();
                            runner.base = base_to_clear;
                        } else {
                            // As soon as one runner doesn't need advancing, no subsequent runners do
                            break;
                        }
                    }
                } else if let Some(last_runner) = self.state.runners_on.back() {
                    if last_runner.base == base {
                        ingest_logs.warn(format!(
                            "Putting batter-runner {} on {:#?} when {} is already on it",
                            runner_name, base, last_runner.runner_name,
                        ));
                    } else if last_runner.base < base {
                        ingest_logs.warn(format!(
                            "Putting batter-runner {} on {:#?} when {} is on {:#?}",
                            runner_name, base, last_runner.runner_name, last_runner.base,
                        ));
                    }
                }

                self.state
                    .runners_on
                    .push_back(RunnerOn { runner_name, base });
            }
        }

        self.add_runs_to_batting_team(runs_to_add);
        self.add_outs(outs_to_add);
    }

    fn update_runners_steals_only(
        &mut self,
        steals: &[BaseSteal<&'g str>],
        ingest_logs: &mut IngestLogs,
    ) {
        self.update_runners(
            RunnerUpdate {
                steals,
                ..Default::default()
            },
            ingest_logs,
        );
    }

    fn process_mound_visit(
        &mut self,
        team: &EmojiTeam<&'g str>,
        ingest_logs: &mut IngestLogs,
        context_after: ContextAfterMoundVisitOutcome<'g>,
    ) -> Option<EventDetail<&'g str>> {
        if team.name != self.defending_team().team_name {
            ingest_logs.info(format!(
                "Defending team name from MoundVisit ({}) did not match the one from \
                                LiveNow ({}). Assuming this was a manual change.",
                team.name,
                self.defending_team().team_name,
            ));
            self.defending_team_mut().team_name = team.name;
        }
        if team.emoji != self.defending_team().team_emoji {
            ingest_logs.info(format!(
                "Defending team emoji from MoundVisit ({}) did not match the one \
                                from LiveNow ({}). Assuming this was a manual change.",
                team.emoji,
                self.defending_team().team_emoji,
            ));
            self.defending_team_mut().team_emoji = team.emoji;
        }

        self.state.context = EventContext::ExpectMoundVisitOutcome(context_after);
        None
    }

    pub fn next(
        &mut self,
        game_event_index: usize,
        event: &ParsedEventMessage<&'g str>,
        raw_event: &'g mmolb_parsing::game::Event,
        ingest_logs: &mut IngestLogs,
    ) -> Result<Option<EventDetail<&'g str>>, SimEventError> {
        let previous_event = self.state.prev_event_type;
        let this_event_discriminant = event.discriminant();

        let detail_builder = self.detail_builder(self.state.clone(), game_event_index, raw_event);

        let result = match self.state.context {
            EventContext::ExpectInningStart => game_event!(
                (previous_event, event),
                [ParsedEventMessageDiscriminants::InningStart]
                ParsedEventMessage::InningStart {
                    number,
                    side,
                    batting_team,
                    pitcher_status,
                    automatic_runner,
                } => {
                    if *side != self.state.inning_half.flip() {
                        ingest_logs.warn(format!(
                            "Unexpected inning side: expected {:?}, but saw {side:?}",
                            self.state.inning_half.flip(),
                        ));
                    }
                    self.state.inning_half = *side;

                    // If we just started a top, the number should increment
                    let expected_number = match self.state.inning_half {
                        TopBottom::Top => self.state.inning_number + 1,
                        TopBottom::Bottom => self.state.inning_number,
                    };

                    if *number != expected_number {
                        ingest_logs.warn(format!(
                            "Unexpected inning number: expected {}, but saw {}",
                            expected_number,
                            number,
                        ));
                    }
                    self.state.inning_number = *number;

                    if batting_team.name != self.batting_team().team_name {
                        ingest_logs.info(format!(
                            "Batting team name from InningStart ({}) did not \
                            match the one from LiveNow ({}). Assuming this was a manual change.",
                            batting_team.name,
                            self.batting_team().team_name,
                        ));
                        self.batting_team_mut().team_name = batting_team.name;
                    }
                    if batting_team.emoji != self.batting_team().team_emoji {
                        ingest_logs.info(format!(
                            "Batting team emoji from InningStart ({}) did not \
                            match the one from LiveNow ({}). Assuming this was a manual change.",
                            batting_team.emoji,
                            self.batting_team().team_emoji,
                        ));
                        self.batting_team_mut().team_emoji = batting_team.emoji;
                    }

                    match pitcher_status {
                        StartOfInningPitcher::Same { emoji, name } => {
                            ingest_logs.info(format!("Not incrementing pitcher_count on returning pitcher {emoji} {name}"));
                        }
                        StartOfInningPitcher::Different { leaving_emoji: _, leaving_pitcher, arriving_emoji: _, arriving_pitcher } => {
                            self.defending_team_mut().active_pitcher = (*arriving_pitcher).into();
                            self.defending_team_mut().pitcher_count += 1;
                            ingest_logs.info(format!("Incrementing pitcher_count as {leaving_pitcher} is replaced by {arriving_pitcher}."));
                        }
                    }

                    // Add the automatic runner to our state without emitting a db event for it.
                    // This way they will just show up on base without having an event that put
                    // them there, which I think is the correct interpretation.
                    if *number > 9 && self.automatic_runner_rule_is_active() {
                        // Before a certain point the automatic runner  wasn't announced
                        // in the event. You just had to figure out who it was based on the
                        // lineup. There were two events in this period where mote timing
                        // means we predict the wrong automatic runner. Since it's only two,
                        // and going forward the game will announce the automatic runner, we
                        // just hard-code fixes for the mispredictions.
                        // It's just a coincidence that they're both  bottoms of 10ths... or
                        // is it...
                        let stored_automatic_runner = if self.game_id == "680b4f1d11f35e62dba3ebb2" && *number == 10 && *side == TopBottom::Bottom {
                            "Victoria Persson"
                        } else if self.game_id == "6812571a17b36c4c9b40e06d" && *number == 10 && *side == TopBottom::Bottom {
                            "Hassan Espinosa"
                        } else {
                            self.active_automatic_runner()
                                .ok_or_else(|| SimEventError::MissingAutomaticRunner {
                                    inning_num: *number
                                })?
                        };

                        let runner_name = if let Some(runner_name) = automatic_runner {
                            // The automatic runner from the message should always take
                            // priority, because certain things (like augments firing during
                            // a game) can cause the stored runner to be incorrect. Those
                            // cases have already been detected and overridden for all games
                            // that happened before the automatic runner was announced.
                            runner_name
                        } else {
                            stored_automatic_runner
                        };

                        ingest_logs.debug(format!("Adding automatic runner {runner_name}"));

                        self.state.runners_on.push_back(RunnerOn {
                            runner_name,
                            base: TaxaBase::Second, // Automatic runners are always placed on second
                        })
                    }

                    self.state.outs = 0;
                    self.state.context = EventContext::ExpectNowBatting;
                    None
                },
                [ParsedEventMessageDiscriminants::MoundVisit]
                ParsedEventMessage::MoundVisit { team, mound_visit_type: _ } => {
                    self.process_mound_visit(team, ingest_logs, ContextAfterMoundVisitOutcome::ExpectNowBatting)
                },
            ),
            EventContext::ExpectPitch(batter_name) => {
                let pitch = raw_event.pitch.as_ref().and_then(|p| {
                    Some(Pitch {
                        pitch_speed: p.speed,
                        pitch_type: p.pitch_type.as_ref().copied().ok()?,
                        pitch_zone: p.zone,
                    })
                });

                if !is_pitchless_pitch(event.discriminant()) {
                    if raw_event.pitch.is_none() {
                        ingest_logs.error("Event is mising a pitch");
                    } else if pitch.is_none() {
                        ingest_logs.error("Pitch type wasn't recognized");
                    }
                }

                game_event!(
                    (previous_event, event),
                    [ParsedEventMessageDiscriminants::Ball]
                    ParsedEventMessage::Ball { count, steals } => {
                        self.state.count_balls += 1;
                        self.check_count(*count, ingest_logs);
                        self.update_runners(RunnerUpdate {
                            steals,
                            ..Default::default()
                        }, ingest_logs);

                        detail_builder
                            .pitch(pitch)
                            .steals(steals.clone())
                            .build_some(self, batter_name, ingest_logs, TaxaEventType::Ball)
                    },
                    [ParsedEventMessageDiscriminants::Strike]
                    ParsedEventMessage::Strike { strike, count, steals } => {
                        self.state.count_strikes += 1;
                        self.check_count(*count, ingest_logs);

                        self.update_runners_steals_only(steals, ingest_logs);

                        detail_builder
                            .pitch(pitch)
                            .steals(steals.clone())
                            .build_some(self, batter_name, ingest_logs, match strike {
                                StrikeType::Looking => { TaxaEventType::CalledStrike }
                                StrikeType::Swinging => { TaxaEventType::SwingingStrike }
                            })
                    },
                    [ParsedEventMessageDiscriminants::StrikeOut]
                    ParsedEventMessage::StrikeOut { foul, batter, strike, steals } => {
                        self.check_batter(batter_name, batter, ingest_logs);
                        if self.state.count_strikes < 2 {
                            ingest_logs.warn(format!(
                                "Unexpected strikeout: expected 2 strikes in the count, but \
                                there were {}",
                                self.state.count_strikes,
                            ));
                        }

                        self.update_runners_steals_only(steals, ingest_logs);
                        self.add_out();
                        self.finish_pa(batter);

                        let event_type = match (foul, strike) {
                            (None, StrikeType::Looking) => { TaxaEventType::CalledStrikeout }
                            (None, StrikeType::Swinging) => { TaxaEventType::SwingingStrikeout }
                            (Some(FoulType::Ball), _) => {
                                ingest_logs.error(
                                    "Can't strike out on a foul ball. \
                                    Recording this as a foul tip instead.",
                                );
                                TaxaEventType::FoulTipStrikeout
                            }
                            (Some(FoulType::Tip), StrikeType::Looking) => {
                                ingest_logs.warn("Can't have a foul tip on a called strike.");
                                TaxaEventType::FoulTipStrikeout
                            }
                            (Some(FoulType::Tip), StrikeType::Swinging) => { TaxaEventType::FoulTipStrikeout }
                        };

                        detail_builder
                            .pitch(pitch)
                            .steals(steals.clone())
                            .build_some(self, batter, ingest_logs, event_type)
                    },
                    [ParsedEventMessageDiscriminants::Foul]
                    ParsedEventMessage::Foul { foul, steals, count } => {
                        // Falsehoods...
                        if !(*foul == FoulType::Ball && self.state.count_strikes >= 2) {
                            self.state.count_strikes += 1;
                        }
                        self.check_count(*count, ingest_logs);

                        self.update_runners_steals_only(steals, ingest_logs);

                        detail_builder
                            .pitch(pitch)
                            .steals(steals.clone())
                            .build_some(self, batter_name, ingest_logs, match foul {
                                FoulType::Tip => TaxaEventType::FoulTip,
                                FoulType::Ball => TaxaEventType::FoulBall,
                            })
                    },
                    [ParsedEventMessageDiscriminants::FairBall]
                    ParsedEventMessage::FairBall { batter, fair_ball_type, destination } => {
                        self.check_batter(batter_name, batter, ingest_logs);

                        self.state.context = EventContext::ExpectFairBallOutcome(batter, FairBall {
                            game_event_index,
                            fair_ball_type: *fair_ball_type,
                            fair_ball_destination: *destination,
                            pitch
                        });
                        None
                    },
                    [ParsedEventMessageDiscriminants::Walk]
                    ParsedEventMessage::Walk { batter, advances, scores } => {
                        self.check_batter(batter_name, batter, ingest_logs);

                        self.update_runners(RunnerUpdate {
                            scores,
                            advances,
                            runner_added: Some((batter, TaxaBase::First)),
                            ..Default::default()
                        }, ingest_logs);
                        self.finish_pa(batter);

                        detail_builder
                            .pitch(pitch)
                            .runner_changes(advances.clone(), scores.clone())
                            .add_runner(batter, TaxaBase::First)
                            .build_some(self, batter, ingest_logs, TaxaEventType::Walk)
                    },
                    [ParsedEventMessageDiscriminants::HitByPitch]
                    ParsedEventMessage::HitByPitch { batter, advances, scores } => {
                        self.check_batter(batter_name, batter, ingest_logs);

                        self.update_runners(RunnerUpdate {
                            scores,
                            advances,
                            runner_added: Some((batter, TaxaBase::First)),
                            ..Default::default()
                        }, ingest_logs);
                        self.finish_pa(batter);

                        detail_builder
                            .pitch(pitch)
                            .runner_changes(advances.clone(), scores.clone())
                            .add_runner(batter, TaxaBase::First)
                            .build_some(self, batter, ingest_logs, TaxaEventType::HitByPitch)
                    },
                    [ParsedEventMessageDiscriminants::MoundVisit]
                    ParsedEventMessage::MoundVisit { team, mound_visit_type: _ } => {
                        self.process_mound_visit(team, ingest_logs, ContextAfterMoundVisitOutcome::ExpectPitch(batter_name))
                    },
                    [ParsedEventMessageDiscriminants::Balk]
                    ParsedEventMessage::Balk { advances, pitcher, scores } => {
                        self.check_pitcher(pitcher, ingest_logs);

                        // A balk has perfectly predictable effects on the runners, but
                        // MMOLB tells us exactly what they were so I don't need to
                        // implement the logic
                        self.update_runners(RunnerUpdate {
                            scores,
                            advances,
                            ..Default::default()
                        }, ingest_logs);

                        detail_builder
                            .pitch(pitch)
                            .runner_changes(advances.clone(), scores.clone())
                            .build_some(self, batter_name, ingest_logs, TaxaEventType::Balk)
                    },
                    [ParsedEventMessageDiscriminants::FallingStar]
                    ParsedEventMessage::FallingStar { player_name } => {
                        self.state.context = EventContext::ExpectFallingStarOutcome {
                            falling_star_hit_player: player_name,
                            batter_name,
                        };
                        None
                    }
                )
            }
            EventContext::ExpectNowBatting => game_event!(
                (previous_event, event),
                [ParsedEventMessageDiscriminants::NowBatting]
                ParsedEventMessage::NowBatting { batter, stats } => {
                    if self.batter_stats(batter).is_none() {
                        ingest_logs.info(format!(
                            "Batter {batter} is new to this game. Assuming they were swapped in \
                            using a mote.",
                        ));
                    }

                    let team = self.batting_team_mut();
                    if team.advance_to_next_batter {
                        team.batter_count += 1;
                        team.batter_subcount = 0;
                        team.advance_to_next_batter = false;
                    } else if team.has_seen_first_batter {
                        team.batter_subcount += 1;
                    } else {
                        team.batter_count = 0;
                        team.batter_subcount = 0;
                        team.has_seen_first_batter = true;
                    }

                    check_now_batting_stats(&stats, self.batter_stats_mut(batter), ingest_logs);

                    self.state.context = EventContext::ExpectPitch(batter);
                    None
                },
                [ParsedEventMessageDiscriminants::MoundVisit]
                ParsedEventMessage::MoundVisit { team, mound_visit_type: _ } => {
                    self.process_mound_visit(team, ingest_logs, ContextAfterMoundVisitOutcome::ExpectNowBatting)
                },
            ),
            EventContext::ExpectFairBallOutcome(batter_name, fair_ball) => game_event!(
                (previous_event, event),
                [ParsedEventMessageDiscriminants::CaughtOut]
                ParsedEventMessage::CaughtOut { batter, fair_ball_type, caught_by, advances, scores, sacrifice, perfect } => {
                    self.check_batter(batter_name, batter, ingest_logs);
                    self.check_fair_ball_type(&fair_ball, *fair_ball_type, ingest_logs);

                    self.update_runners(RunnerUpdate {
                        scores,
                        advances,
                        ..Default::default()
                    }, ingest_logs);
                    self.add_out();
                    self.finish_pa(batter_name);

                    detail_builder
                        .fair_ball(fair_ball)
                        .described_as_sacrifice(*sacrifice)
                        .is_toasty(*perfect)
                        .fielder(*caught_by, ingest_logs)?
                        .runner_changes(advances.clone(), scores.clone())
                        .build_some(self, batter_name, ingest_logs, TaxaEventType::CaughtOut)
                },
                [ParsedEventMessageDiscriminants::GroundedOut]
                ParsedEventMessage::GroundedOut { batter, fielders, scores, advances, perfect } => {
                    self.check_batter(batter_name, batter, ingest_logs);

                    self.update_runners(RunnerUpdate {
                        scores,
                        advances,
                        ..Default::default()
                    }, ingest_logs);
                    self.add_out();
                    self.finish_pa(batter_name);

                    detail_builder
                        .fair_ball(fair_ball)
                        .is_toasty(*perfect)
                        .fielders(fielders.clone(), ingest_logs)?
                        .runner_changes(advances.clone(), scores.clone())
                        .build_some(self, batter_name, ingest_logs, TaxaEventType::GroundedOut)
                },
                [ParsedEventMessageDiscriminants::BatterToBase]
                ParsedEventMessage::BatterToBase { batter, distance, fair_ball_type, fielder, advances, scores } => {
                    self.check_batter(batter_name, batter, ingest_logs);
                    self.check_fair_ball_type(&fair_ball, *fair_ball_type, ingest_logs);

                    self.update_runners(RunnerUpdate {
                        scores,
                        advances,
                        runner_added: Some((batter, (*distance).into())),
                        ..Default::default()
                    }, ingest_logs);
                    self.finish_pa(batter_name);

                    detail_builder
                        .fair_ball(fair_ball)
                        .hit_base((*distance).into())
                        .fielder(*fielder, ingest_logs)?
                        .runner_changes(advances.clone(), scores.clone())
                        .add_runner(batter, (*distance).into())
                        .build_some(self, batter_name, ingest_logs, TaxaEventType::Hit)
                },
                [ParsedEventMessageDiscriminants::ReachOnFieldingError]
                ParsedEventMessage::ReachOnFieldingError { batter, fielder, error, scores, advances } => {
                    self.check_batter(batter_name, batter, ingest_logs);

                    self.update_runners(RunnerUpdate {
                        scores,
                        advances,
                        runner_added: Some((batter, TaxaBase::First)),
                        ..Default::default()
                    }, ingest_logs);
                    self.finish_pa(batter_name);

                    detail_builder
                        .fair_ball(fair_ball)
                        .fielding_error_type((*error).into())
                        .fielder(*fielder, ingest_logs)?
                        .runner_changes(advances.clone(), scores.clone())
                        .add_runner(batter, TaxaBase::First)
                        .build_some(self, batter_name, ingest_logs, TaxaEventType::FieldingError)
                },
                [ParsedEventMessageDiscriminants::HomeRun]
                ParsedEventMessage::HomeRun { batter, fair_ball_type, destination, scores, grand_slam } => {
                    self.check_batter(batter_name, batter, ingest_logs);
                    self.check_fair_ball_type(&fair_ball, *fair_ball_type, ingest_logs);
                    self.check_fair_ball_destination(&fair_ball, *destination, ingest_logs);

                    if *grand_slam && scores.len() != 3 {
                        ingest_logs.warn(format!(
                            "Parsed a grand slam, but {} non-batter runners scored (expected 3)",
                            scores.len(),
                        ));
                    } else if !*grand_slam && scores.len() == 3 {
                        ingest_logs.warn("3 non-batter scored but we didn't parse a grand slam");
                    }

                    // This is the one situation where you can have
                    // scores but no advances, because after everyone
                    // scores there's no one left to advance
                    self.update_runners(RunnerUpdate {
                        scores,
                        ..Default::default()
                    }, ingest_logs);
                    // Also the only situation where you have a score
                    // without an existing runner
                    self.add_runs_to_batting_team(1);
                    self.finish_pa(batter_name);

                    detail_builder
                        .fair_ball(fair_ball)
                        .hit_base(TaxaBase::Home)
                        .runner_changes(Vec::new(), scores.clone())
                        .set_batter_scores()
                        .build_some(self, batter_name, ingest_logs, TaxaEventType::HomeRun)
                },
                [ParsedEventMessageDiscriminants::DoublePlayCaught]
                ParsedEventMessage::DoublePlayCaught { batter, advances, scores, out_two, fair_ball_type, fielders } => {
                    self.check_batter(batter_name, batter, ingest_logs);
                    self.check_fair_ball_type(&fair_ball, *fair_ball_type, ingest_logs);

                    self.update_runners(RunnerUpdate {
                        scores,
                        advances,
                        runners_out: &[*out_two],
                        ..Default::default()
                    }, ingest_logs);
                    self.add_out(); // This is the out for the batter
                    self.finish_pa(batter_name);  // Must be after all outs are added

                    detail_builder
                        .fair_ball(fair_ball)
                        .runner_changes(advances.clone(), scores.clone())
                        .add_out(*out_two)
                        .fielders(fielders.clone(), ingest_logs)?
                        .build_some(self, batter_name, ingest_logs, TaxaEventType::DoublePlay)
                },
                [ParsedEventMessageDiscriminants::DoublePlayGrounded]
                ParsedEventMessage::DoublePlayGrounded { batter, advances, scores, out_one, out_two, fielders, sacrifice } => {
                    self.check_batter(batter_name, batter, ingest_logs);

                    self.update_runners(RunnerUpdate {
                        scores,
                        advances,
                        runners_out: &[*out_one, *out_two],
                        runners_out_may_include_batter: Some(batter),
                        runner_advances_may_include_batter: Some(batter),
                        ..Default::default()
                    }, ingest_logs);
                    self.finish_pa(batter_name);

                    detail_builder
                        .fair_ball(fair_ball)
                        .described_as_sacrifice(*sacrifice)
                        .runner_changes(advances.clone(), scores.clone())
                        .add_out(*out_one)
                        .add_out(*out_two)
                        .fielders(fielders.clone(), ingest_logs)?
                        .build_some(self, batter_name, ingest_logs, TaxaEventType::DoublePlay)
                },
                [ParsedEventMessageDiscriminants::ForceOut]
                ParsedEventMessage::ForceOut { batter, out, fielders, scores, advances, fair_ball_type } => {
                    self.check_batter(batter_name, batter, ingest_logs);
                    self.check_fair_ball_type(&fair_ball, *fair_ball_type, ingest_logs);

                    self.update_runners(RunnerUpdate {
                        scores,
                        advances,
                        runners_out: &[*out],
                        runner_added: Some((batter, TaxaBase::First)),
                        runner_added_forces_advances: true,
                        runner_advances_may_include_batter: Some(batter),
                        ..Default::default()
                    }, ingest_logs);
                    self.finish_pa(batter_name);

                    let detail_builder = detail_builder
                        .fair_ball(fair_ball)
                        .runner_changes(advances.clone(), scores.clone())
                        .add_out(*out);

                    // The batter-runner making it to base will be listed in `advances` if
                    // any other runner advanced without getting out or scoring. That's a
                    // complicated condition to check, so instead we just check if there's
                    // already an advance to first (which should always be the batter)
                    let detail_builder = if advances.iter().any(|a| a.base == Base::First) {
                        detail_builder
                    } else {
                        detail_builder.add_runner(batter, TaxaBase::First)
                    };

                    detail_builder
                        .fielders(fielders.clone(), ingest_logs)?
                        .build_some(self, batter_name, ingest_logs, TaxaEventType::ForceOut)
                },
                [ParsedEventMessageDiscriminants::ReachOnFieldersChoice]
                ParsedEventMessage::ReachOnFieldersChoice { batter, fielders, result, scores, advances } => {
                    self.check_batter(batter_name, batter, ingest_logs);

                    if let FieldingAttempt::Out { out } = result {
                        self.update_runners(RunnerUpdate {
                            scores,
                            advances,
                            runners_out: &[*out],
                            runner_added: Some((batter, TaxaBase::First)),
                            runner_added_forces_advances: true,
                            ..Default::default()
                        }, ingest_logs)
                    } else {
                        self.update_runners(RunnerUpdate {
                            scores,
                            advances,
                            runner_added: Some((batter, TaxaBase::First)),
                            runner_added_forces_advances: true,
                            ..Default::default()
                        }, ingest_logs)
                    };

                    self.finish_pa(batter_name);

                    match result {
                        FieldingAttempt::Out { out } => {
                            detail_builder
                                .fair_ball(fair_ball)
                                .runner_changes(advances.clone(), scores.clone())
                                .add_runner(batter, TaxaBase::First)
                                .add_out(*out)
                                .fielders(fielders.clone(), ingest_logs)?
                                .build_some(self, batter_name, ingest_logs, TaxaEventType::FieldersChoice)
                        }
                        FieldingAttempt::Error { fielder, error } => {
                            if let Some((listed_fielder,)) = fielders.iter().collect_tuple() {
                                if listed_fielder.name != *fielder {
                                    ingest_logs.warn(format!("Fielder who made the error ({}) is not the one listed as fielding the ball ({})", fielder, listed_fielder.name));
                                }
                            } else {
                                ingest_logs.warn("Expected exactly one listed fielder in a fielder's choice with an error");
                            }

                            detail_builder
                                .fair_ball(fair_ball)
                                .runner_changes(advances.clone(), scores.clone())
                                .add_runner(batter, TaxaBase::First)
                                .fielders(fielders.clone(), ingest_logs)?
                                .fielding_error_type((*error).into())
                                .build_some(self, batter_name, ingest_logs, TaxaEventType::ErrorOnFieldersChoice)
                        }
                    }
                },
                [ParsedEventMessageDiscriminants::KnownBug]
                ParsedEventMessage::KnownBug { bug } => {
                    match bug {
                        KnownBug::FirstBasemanChoosesAGhost { batter, first_baseman } => {
                            self.check_batter(batter_name, batter, ingest_logs);
                            ingest_logs.debug("Known bug: FirstBasemanChoosesAGhost");


                            // This is a Weird Event:tm: that puts the batter on first and
                            // adds an out, even though no runner actually got out. It's only
                            // been observed with no runners on base.
                            if !self.state.runners_on.is_empty() {
                                ingest_logs.warn(format!(
                                    "Observed FirstBasemanChoosesAGhost bug with runners {:?}. \
                                    This bug is only expected when there are no runners.",
                                    self.state.runners_on,
                                ));
                            }

                            self.add_outs(1);
                            self.state.runners_on.push_back(RunnerOn {
                                runner_name: batter,
                                base: TaxaBase::First,
                            });
                            self.finish_pa(batter_name);

                            let fielders = vec![PlacedPlayer {
                                name: *first_baseman,
                                place: Place::FirstBaseman,
                            }];

                            detail_builder
                                .fair_ball(fair_ball)
                                .add_runner(batter, TaxaBase::First)
                                .fielders(fielders, ingest_logs)?
                                .build_some(self, batter_name, ingest_logs, TaxaEventType::FieldersChoice)
                        }
                    }
                },
            ),
            EventContext::ExpectFallingStarOutcome {
                falling_star_hit_player,
                batter_name,
            } => game_event!(
                (previous_event, event),
                [ParsedEventMessageDiscriminants::FallingStarOutcome]
                ParsedEventMessage::FallingStarOutcome { deflection, player_name, outcome } => {
                    // Deflected retirements were bugged at first. (Note we
                    // only know it was a deflection because Danny said so. The
                    // event message doesn't say who it was deflected off so we
                    // can't fill it in.)
                    let (player_name, outcome) = if self.game_id == "68741e50f86033e4ba111a3f" && falling_star_hit_player == "Mia Parks" {
                        ingest_logs.info(
                            "Manually corrected a bugged retirement. This event should have said \
                            that Mia Parks Retired and was replaced by Louise Galvan"
                        );
                        ("Mia Parks", FallingStarOutcome::Retired(Some("Louise Galvan")))
                    } else {
                        (*player_name, *outcome)
                    };

                    if let Some(player_name) = deflection {
                        if *player_name != falling_star_hit_player {
                            ingest_logs.warn(format!(
                                "Expected a falling star to be deflected off {}, but it was \
                                deflected off {}",
                                falling_star_hit_player,
                                player_name,
                            ));
                        }
                    } else {
                        if player_name != falling_star_hit_player {
                            ingest_logs.warn(format!(
                                "Expected a falling star to hit {}, but it hit {}",
                                falling_star_hit_player,
                                player_name,
                            ));
                        }
                    }

                    self.state.context = EventContext::ExpectPitch(self.batter_after_retirement(batter_name, player_name, outcome, ingest_logs));

                    None
                },
            ),
            EventContext::ExpectInningEnd => game_event!(
                (previous_event, event),
                [ParsedEventMessageDiscriminants::InningEnd]
                ParsedEventMessage::InningEnd { number, side } => {
                    if *number != self.state.inning_number {
                        ingest_logs.warn(format!(
                            "Unexpected inning number: expected {}, but saw {number}",
                            self.state.inning_number,
                        ));
                    }

                    if *side != self.state.inning_half {
                        ingest_logs.warn(format!(
                            "Unexpected inning side: expected {:?}, but saw {side:?}",
                            self.state.inning_half,
                        ));
                    }

                    // These get cleared at the end of a PA, but the PA doesn't end for an inning-
                    // ending caught stealing
                    self.state.count_balls = 0;
                    self.state.count_strikes = 0;

                    self.state.runners_on.clear();

                    let game_finished = if *number < 9 {
                        // Game never ends if inning number is less than 9
                        ingest_logs.info(format!(
                            "Game didn't end after the {side:#?} of the {number} because it was \
                            before the 9th inning",
                        ));
                        false
                    } else if *side == TopBottom::Top && self.state.home_score > self.state.away_score {
                        // Game ends after the top of the inning if it's 9 or later and the home
                        // team is winning
                        ingest_logs.info(format!(
                            "Game ended after the top of the {number} because the home team was \
                            winning ({}-{})",
                            self.state.away_score, self.state.home_score,
                        ));
                        true
                    } else if *side == TopBottom::Bottom && self.state.home_score != self.state.away_score {
                        // Game ends after the bottom of the inning if it's 9 or later and it's not
                        // a tie
                        ingest_logs.info(format!(
                            "Game ended after the bottom of the {number} because the score was not \
                            tied ({}-{})",
                            self.state.away_score, self.state.home_score,
                        ));
                        true
                    } else {
                        // Otherwise the game does not end
                        ingest_logs.info(format!(
                            "Game didn't end after the {side:#?} of the {number} \
                            (with score {}-{})",
                            self.state.away_score, self.state.home_score,
                        ));
                        false
                    };

                    if game_finished {
                        self.end_game();
                    } else {
                        self.state.context = EventContext::ExpectInningStart;
                    }
                    None
                },
            ),
            EventContext::ExpectMoundVisitOutcome(context_after) => game_event!(
                (previous_event, event),
                [ParsedEventMessageDiscriminants::PitcherRemains]
                ParsedEventMessage::PitcherRemains { remaining_pitcher } => {
                    ingest_logs.info(format!("Not incrementing pitcher_count on remaining pitcher {remaining_pitcher}"));
                    self.state.context = context_after.into();
                    None
                },
                [ParsedEventMessageDiscriminants::PitcherSwap]
                ParsedEventMessage::PitcherSwap { leaving_pitcher, arriving_pitcher_name, arriving_pitcher_place } => {
                    ingest_logs.debug(format!("Arriving pitcher {arriving_pitcher_name} with place {arriving_pitcher_place:?}"));
                    self.defending_team_mut().active_pitcher = BestEffortSlottedPlayer {
                        name: *arriving_pitcher_name,
                        slot: arriving_pitcher_place.map_or(BestEffortSlot::GenericPitcher, Into::into),
                    };
                    self.defending_team_mut().pitcher_count += 1;
                    ingest_logs.info(format!("Incrementing pitcher_count as {leaving_pitcher} is replaced by {arriving_pitcher_name}."));
                    self.state.context = context_after.into();
                    None
                },
            ),
            EventContext::ExpectGameEnd => game_event!(
                (previous_event, event),
                [ParsedEventMessageDiscriminants::GameOver]
                ParsedEventMessage::GameOver { message } => {
                    match message {
                        GameOverMessage::GameOver => {
                            // This only happened in season 0 days 1 and 2
                            if let Day::Day(day) = self.day {  // day
                                if (self.season, day) > (0, 2) {
                                    ingest_logs.warn(
                                        "Old-style <em>Game Over.</em> message appeared after s0d2",
                                    );
                                }
                            } else {
                                ingest_logs.warn(
                                    "Old-style <em>Game Over.</em> message appeared after s0d2",
                                );
                            }
                        }
                        GameOverMessage::QuotedGAMEOVER => {
                            // This has happened since season 0 day 2
                            if let Day::Day(day) = self.day {  // day
                                if (self.season, day) <= (0, 2) {
                                    ingest_logs.warn(
                                        "New-style <em>\"GAME OVER.\"</em> message appeared on or \
                                        before s0d2",
                                    );
                                }
                            }
                        }
                    }

                    // Note: Not setting self.state.game_finished here,
                    // because proper baserunner accounting requires it
                    // be marked as finished before we finish
                    // processing the event that set the context to
                    // ExpectGameEnd
                    self.state.context = EventContext::ExpectFinalScore;
                    None
                },
            ),
            EventContext::ExpectFinalScore => game_event!(
                (previous_event, event),
                [ParsedEventMessageDiscriminants::Recordkeeping]
                ParsedEventMessage::Recordkeeping { winning_score, winning_team, losing_score, losing_team } => {
                    macro_rules! warn_if_mismatch {
                        ($ingest_logs: expr, $winning_or_losing:expr, $comparison_description:expr, $home_or_away:expr, $actual:expr, $expected:expr $(,)?) => {
                            if $actual != $expected {
                                $ingest_logs.warn(format!(
                                    "Expected the {} {} to be {} ({} team) but it was {}",
                                    $winning_or_losing,
                                    $comparison_description,
                                    $expected,
                                    $home_or_away,
                                    $actual,
                                ));
                            }
                        };
                    }

                    if self.state.away_score < self.state.home_score {
                        warn_if_mismatch!(ingest_logs, "winning", "score", "home", *winning_score, self.state.home_score);
                        warn_if_mismatch!(ingest_logs, "winning", "team emoji", "home", winning_team.emoji, self.home.team_emoji);
                        warn_if_mismatch!(ingest_logs, "winning", "team name", "home", winning_team.name, self.home.team_name);

                        warn_if_mismatch!(ingest_logs, "losing", "score", "away", *losing_score, self.state.away_score);
                        warn_if_mismatch!(ingest_logs, "losing", "team emoji", "away", losing_team.emoji, self.away.team_emoji);
                        warn_if_mismatch!(ingest_logs, "losing", "team name", "away", losing_team.name, self.away.team_name);
                    } else {
                        warn_if_mismatch!(ingest_logs, "winning", "score", "away", *winning_score, self.state.away_score);
                        warn_if_mismatch!(ingest_logs, "winning", "team emoji", "away", winning_team.emoji, self.away.team_emoji);
                        warn_if_mismatch!(ingest_logs, "winning", "team name", "away", winning_team.name, self.away.team_name);

                        warn_if_mismatch!(ingest_logs, "losing", "score", "home", *losing_score, self.state.home_score);
                        warn_if_mismatch!(ingest_logs, "losing", "team emoji", "home", losing_team.emoji, self.home.team_emoji);
                        warn_if_mismatch!(ingest_logs, "losing", "team name", "home", losing_team.name, self.home.team_name);
                    }

                    self.state.context = EventContext::Finished;
                    None
                },
                [ParsedEventMessageDiscriminants::WeatherDelivery]
                ParsedEventMessage::WeatherDelivery { .. } => {
                    // TODO Don't ignore weather delivery
                    None
                },
                [ParsedEventMessageDiscriminants::WeatherShipment]
                ParsedEventMessage::WeatherShipment { .. } => {
                    // TODO Don't ignore weather shipment
                    None
                },
                [ParsedEventMessageDiscriminants::WeatherSpecialDelivery]
                ParsedEventMessage::WeatherSpecialDelivery { .. } => {
                    // TODO Don't ignore weather shipment
                    None
                }
            ),
            EventContext::Finished => game_event!((previous_event, event)),
        }?;

        self.state.prev_event_type = this_event_discriminant;

        // In season 0 the game didn't clear the bases immediately
        // when the third inning is recorded, but Danny has said it
        // will. That means that for now, baserunner consistency
        // may be wrong after the 3rd out.
        if self.state.outs >= 3 {
            if !self.state.runners_on.is_empty() {
                ingest_logs.error("runners_on must be empty when there are 3 (or more) outs");
            }
        } else if self.state.game_finished {
            if !self.state.runners_on.is_empty() {
                ingest_logs.error("runners_on must be empty when the game is over");
            }
        } else {
            self.check_baserunner_consistency(raw_event, ingest_logs);
        }

        Ok(result)
    }

    fn batter_after_retirement(
        &self,
        batter_name: &'g str,
        hit_player_name: &str,
        outcome: FallingStarOutcome<&'g str>,
        ingest_logs: &mut IngestLogs,
    ) -> &'g str {
        // First: If the hit player doesn't match the batter name, it can't
        // be a batter replacement
        if batter_name != hit_player_name {
            return batter_name;
        }

        // Next: If it's not a Retired outcome, it can't be a batter
        // replacement
        let FallingStarOutcome::Retired(replacement_name) = outcome else {
            return batter_name;
        };

        // If we don't know the replacement's name, our only option is to
        // proceed as if the batter wasn't replaced, even if they were
        let Some(replacement_name) = replacement_name else {
            if self.game_id == "686ee660e52e01aa1b9eb7ca" && batter_name == "Vicki Nagai" {
                ingest_logs.info("Hard-coded player replacement: Norma Muñoz for Vicki Nagai");
                return "Norma Muñoz";
            } else if self.game_id == "686ee660e52e01aa1b9eb7ca" && batter_name == "Lena Vitale" {
                ingest_logs.info("Hard-coded player replacement: Ido Barrington for Lena Vitale");
                return "Ido Barrington";
            } else {
                ingest_logs.error(format!(
                    "MMOLB did not name {batter_name}'s replacement. This event requires manual \
                    correction. In the meantime, we'll act as if {batter_name} was not replaced."
                ));
                return batter_name;
            };
        };

        // Then, if we've gotten this far, we have to assume this is a
        // batter replacement.
        ingest_logs.info(format!(
            "Replacing stored batter {batter_name} with replacement player {replacement_name}.

            Note: MMOLB currently cannot detect cases when a different player with the same name \
            as the active batter is replaced. If you find an example of this, please let us know \
            so we can handle it manually.",
        ));

        replacement_name
    }
}

fn format_lineup(lineup: &[PlacedPlayer<impl AsRef<str>>]) -> String {
    let mut s = String::new();
    for player in lineup {
        write!(
            s,
            "\n    - name: \"{}\", place: \"{}\"",
            player.name.as_ref(),
            player.place
        )
        .unwrap();
    }
    s
}

// This can be disabled once the to-do is addressed
#[allow(unreachable_code, unused_variables)]
fn check_now_batting_stats(
    stats: &NowBattingStats,
    batter_stats: &BatterStats,
    ingest_logs: &mut IngestLogs,
) {
    return; // TODO Finish implementing this

    match stats {
        NowBattingStats::FirstPA => {
            if !batter_stats.is_empty() {
                ingest_logs.warn(
                    "In NowBatting, expected this batter to have no stats in the current game",
                );
            }
        }
        NowBattingStats::Stats(stats) => {
            let mut their_stats = stats.iter();

            match their_stats.next() {
                None => {
                    ingest_logs.warn("This NowBatting event had stats, but the vec was empty");
                }
                Some(BatterStat::HitsForAtBats { hits, at_bats }) => {
                    if *hits != batter_stats.hits {
                        ingest_logs.warn(format!(
                            "NowBatting said player has {hits} hits, but our records say {}",
                            batter_stats.hits
                        ));
                    }
                    if *at_bats != batter_stats.at_bats {
                        ingest_logs.warn(format!(
                            "NowBatting said player has {at_bats} at bats, but our records say {}",
                            batter_stats.at_bats
                        ));
                    }
                }
                Some(other) => {
                    ingest_logs.warn(format!(
                        "First item in stats was not HitsForAtBats {:?}",
                        other
                    ));
                }
            }

            let our_stats = batter_stats.stats.iter();

            for zipped in their_stats.zip_longest(our_stats) {
                match zipped {
                    EitherOrBoth::Left(theirs) => {
                        ingest_logs.warn(format!(
                            "NowBatting event had unexpected stat entry {:?}",
                            theirs
                        ));
                    }
                    EitherOrBoth::Right(ours) => {
                        ingest_logs
                            .warn(format!("NowBatting missing expected stat entry {:?}", ours));
                    }
                    EitherOrBoth::Both(theirs, ours) => {
                        todo!("Compare {:?} to {:?}", theirs, ours)
                    }
                }
            }
        }
        NowBattingStats::NoStats => {
            todo!("What does this mean?")
        }
    }
}
