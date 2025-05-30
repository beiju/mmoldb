use crate::db::{
    TaxaBase, TaxaBaseDescriptionFormat, TaxaBaseWithDescriptionFormat, TaxaEventType,
    TaxaFairBallType, TaxaFieldingErrorType, TaxaHitType, TaxaPosition,
};
use itertools::{EitherOrBoth, Itertools, PeekingNext};
use log::warn;
use mmolb_parsing::ParsedEventMessage;
use mmolb_parsing::enums::{
    Base, BaseNameVariant, BatterStat, Distance, FairBallDestination, FairBallType, FoulType,
    HomeAway, NowBattingStats, Position, StrikeType, TopBottom,
};
use mmolb_parsing::parsed_event::{
    BaseSteal, FieldingAttempt, ParsedEventMessageDiscriminants, PositionedPlayer, RunnerAdvance,
    RunnerOut, StartOfInningPitcher,
};
use std::collections::VecDeque;
use std::fmt::{Debug, Formatter};
use std::fmt::Write;
use strum::IntoDiscriminant;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SimFatalError {
    #[error("This game had no events")]
    NoEvents,

    #[error("Not enough events. Expected {expected:?} event after {previous:?}")]
    NotEnoughEvents {
        expected: &'static [ParsedEventMessageDiscriminants],
        previous: ParsedEventMessageDiscriminants,
    },

    #[error("Expected {expected:?} event after {previous:?}, but received {received:?}")]
    UnexpectedEventType {
        expected: &'static [ParsedEventMessageDiscriminants],
        previous: Option<ParsedEventMessageDiscriminants>,
        received: ParsedEventMessageDiscriminants,
    },
}

#[derive(Debug, Clone)]
pub struct EventDetailRunner<StrT: Clone> {
    pub name: StrT,
    pub base_before: Option<TaxaBase>,
    pub base_after: TaxaBase,
    pub is_out: bool,
    pub base_description_format: Option<TaxaBaseDescriptionFormat>,
    pub is_steal: bool,
}

#[derive(Debug, Clone)]
pub struct EventDetailFielder<StrT: Clone> {
    pub name: StrT,
    pub position: TaxaPosition,
    pub is_perfect_catch: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct EventDetail<StrT: Clone> {
    pub game_event_index: usize,
    pub fair_ball_event_index: Option<usize>,
    pub inning: u8,
    pub top_of_inning: bool,
    pub count_balls: u8,
    pub count_strikes: u8,
    pub outs_before: i32,
    pub outs_after: i32,
    pub batter_count: usize,
    pub batter_name: StrT,
    pub pitcher_name: StrT,
    pub fielders: Vec<EventDetailFielder<StrT>>,

    pub detail_type: TaxaEventType,
    pub hit_type: Option<TaxaHitType>,
    pub fair_ball_type: Option<TaxaFairBallType>,
    pub fair_ball_direction: Option<TaxaPosition>,
    pub fielding_error_type: Option<TaxaFieldingErrorType>,

    pub baserunners: Vec<EventDetailRunner<StrT>>,
}

#[derive(Debug)]
pub struct IngestLog {
    pub log_level: i32,
    pub log_text: String,
}

// A utility to more conveniently build a Vec<IngestLog>
pub struct IngestLogs {
    logs: Vec<IngestLog>,
}

impl IngestLogs {
    pub fn new() -> Self {
        Self { logs: Vec::new() }
    }

    pub fn critical(&mut self, s: impl Into<String>) {
        self.logs.push(IngestLog {
            log_level: 0,
            log_text: s.into(),
        });
    }

    pub fn error(&mut self, s: impl Into<String>) {
        self.logs.push(IngestLog {
            log_level: 1,
            log_text: s.into(),
        });
    }

    pub fn warn(&mut self, s: impl Into<String>) {
        self.logs.push(IngestLog {
            log_level: 2,
            log_text: s.into(),
        });
    }

    pub fn info(&mut self, s: impl Into<String>) {
        self.logs.push(IngestLog {
            log_level: 3,
            log_text: s.into(),
        });
    }

    pub fn debug(&mut self, s: impl Into<String>) {
        self.logs.push(IngestLog {
            log_level: 4,
            log_text: s.into(),
        });
    }

    #[allow(dead_code)]
    pub fn trace(&mut self, s: impl Into<String>) {
        self.logs.push(IngestLog {
            log_level: 5,
            log_text: s.into(),
        });
    }

    pub fn into_vec(self) -> Vec<IngestLog> {
        self.logs
    }
}

#[derive(Debug, Copy, Clone)]
struct FairBall {
    index: usize,
    fair_ball_type: FairBallType,
    fair_ball_destination: FairBallDestination,
}

#[derive(Debug, Copy, Clone)]
enum GamePhase {
    ExpectInningStart,
    ExpectNowBatting,
    ExpectPitch,
    ExpectFairBallOutcome(FairBall),
    ExpectInningEnd,
    ExpectMoundVisitOutcome,
    ExpectGameEnd,
    ExpectFinalScore,
    Finished,
}

#[derive(Debug)]
pub struct BatterStats {
    hits: u8,
    at_bats: u8,
    stats: Vec<()>,
}

impl BatterStats {
    pub fn is_empty(&self) -> bool {
        self.stats.is_empty() && self.hits == 0 && self.at_bats == 0
    }
}

#[derive(Debug)]
pub struct BatterInGame<StrT> {
    name: StrT,
    stats: BatterStats,
}

impl<'g> BatterInGame<&'g str> {
    pub fn new(name: &'g str) -> Self {
        Self {
            name,
            stats: BatterStats {
                hits: 0,
                at_bats: 0,
                stats: Vec::new(),
            },
        }
    }
    
    pub fn from_position_player(player: &PositionedPlayer<&'g str>) -> Self {
        Self {
            name: player.name,
            stats: BatterStats {
                hits: 0,
                at_bats: 0,
                stats: Vec::new(),
            },
        }
    }
}

#[derive(Debug)]
pub struct TeamInGame<'g> {
    team_name: &'g str,
    team_emoji: &'g str,
    pitcher: PositionedPlayer<&'g str>,
    lineup: Vec<BatterInGame<&'g str>>,
    // This is incremented when a PA finishes, so in between PAs (and before the first PA) it
    // represents the next batter
    batter_count: usize,
}

#[derive(Debug, Clone)]
struct RunnerOn<'g> {
    runner_name: &'g str,
    base: TaxaBase,
}

#[derive(Debug, Clone)]
struct GameState<'g> {
    prev_event_type: ParsedEventMessageDiscriminants,
    phase: GamePhase,
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
    // Should never change
    game_id: &'g str,

    // Aggregates
    away: TeamInGame<'g>,
    home: TeamInGame<'g>,
    
    // Changes at most once
    used_mote: bool,

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
    ) -> Result<&'a ParsedEventMessage<&'g str>, SimFatalError> {
        match self.inner.next() {
            Some(val) => {
                self.prev_event_type = Some(val.discriminant());
                Ok(val)
            }
            None => match self.prev_event_type {
                None => Err(SimFatalError::NoEvents),
                Some(previous) => Err(SimFatalError::NotEnoughEvents { expected, previous }),
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
            other => Err(SimFatalError::UnexpectedEventType {
                expected,
                previous,
                received: other.discriminant(),
            })
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
            other => Err(SimFatalError::UnexpectedEventType {
                expected,
                previous,
                received: other.discriminant(),
            })
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
    prev_game_state: GameState<'g>,
    game_event_index: usize,
    batter_count_at_event_start: usize,
    fair_ball_event_index: Option<usize>,
    fair_ball_type: Option<TaxaFairBallType>,
    fair_ball_direction: Option<TaxaPosition>,
    hit_type: Option<TaxaHitType>,
    fielding_error_type: Option<TaxaFieldingErrorType>,
    fielders: Vec<EventDetailFielder<&'g str>>,
    advances: Vec<RunnerAdvance<&'g str>>,
    scores: Vec<&'g str>,
    steals: Vec<BaseSteal<&'g str>>,
    runner_added: Option<(&'g str, TaxaBase)>,
    runners_out: Vec<RunnerOut<&'g str>>,
}

impl<'g> EventDetailBuilder<'g> {
    fn fair_ball(mut self, fair_ball: FairBall) -> Self {
        self.fair_ball_event_index = Some(fair_ball.index);
        self.fair_ball_type = Some(fair_ball.fair_ball_type.into());
        self.fair_ball_direction = Some(fair_ball.fair_ball_destination.into());
        self
    }

    fn hit_type(mut self, hit_type: TaxaHitType) -> Self {
        self.hit_type = Some(hit_type);
        self
    }

    fn fielding_error_type(mut self, fielding_error_type: TaxaFieldingErrorType) -> Self {
        self.fielding_error_type = Some(fielding_error_type);
        self
    }

    fn fielder(mut self, fielder: PositionedPlayer<&'g str>) -> Self {
        if !self.fielders.is_empty() {
            warn!("EventDetailBuilder overwrote existing fielders");
        }

        self.fielders = vec![EventDetailFielder {
            name: fielder.name,
            position: fielder.position.into(),
            is_perfect_catch: None,
        }];
        self
    }

    fn catch_fielder(mut self, fielder: PositionedPlayer<&'g str>, is_perfect: bool) -> Self {
        if !self.fielders.is_empty() {
            warn!("EventDetailBuilder overwrote existing fielders");
        }

        self.fielders = vec![EventDetailFielder {
            name: fielder.name,
            position: fielder.position.into(),
            is_perfect_catch: Some(is_perfect),
        }];
        self
    }

    fn fielders(mut self, fielders: impl IntoIterator<Item = PositionedPlayer<&'g str>>) -> Self {
        if !self.fielders.is_empty() {
            warn!("EventDetailBuilder overwrote existing fielders");
        }

        self.fielders = fielders.into_iter().map(|f| EventDetailFielder {
            name: f.name,
            position: f.position.into(),
            is_perfect_catch: None,
        }).collect();
        
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

    pub fn build_some(
        self,
        game: &Game<'g>,
        ingest_logs: &mut IngestLogs,
        type_detail: TaxaEventType,
    ) -> Option<EventDetail<&'g str>> {
        Some(self.build(game, ingest_logs, type_detail))
    }

    pub fn build(
        self,
        game: &Game<'g>,
        ingest_logs: &mut IngestLogs,
        type_detail: TaxaEventType,
    ) -> EventDetail<&'g str> {
        let batter_name = game
            .batter_for_active_team(self.batter_count_at_event_start)
            .name;

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
                            ({}, expected {batter_name})",
                        out.runner,
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
                        "Got a stray advance ({}) that doesn't match the batter name \
                            ({batter_name})",
                        advance.runner,
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
            count_balls: game.state.count_balls,
            count_strikes: game.state.count_strikes,
            outs_before: self.prev_game_state.outs,
            outs_after: game.state.outs,
            batter_count: self.batter_count_at_event_start,
            batter_name,
            pitcher_name: game.active_pitcher().name,
            fielders: self.fielders,
            detail_type: type_detail,
            hit_type: self.hit_type,
            fair_ball_type: self.fair_ball_type,
            fair_ball_direction: self.fair_ball_direction,
            fielding_error_type: self.fielding_error_type,
            baserunners,
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

impl<'g> Game<'g> {
    pub fn new<'a, IterT>(
        game_id: &'g str,
        events: &'a mut IterT,
    ) -> Result<(Game<'g>, Vec<Vec<IngestLog>>), SimFatalError>
    where
        'g: 'a,
        IterT: Iterator<Item = &'a ParsedEventMessage<&'g str>>,
    {
        let mut events = ParsedEventMessageIter::new(events);
        let mut ingest_logs = Vec::new();

        let (away_team_name, away_team_emoji, home_team_name, home_team_emoji) = extract_next_game_event!(
            events,
            [ParsedEventMessageDiscriminants::LiveNow]
            ParsedEventMessage::LiveNow {
                away_team_name,
                away_team_emoji,
                home_team_name,
                home_team_emoji,
            } => (
                away_team_name,
                away_team_emoji,
                home_team_name,
                home_team_emoji,
            )
        )?;

        ingest_logs.push({
            let mut logs = IngestLogs::new();
            logs.debug(format!(
                "Set home team to name: \"{home_team_name}\", emoji: \"{home_team_emoji}\""
            ));
            logs.debug(format!(
                "Set away team to name: \"{away_team_name}\", emoji: \"{away_team_emoji}\""
            ));
            logs.into_vec()
        });

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
                away_team_name,
                away_team_emoji,
                home_team_name,
                home_team_emoji,
            } => (
                home_pitcher,
                away_pitcher,
                away_team_name,
                away_team_emoji,
                home_team_name,
                home_team_emoji,
            )
        )?;
        let mut event_ingest_logs = IngestLogs::new();
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

        let away_lineup = extract_next_game_event!(
            events,
            [ParsedEventMessageDiscriminants::Lineup]
            ParsedEventMessage::Lineup { side: HomeAway::Away, players } => players
        )?;
        ingest_logs.push({
            let mut logs = IngestLogs::new();
            logs.debug(format!(
                "Set away lineup to: {}",
                format_lineup(&away_lineup)
            ));
            logs.into_vec()
        });

        let home_lineup = extract_next_game_event!(
            events,
            [ParsedEventMessageDiscriminants::Lineup]
            ParsedEventMessage::Lineup { side: HomeAway::Home, players } => players
        )?;
        ingest_logs.push({
            let mut logs = IngestLogs::new();
            logs.debug(format!(
                "Set home lineup to: {}",
                format_lineup(&home_lineup)
            ));
            logs.into_vec()
        });

        extract_next_game_event!(
            events,
            [ParsedEventMessageDiscriminants::PlayBall]
            ParsedEventMessage::PlayBall => ()
        )?;
        ingest_logs.push(Vec::new());

        let game = Self {
            game_id,
            away: TeamInGame {
                team_name: away_team_name,
                team_emoji: away_team_emoji,
                pitcher: PositionedPlayer {
                    name: away_pitcher_name,
                    position: Position::StartingPitcher,
                },
                lineup: away_lineup
                    .into_iter()
                    .map(BatterInGame::from_position_player)
                    .collect(),
                batter_count: 0,
            },
            home: TeamInGame {
                team_name: home_team_name,
                team_emoji: home_team_emoji,
                pitcher: PositionedPlayer {
                    name: home_pitcher_name,
                    position: Position::StartingPitcher,
                },
                lineup: home_lineup
                    .iter()
                    .map(BatterInGame::from_position_player)
                    .collect(),
                batter_count: 0,
            },
            used_mote: false,
            state: GameState {
                prev_event_type: ParsedEventMessageDiscriminants::PlayBall,
                phase: GamePhase::ExpectInningStart,
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

    fn active_pitcher(&self) -> &PositionedPlayer<&'g str> {
        match self.state.inning_half {
            TopBottom::Top => &self.home.pitcher,
            TopBottom::Bottom => &self.away.pitcher,
        }
    }

    fn active_pitcher_mut(&mut self) -> &mut PositionedPlayer<&'g str> {
        match self.state.inning_half {
            TopBottom::Top => &mut self.home.pitcher,
            TopBottom::Bottom => &mut self.away.pitcher,
        }
    }

    fn active_batter(&self) -> &BatterInGame<&'g str> {
        let batting_team = self.batting_team();
        let lineup = &batting_team.lineup;
        &lineup[batting_team.batter_count % lineup.len()]
    }

    fn active_batter_mut(&mut self) -> &mut BatterInGame<&'g str> {
        let batting_team = self.batting_team_mut();
        let lineup = &mut batting_team.lineup;
        let lineup_len = lineup.len();
        &mut lineup[batting_team.batter_count % lineup_len]
    }

    fn active_automatic_runner(&self) -> &BatterInGame<&'g str> {
        let batting_team = self.batting_team();
        let lineup = &batting_team.lineup;
        // Add lineup.len to avoid underflow when batter_count is 0
        &lineup[(batting_team.batter_count + lineup.len() - 1) % lineup.len()]
    }

    fn batter_for_active_team(&self, batter_count: usize) -> &BatterInGame<&'g str> {
        let batting_team = self.batting_team();
        let lineup = &batting_team.lineup;
        &lineup[batter_count % lineup.len()]
    }

    fn check_batter(&self, batter_name: &str, ingest_logs: &mut IngestLogs) {
        let active_batter = self.active_batter();
        if active_batter.name != batter_name {
            ingest_logs.warn(format!(
                "Unexpected batter name: Expected {}, but saw {}",
                active_batter.name, batter_name,
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
        batter_count: usize,
    ) -> EventDetailBuilder<'g> {
        EventDetailBuilder {
            prev_game_state,
            batter_count_at_event_start: batter_count,
            fair_ball_event_index: None,
            game_event_index,
            fielders: Vec::new(),
            advances: Vec::new(),
            hit_type: None,
            fair_ball_type: None,
            fair_ball_direction: None,
            fielding_error_type: None,
            scores: Vec::new(),
            steals: Vec::new(),
            runner_added: None,
            runners_out: Vec::new(),
        }
    }

    // Note: Must happen after all outs for this event are added
    pub fn finish_pa(&mut self) {
        // Occam's razor: assume "at bats" is actually PAs until proven
        // otherwise
        self.active_batter_mut().stats.at_bats += 1;

        self.state.count_strikes = 0;
        self.state.count_balls = 0;

        if self.state.inning_number >= 9
            && self.state.inning_half == TopBottom::Bottom
            && self.state.home_score > self.state.away_score
        {
            // If it's the bottom of a 9th or later, and the score is
            // now in favor of the home team, it's a walk-off
            self.state.runners_on.clear();
            self.state.game_finished = true;
            self.state.phase = GamePhase::ExpectGameEnd;
        } else if self.state.outs >= 3 {
            // Otherwise, if there's 3 outs, the inning ends
            self.state.phase = GamePhase::ExpectInningEnd;
        } else {
            // Otherwise just go to the next batter
            self.state.phase = GamePhase::ExpectNowBatting;
        }

        self.batting_team_mut().batter_count += 1;
    }

    pub fn add_outs(&mut self, num_outs: i32) {
        self.state.outs += num_outs;

        // This is usually redundant with finish_pa, but not in the case of inning-ending
        // caught stealing
        if self.state.outs >= 3 {
            self.state.phase = GamePhase::ExpectInningEnd;
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
                // A steal is eligible if the name matches and the
                // next occupied base is later than the one they
                // tried to steal
                s.runner == runner.runner_name
                    && last_occupied_base
                        .map_or(true, |occupied_base| occupied_base > s.base.into())
            }) {
                return if steal.caught {
                    // Caught out: Add an out and remove the runner
                    outs_to_add += 1;
                    false
                } else if steal.base == Base::Home {
                    // Stole home: Add a run and remove the runner
                    runs_to_add += 1;
                    false
                } else {
                    // Stole any other base: Update the runner and
                    // retain them, also updating the last occupied
                    // base
                    runner.base = steal.base.into();
                    last_occupied_base = Some(runner.base);
                    true
                };
            }

            // Next, look for advances
            if let Some(advance) = advances_iter.peeking_next(|a| {
                // An advance is eligible if the name matches and
                // the next occupied base is later than the one
                // they advanced to
                a.runner == runner.runner_name
                    && last_occupied_base
                        .map_or(true, |occupied_base| occupied_base > a.base.into())
            }) {
                // For an advance, the only thing necessary is to
                // update the runner's base and the last occupied
                // base, then retain the runner
                runner.base = advance.base.into();
                last_occupied_base = Some(runner.base);
                return true;
            }

            // Next, look for outs
            if let Some(_) = runners_out_iter.peeking_next(|o| {
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
                outs_to_add += 1;
                return false;
            }

            // If none of the above applies, the runner must not have moved
            last_occupied_base = Some(runner.base);
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

    pub fn next(
        &mut self,
        index: usize,
        event: &ParsedEventMessage<&'g str>,
        raw_event: &mmolb_parsing::game::Event,
        ingest_logs: &mut IngestLogs,
    ) -> Result<Option<EventDetail<&'g str>>, SimFatalError> {
        let previous_event = self.state.prev_event_type;
        let this_event_discriminant = event.discriminant();

        let detail_builder =
            self.detail_builder(self.state.clone(), index, self.batting_team().batter_count);

        let result = match self.state.phase {
            GamePhase::ExpectInningStart => game_event!(
                (previous_event, event),
                [ParsedEventMessageDiscriminants::InningStart]
                ParsedEventMessage::InningStart {
                    number,
                    side,
                    batting_team_emoji,
                    batting_team_name,
                    pitcher_status,
                    automatic_runner,
                } => {
                    if *side != self.state.inning_half.flip() {
                        ingest_logs.warn(format!(
                            "Unexpected inning side in {}: expected {:?}, but saw {side:?}",
                            self.game_id,
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
                            "Unexpected inning number in {}: expected {}, but saw {}",
                            self.game_id,
                            expected_number,
                            number,
                        ));
                    }
                    self.state.inning_number = *number;

                    if *batting_team_name != self.batting_team().team_name {
                        ingest_logs.warn(format!(
                            "Batting team name from InningStart ({batting_team_name}) did \
                            not match the one from LiveNow ({})",
                            self.batting_team().team_name,
                        ));
                    }
                    if *batting_team_emoji != self.batting_team().team_emoji {
                        ingest_logs.warn(format!(
                            "Batting team emoji from InningStart ({batting_team_emoji}) did \
                            not match the one from LiveNow ({})",
                            self.batting_team().team_emoji,
                        ));
                    }

                    match pitcher_status {
                        StartOfInningPitcher::Same { name, emoji } => {
                            ingest_logs.info(format!(
                                "Started {} of {} with same pitcher {emoji} {name}",
                                self.state.inning_half,
                                self.state.inning_number,
                            ));
                        }
                        StartOfInningPitcher::Different { arriving_pitcher, arriving_position, leaving_pitcher, leaving_position } => {
                            if *leaving_pitcher != self.active_pitcher().name {
                                ingest_logs.warn(format!(
                                    "The pitcher who left ({}) did not match the previously active \
                                    pitcher ({})",
                                    leaving_pitcher, self.active_pitcher().name,
                                ));
                            }

                            if *leaving_position != self.active_pitcher().position {
                                ingest_logs.warn(format!(
                                    "The position of the pitcher who left ({}) did not match the \
                                    previously active pitcher's position ({})",
                                    leaving_position, self.active_pitcher().position,
                                ));
                            }

                            ingest_logs.info(format!(
                                "Started {} of {} with new pitcher {arriving_position} {arriving_pitcher}",
                                self.state.inning_half,
                                self.state.inning_number,
                            ));

                            *self.active_pitcher_mut() = PositionedPlayer {
                                name: arriving_pitcher,
                                position: *arriving_position,
                            };
                        }
                    }

                    // Add the automatic runner to our state without emitting a db event for it.
                    // This way they will just show up on base without having an event that put
                    // them there, which I think is the correct interpretation.
                    if let Some(runner_name) = automatic_runner {
                        if *runner_name != self.active_automatic_runner().name {
                            ingest_logs.warn(format!(
                                "Unexpected automatic runner: expected {}, but saw {}",
                                self.active_automatic_runner().name, runner_name,
                            ));
                        }

                        self.state.runners_on.push_back(RunnerOn {
                            runner_name,
                            base: TaxaBase::Second, // Automatic runners are always placed on second
                        })
                    } else if *number > 9 {
                        // Before a certain point the automatic runner
                        // wasn't announced in the event. You just had
                        // to figure out who it was based on the lineup
                        self.state.runners_on.push_back(RunnerOn {
                            runner_name: self.active_automatic_runner().name,
                            base: TaxaBase::Second, // Automatic runners are always placed on second
                        })
                    }

                    self.state.outs = 0;
                    self.state.phase = GamePhase::ExpectNowBatting;
                    None
                },
                [ParsedEventMessageDiscriminants::MoundVisit]
                ParsedEventMessage::MoundVisit { emoji, team } => {
                    if *team != self.defending_team().team_name {
                        ingest_logs.warn(format!(
                            "Batting team name from MoundVisit ({team}) did \
                            not match the one from LiveNow ({})",
                            self.defending_team().team_name,
                        ));
                    }
                    if *emoji != self.defending_team().team_emoji {
                        ingest_logs.warn(format!(
                            "Batting team emoji from MoundVisit ({emoji}) did \
                            not match the one from LiveNow ({})",
                            self.defending_team().team_emoji,
                        ));
                    }

                    self.state.phase = GamePhase::ExpectMoundVisitOutcome;
                    None
                },
            ),
            GamePhase::ExpectPitch => game_event!(
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
                        .steals(steals.clone())
                        .build_some(self, ingest_logs, TaxaEventType::Ball)
                },
                [ParsedEventMessageDiscriminants::Strike]
                ParsedEventMessage::Strike { strike, count, steals } => {
                    self.state.count_strikes += 1;
                    self.check_count(*count, ingest_logs);

                    self.update_runners_steals_only(steals, ingest_logs);

                    detail_builder
                        .steals(steals.clone())
                        .build_some(self, ingest_logs, match strike {
                            StrikeType::Looking => { TaxaEventType::CalledStrike }
                            StrikeType::Swinging => { TaxaEventType::SwingingStrike }
                        })
                },
                [ParsedEventMessageDiscriminants::StrikeOut]
                ParsedEventMessage::StrikeOut { foul, batter, strike, steals } => {
                    self.check_batter(batter, ingest_logs);
                    if self.state.count_strikes < 2 {
                        ingest_logs.warn(format!(
                            "Unexpected strikeout in {}: expected 2 strikes in the count, but \
                            there were {}",
                            self.game_id,
                            self.state.count_strikes,
                        ));
                    }

                    self.update_runners_steals_only(steals, ingest_logs);
                    self.add_out();
                    self.finish_pa();
                    
                    let event_type = match (foul, strike) {
                        (None, StrikeType::Looking) => { TaxaEventType::CalledStrike }
                        (None, StrikeType::Swinging) => { TaxaEventType::SwingingStrike }
                        (Some(FoulType::Ball), _) => { 
                            ingest_logs.error(
                                "Can't strike out on a foul ball. \
                                Recording this as a foul tip instead.",
                            );
                            TaxaEventType::FoulTip
                        }
                        (Some(FoulType::Tip), StrikeType::Looking) => { 
                            ingest_logs.warn("Can't have a foul tip on a called strike.");
                            TaxaEventType::FoulTip
                        }
                        (Some(FoulType::Tip), StrikeType::Swinging) => { TaxaEventType::FoulTip }
                    };

                    detail_builder
                        .steals(steals.clone())
                        .build_some(self, ingest_logs, event_type)
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
                        .steals(steals.clone())
                        .build_some(self, ingest_logs, match foul {
                            FoulType::Tip => TaxaEventType::FoulTip,
                            FoulType::Ball => TaxaEventType::FoulBall,
                        })
                },
                [ParsedEventMessageDiscriminants::FairBall]
                ParsedEventMessage::FairBall { batter, fair_ball_type, destination } => {
                    self.check_batter(batter, ingest_logs);

                    self.state.phase = GamePhase::ExpectFairBallOutcome(FairBall {
                        index,
                        fair_ball_type: *fair_ball_type,
                        fair_ball_destination: *destination,
                    });
                    None
                },
                [ParsedEventMessageDiscriminants::Walk]
                ParsedEventMessage::Walk { batter, advances, scores } => {
                    self.check_batter(batter, ingest_logs);

                    self.update_runners(RunnerUpdate {
                        scores,
                        advances,
                        runner_added: Some((batter, TaxaBase::First)),
                        ..Default::default()
                    }, ingest_logs);
                    self.finish_pa();

                    detail_builder
                        .runner_changes(advances.clone(), scores.clone())
                        .add_runner(batter, TaxaBase::First)
                        .build_some(self, ingest_logs, TaxaEventType::Walk)
                },
                [ParsedEventMessageDiscriminants::HitByPitch]
                ParsedEventMessage::HitByPitch { batter, advances, scores } => {
                    self.check_batter(batter, ingest_logs);

                    self.update_runners(RunnerUpdate {
                        scores,
                        advances,
                        runner_added: Some((batter, TaxaBase::First)),
                        ..Default::default()
                    }, ingest_logs);
                    self.finish_pa();

                    detail_builder
                        .runner_changes(advances.clone(), scores.clone())
                        .build_some(self, ingest_logs, TaxaEventType::HitByPitch)
                },
            ),
            GamePhase::ExpectNowBatting => game_event!(
                (previous_event, event),
                [ParsedEventMessageDiscriminants::NowBatting]
                ParsedEventMessage::NowBatting { batter: batter_name, stats } => {
                    let batter = self.active_batter_mut();
                    if batter.name != *batter_name {
                         ingest_logs.info(format!(
                            "Batter {batter_name} did not match expected batter {}. \
                            Assuming this player was replaced using a mote.",
                            batter.name,
                        ));
                        
                        *batter = BatterInGame::new(batter_name);
                        
                        if self.used_mote {
                            ingest_logs.error(
                                "Attempted to use a mote to swap batters, but a mote was \
                                already used in this game!",
                            );
                        } else {
                            self.used_mote = true;
                        }
                    }
                    // Need to reborrow for lifetime safety
                    let batter = self.active_batter();
                    check_now_batting_stats(&stats, &batter.stats, ingest_logs);
 
                    self.state.phase = GamePhase::ExpectPitch;
                    None
                },
                [ParsedEventMessageDiscriminants::MoundVisit]
                ParsedEventMessage::MoundVisit { emoji, team } => {
                    if self.defending_team().team_name != *team {
                         ingest_logs.warn(format!(
                             "Team name in MoundVisit doesn't match: Expected {}, but saw {team}",
                             self.defending_team().team_name,
                         ));
                    }
 
                    if self.defending_team().team_emoji != *emoji {
                         ingest_logs.warn(format!(
                             "Team emoji in MoundVisit doesn't match: Expected {}, but saw {emoji}",
                             self.defending_team().team_emoji,
                         ));
                    }
 
                    self.state.phase = GamePhase::ExpectMoundVisitOutcome;
                    None
                },
            ),
            GamePhase::ExpectFairBallOutcome(fair_ball) => game_event!(
                (previous_event, event),
                [ParsedEventMessageDiscriminants::CaughtOut]
                ParsedEventMessage::CaughtOut { batter, fair_ball_type, caught_by, advances, scores, sacrifice, perfect } => {
                    self.check_batter(batter, ingest_logs);
                    self.check_fair_ball_type(&fair_ball, *fair_ball_type, ingest_logs);

                    self.update_runners(RunnerUpdate {
                        scores,
                        advances,
                        ..Default::default()
                    }, ingest_logs);
                    self.add_out();
                    self.finish_pa();

                    if *fair_ball_type != FairBallType::GroundBall {
                        if *sacrifice && scores.is_empty() {
                            ingest_logs.warn("Flyout was described as a sacrifice, but nobody scored");
                        } else if !*sacrifice && !scores.is_empty() {
                            ingest_logs.warn(
                                "Player(s) scored on flyout, but it was not described as a \
                                sacrifice",
                            );
                        }
                    } else if *sacrifice {
                        ingest_logs.warn("Non-flyout was described as sacrifice");
                    }

                    detail_builder
                        .fair_ball(fair_ball)
                        .catch_fielder(*caught_by, *perfect)
                        .runner_changes(advances.clone(), scores.clone())
                        .build_some(self, ingest_logs, TaxaEventType::CaughtOut)
                },
                [ParsedEventMessageDiscriminants::GroundedOut]
                ParsedEventMessage::GroundedOut { batter, fielders, scores, advances } => {
                    self.check_batter(batter, ingest_logs);

                    self.update_runners(RunnerUpdate {
                        scores,
                        advances,
                        ..Default::default()
                    }, ingest_logs);
                    self.add_out();
                    self.finish_pa();

                    detail_builder
                        .fair_ball(fair_ball)
                        .fielders(fielders.clone())
                        .runner_changes(advances.clone(), scores.clone())
                        .build_some(self, ingest_logs, TaxaEventType::GroundedOut)
                },
                [ParsedEventMessageDiscriminants::BatterToBase]
                ParsedEventMessage::BatterToBase { batter, distance, fair_ball_type, fielder, advances, scores } => {
                    self.check_batter(batter, ingest_logs);
                    self.check_fair_ball_type(&fair_ball, *fair_ball_type, ingest_logs);

                    self.update_runners(RunnerUpdate {
                        scores,
                        advances,
                        runner_added: Some((batter, (*distance).into())),
                        ..Default::default()
                    }, ingest_logs);
                    self.finish_pa();

                    detail_builder
                        .fair_ball(fair_ball)
                        .hit_type((*distance).into())
                        .fielder(*fielder)
                        .runner_changes(advances.clone(), scores.clone())
                        .build_some(self, ingest_logs, TaxaEventType::Hit)
                },
                [ParsedEventMessageDiscriminants::ReachOnFieldingError]
                ParsedEventMessage::ReachOnFieldingError { batter, fielder, error, scores, advances } => {
                    self.check_batter(batter, ingest_logs);

                    self.update_runners(RunnerUpdate {
                        scores,
                        advances,
                        runner_added: Some((batter, TaxaBase::First)),
                        ..Default::default()
                    }, ingest_logs);
                    self.finish_pa();

                    detail_builder
                        .fair_ball(fair_ball)
                        .fielding_error_type((*error).into())
                        .fielder(*fielder)
                        .runner_changes(advances.clone(), scores.clone())
                        .build_some(self, ingest_logs, TaxaEventType::FieldingError)
                },
                [ParsedEventMessageDiscriminants::HomeRun]
                ParsedEventMessage::HomeRun { batter, fair_ball_type, destination, scores, grand_slam } => {
                    self.check_batter(batter, ingest_logs);
                    self.check_fair_ball_type(&fair_ball, *fair_ball_type, ingest_logs);
                    self.check_fair_ball_destination(&fair_ball, *destination, ingest_logs);

                    if *grand_slam && scores.len() != 3 {
                        ingest_logs.warn(format!(
                            "Parsed a grand slam, but there were {} runners scored (expected 3)",
                            scores.len(),
                        ));
                    } else if !*grand_slam && scores.len() == 3 {
                        ingest_logs.warn("There were 3 runners scored but we didn't parse a grand slam");
                    }

                    // This is the one situation where you can have
                    // scores but no advances, because after everyone
                    // scores there's no one left to advance
                    self.update_runners(RunnerUpdate {
                        scores,
                        ..Default::default()
                    }, ingest_logs);
                    // Also the only situation where you have a score
                    // without the runner
                    self.add_runs_to_batting_team(1);
                    self.finish_pa();

                    detail_builder
                        .fair_ball(fair_ball)
                        .runner_changes(Vec::new(), scores.clone())
                        .build_some(self, ingest_logs, TaxaEventType::HomeRun)
                },
                [ParsedEventMessageDiscriminants::DoublePlayCaught]
                ParsedEventMessage::DoublePlayCaught { batter, advances, scores, out_two, fair_ball_type, fielders } => {
                    self.check_batter(batter, ingest_logs);
                    self.check_fair_ball_type(&fair_ball, *fair_ball_type, ingest_logs);

                    self.update_runners(RunnerUpdate {
                        scores,
                        advances,
                        runners_out: &[*out_two],
                        ..Default::default()
                    }, ingest_logs);
                    self.add_out(); // This is the out for the batter
                    self.finish_pa();  // Must be after all outs are added

                    detail_builder
                        .fair_ball(fair_ball)
                        .runner_changes(advances.clone(), scores.clone())
                        .add_out(*out_two)
                        .fielders(fielders.clone())
                        .build_some(self, ingest_logs, TaxaEventType::DoublePlay)
                },
                [ParsedEventMessageDiscriminants::DoublePlayGrounded]
                ParsedEventMessage::DoublePlayGrounded { batter, advances, scores, out_one, out_two, fielders, sacrifice } => {
                    self.check_batter(batter, ingest_logs);

                    // Assuming for now that sacrifice is any time
                    // there are scores
                    if *sacrifice && scores.is_empty() {
                        ingest_logs.warn("DoublePlayGrounded was described as a sacrifice, but nobody scored");
                    } else if !*sacrifice && !scores.is_empty() {
                        ingest_logs.warn(
                            "DoublePlayGrounded wasn't described as a sacrifice even though there \
                            were scores",
                        );
                    }

                    self.update_runners(RunnerUpdate {
                        scores,
                        advances,
                        runners_out: &[*out_one, *out_two],
                        runners_out_may_include_batter: Some(batter),
                        ..Default::default()
                    }, ingest_logs);
                    self.finish_pa();

                    detail_builder
                        .fair_ball(fair_ball)
                        .runner_changes(advances.clone(), scores.clone())
                        .add_out(*out_one)
                        .add_out(*out_two)
                        .fielders(fielders.clone())
                        .build_some(self, ingest_logs, TaxaEventType::DoublePlay)
                },
                [ParsedEventMessageDiscriminants::ForceOut]
                ParsedEventMessage::ForceOut { batter, out, fielders, scores, advances, fair_ball_type } => {
                    self.check_batter(batter, ingest_logs);
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
                    self.finish_pa();

                    detail_builder
                        .fair_ball(fair_ball)
                        .runner_changes(advances.clone(), scores.clone())
                        .add_out(*out)
                        .fielders(fielders.clone())
                        .build_some(self, ingest_logs, TaxaEventType::ForceOut)
                },
                [ParsedEventMessageDiscriminants::ReachOnFieldersChoice]
                ParsedEventMessage::ReachOnFieldersChoice { batter, fielders, result, scores, advances } => {
                    self.check_batter(batter, ingest_logs);

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

                    self.finish_pa();

                    match result {
                        FieldingAttempt::Out { out } => {
                            detail_builder
                                .fair_ball(fair_ball)
                                .runner_changes(advances.clone(), scores.clone())
                                .add_out(*out)
                                .fielders(fielders.clone())
                                .build_some(self, ingest_logs, TaxaEventType::FieldersChoice)
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
                                .fielders(fielders.clone())
                                .fielding_error_type((*error).into())
                                .build_some(self, ingest_logs, TaxaEventType::ErrorOnFieldersChoice)
                        }
                    }
                },
            ),
            GamePhase::ExpectInningEnd => game_event!(
                (previous_event, event),
                [ParsedEventMessageDiscriminants::InningEnd]
                ParsedEventMessage::InningEnd { number, side } => {
                    if *number != self.state.inning_number {
                        ingest_logs.warn(format!(
                            "Unexpected inning number in {}: expected {}, but saw {number}",
                            self.game_id,
                            self.state.inning_number,
                        ));
                    }

                    if *side != self.state.inning_half {
                        ingest_logs.warn(format!(
                            "Unexpected inning side in {}: expected {:?}, but saw {side:?}",
                            self.game_id,
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
                        ingest_logs.info(format!("Game didn't end after the {side:#?} of the {number} because it was before the 9th"));
                        false
                    } else if *side == TopBottom::Top && self.state.home_score > self.state.away_score {
                        // Game ends after the top of the inning if it's 9 or later and the home
                        // team is winning
                        ingest_logs.info(format!("Game ended after the top of the {number} because the home team was winning"));
                        true
                    } else if *side == TopBottom::Bottom && self.state.home_score != self.state.away_score {
                        // Game ends after the bottom of the inning if it's 9 or later and it's not
                        // a tie
                        ingest_logs.info(format!("Game ended after the bottom of the {number} because the score was not tied"));
                        true
                    } else {
                        // Otherwise the game does not end
                        ingest_logs.info(format!("Game didn't end after the {side:#?} of the {number} because the score was tied"));
                        false
                    };

                    if game_finished {
                        self.state.game_finished = true;
                        self.state.phase = GamePhase::ExpectGameEnd;
                    } else {
                        self.state.phase = GamePhase::ExpectInningStart;
                    }
                    None
                },
            ),
            GamePhase::ExpectMoundVisitOutcome => game_event!(
                (previous_event, event),
                [ParsedEventMessageDiscriminants::PitcherRemains]
                ParsedEventMessage::PitcherRemains { remaining_pitcher } => {
                    if remaining_pitcher.name != self.active_pitcher().name {
                        ingest_logs.warn(format!(
                            "In a PitcherRemains event, the pitcher who remained ({}) did not \
                            match the active pitcher ({})",
                            remaining_pitcher.name, self.active_pitcher().name,
                        ));
                    }

                    if remaining_pitcher.position != self.active_pitcher().position {
                        ingest_logs.warn(format!(
                            "In a PitcherRemains event, the position of the pitcher who remained \
                            ({}) did not match the active pitcher's position ({})",
                            remaining_pitcher.position, self.active_pitcher().position,
                        ));
                    }

                    self.state.phase = GamePhase::ExpectNowBatting;
                    None
                },
                [ParsedEventMessageDiscriminants::PitcherSwap]
                ParsedEventMessage::PitcherSwap { arriving_pitcher, arriving_position, leaving_pitcher, leaving_position } => {
                    if *leaving_pitcher != self.active_pitcher().name {
                        ingest_logs.warn(format!(
                            "In a PitcherSwap event, the pitcher who left ({}) did not match the \
                            previously active pitcher ({})",
                            leaving_pitcher, self.active_pitcher().name,
                        ));
                    }

                    if *leaving_position != self.active_pitcher().position {
                        ingest_logs.warn(format!(
                            "In a PitcherSwap event, the position of the pitcher who left ({}) \
                            did not match the previously active pitcher's position ({})",
                            leaving_position, self.active_pitcher().position,
                        ));
                    }

                    *self.active_pitcher_mut() = PositionedPlayer {
                        name: arriving_pitcher,
                        position: *arriving_position,
                    };

                    self.state.phase = GamePhase::ExpectNowBatting;
                    None
                },
            ),
            GamePhase::ExpectGameEnd => game_event!(
                (previous_event, event),
                [ParsedEventMessageDiscriminants::GameOver]
                ParsedEventMessage::GameOver => {
                    // Note: Not setting self.state.game_finished here,
                    // because proper baserunner accounting requires it
                    // be marked as finished before we finish
                    // processing the event that set the phase to
                    // ExpectGameEnd
                    self.state.phase = GamePhase::ExpectFinalScore;
                    None
                },
            ),
            GamePhase::ExpectFinalScore => game_event!(
                (previous_event, event),
                [ParsedEventMessageDiscriminants::Recordkeeping]
                ParsedEventMessage::Recordkeeping { winning_score, winning_team_emoji, winning_team_name, losing_score, losing_team_emoji, losing_team_name } => {
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
                        warn_if_mismatch!(ingest_logs, "winning", "team emoji", "home", *winning_team_emoji, self.home.team_emoji);
                        warn_if_mismatch!(ingest_logs, "winning", "team name", "home", *winning_team_name, self.home.team_name);

                        warn_if_mismatch!(ingest_logs, "losing", "score", "away", *losing_score, self.state.away_score);
                        warn_if_mismatch!(ingest_logs, "losing", "team emoji", "away", *losing_team_emoji, self.away.team_emoji);
                        warn_if_mismatch!(ingest_logs, "losing", "team name", "away", *losing_team_name, self.away.team_name);
                    } else {
                        warn_if_mismatch!(ingest_logs, "winning", "score", "away", *winning_score, self.state.away_score);
                        warn_if_mismatch!(ingest_logs, "winning", "team emoji", "away", *winning_team_emoji, self.away.team_emoji);
                        warn_if_mismatch!(ingest_logs, "winning", "team name", "away", *winning_team_name, self.away.team_name);

                        warn_if_mismatch!(ingest_logs, "losing", "score", "home", *losing_score, self.state.home_score);
                        warn_if_mismatch!(ingest_logs, "losing", "team emoji", "home", *losing_team_emoji, self.home.team_emoji);
                        warn_if_mismatch!(ingest_logs, "losing", "team name", "home", *losing_team_name, self.home.team_name);
                    }

                    self.state.phase = GamePhase::Finished;
                    None
                },
            ),
            GamePhase::Finished => game_event!((previous_event, event)),
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
}

fn format_lineup(lineup: &[PositionedPlayer<impl AsRef<str>>]) -> String {
    let mut s = String::new();
    for player in lineup {
        write!(
            s,
            "\n    - name: \"{}\", position: \"{}\"",
            player.name.as_ref(),
            player.position
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
        NowBattingStats::Stats { stats } => {
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

fn positioned_player_as_ref<StrT: AsRef<str> + Clone>(
    p: &EventDetailFielder<StrT>,
) -> PositionedPlayer<&str> {
    PositionedPlayer {
        name: p.name.as_ref(),
        position: p.position.into(),
    }
}

// Exactly equivalent to Option<TaxaBase> but we can derive Display on it
#[derive(Debug)]
pub enum MaybeBase {
    NoBase,
    Base(TaxaBase),
}

impl std::fmt::Display for MaybeBase {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoBase => write!(f, "batting"),
            Self::Base(b) => write!(f, "{}", b),
        }
    }
}

impl From<Option<TaxaBase>> for MaybeBase {
    fn from(value: Option<TaxaBase>) -> Self {
        match value {
            None => { MaybeBase::NoBase }
            Some(b) => { MaybeBase::Base(b) }
        }
    }
}

#[derive(Debug, Error)]
pub enum MissingBaseDescriptionFormat<'g> {
    #[error(
        "Missing base description format when runner {runner_name} got out moving from {prev_base} \
        to {out_at_base}"
    )]
    Out {
        runner_name: &'g str,
        prev_base: MaybeBase,
        out_at_base: TaxaBase,
    },
}

#[derive(Debug, Error)]
pub enum ToParsedError<'g> {
    #[error(transparent)]
    // Note: Can't use #[from] because of the lifetime
    MissingBaseDescriptionFormat(MissingBaseDescriptionFormat<'g>),
    
    #[error("{event_type} must have a fair_ball_type")]
    MissingFairBallType { event_type: TaxaEventType },
    
    #[error("{event_type} must have a fair_ball_direction")]
    MissingFairBallDirection { event_type: TaxaEventType },
    
    #[error("{event_type} must have a valid fair_ball_direction, but it had the non-fair-direction position {invalid_direction}")]
    InvalidFairBallDirection { 
        event_type: TaxaEventType,
        invalid_direction: TaxaPosition,
    },

    #[error("{event_type} must have a fielding_error_type")]
    MissingFieldingErrorType { event_type: TaxaEventType },
    
    #[error("{event_type} must have a hit_type")]
    MissingHitType { event_type: TaxaEventType },

    #[error("{event_type} must have exactly {required} runners out, but there were {actual}")]
    WrongNumberOfRunnersOut {
        event_type: TaxaEventType,
        required: usize,
        actual: usize,
    },

    #[error("{event_type} must have 1 or 2 runners out, but there were {actual}")]
    WrongNumberOfRunnersOutInDoublePlay {
        event_type: TaxaEventType,
        actual: usize,
    },

    #[error("{event_type} must have exactly {required} fielder, but there were {actual}")]
    WrongNumberOfFielders {
        event_type: TaxaEventType,
        required: usize,
        actual: usize,
    },
    
    #[error("{event_type} fielder(s) must have a Some() perfect catch")]
    MissingPerfectCatch {
        event_type: TaxaEventType,
    }
}

#[derive(Debug, Error)]
pub enum ToParsedContactError {
    #[error("Event with a fair_ball_index must have a fair_ball_type")]
    MissingFairBallType,
    
    #[error("Event with a fair_ball_index must have a fair_ball_direction")]
    MissingFairBallDirection,
    
    #[error("Event with a fair_ball_index must have a valid fair_ball_direction, but it had the non-fair-direction position {invalid_direction}")]
    InvalidFairBallDirection { 
        invalid_direction: TaxaPosition,
    },
}

impl<StrT: AsRef<str> + Clone> EventDetail<StrT> {
    fn count(&self) -> (u8, u8) {
        (self.count_balls, self.count_strikes)
    }

    fn fielders_iter(&self) -> impl Iterator<Item = PositionedPlayer<&str>> {
        self.fielders.iter().map(positioned_player_as_ref)
    }

    fn fielders(&self) -> Vec<PositionedPlayer<&str>> {
        self.fielders_iter().collect()
    }

    fn steals_iter(&self) -> impl Iterator<Item = BaseSteal<&str>> {
        self.baserunners.iter().flat_map(|runner| {
            if runner.is_steal {
                Some(BaseSteal {
                    runner: runner.name.as_ref(),
                    base: runner.base_after.into(),
                    caught: runner.is_out,
                })
            } else {
                None
            }
        })
    }

    fn steals(&self) -> Vec<BaseSteal<&str>> {
        self.steals_iter().collect()
    }

    // An advance is a baserunner who was on a non-home base before AND after this event
    fn advances_iter(
        &self,
        include_batter_runner: bool,
    ) -> impl Iterator<Item = RunnerAdvance<&str>> {
        self.baserunners.iter().flat_map(move |runner| {
            // If they got out, or
            if runner.is_out
                    // If they scored, or
                    || runner.base_after == TaxaBase::Home
                    // If they stayed still, or
                    || runner.base_before == Some(runner.base_after)
                    // If they're the batter and we're not asked to include the batter
                    || (runner.base_before == None && !include_batter_runner)
            {
                // Then don't return them
                None
            } else {
                // Otherwise do return them
                Some(RunnerAdvance {
                    runner: runner.name.as_ref(),
                    base: runner.base_after.into(),
                })
            }
        })
    }

    fn advances(&self, include_batter_runner: bool) -> Vec<RunnerAdvance<&str>> {
        self.advances_iter(include_batter_runner).collect()
    }

    // A score is any runner whose final base is Home
    fn scores_iter(&self) -> impl Iterator<Item = &str> {
        self.baserunners.iter().flat_map(|runner| {
            if !runner.is_out && runner.base_after == TaxaBase::Home {
                Some(runner.name.as_ref())
            } else {
                None
            }
        })
    }

    fn scores(&self) -> Vec<&str> {
        self.scores_iter().collect()
    }

    // A runner out is any runner where the final base is None
    // Every such runner must have a base_before of Some
    fn runners_out_iter(&self) -> impl Iterator<Item = Result<(&str, BaseNameVariant), MissingBaseDescriptionFormat>> {
        self.baserunners
            .iter()
            .filter(|runner| runner.is_out)
            .map(|runner| {
                let base_format = runner.base_description_format
                    .ok_or_else(|| MissingBaseDescriptionFormat::Out {
                        runner_name: runner.name.as_ref(),
                        prev_base: runner.base_before.into(),
                        out_at_base: runner.base_after,
                    })?;

                Ok((
                    runner.name.as_ref(),
                    TaxaBaseWithDescriptionFormat(runner.base_after, base_format).into(),
                ))
            })
    }

    fn runners_out(&self) -> Result<Vec<(&str, BaseNameVariant)>, MissingBaseDescriptionFormat> {
        self.runners_out_iter().collect()
    }

    pub fn to_parsed(&self) -> Result<ParsedEventMessage<&str>, ToParsedError> {
        let exactly_one_runner_out = || {
            let runners_out = self.runners_out()
                .map_err(ToParsedError::MissingBaseDescriptionFormat)?;
            
            match <[_; 1]>::try_from(runners_out) {
                Ok([runner]) => Ok(runner),
                Err(runners_out) => Err(ToParsedError::WrongNumberOfRunnersOut {
                    event_type: self.detail_type,
                    required: 1,
                    actual: runners_out.len(),
                })
            }
        };

        let exactly_one_fielder = || {
            match <[_; 1]>::try_from(self.fielders()) {
                Ok([fielder]) => Ok(fielder),
                Err(fielders) => Err(ToParsedError::WrongNumberOfFielders {
                    event_type: self.detail_type,
                    required: 1,
                    actual: fielders.len(),
                })
            }
        };
        
        let mandatory_fair_ball_type = || {
            Ok(self
                .fair_ball_type
                .ok_or_else(|| ToParsedError::MissingFairBallType { event_type: self.detail_type })?
                .into())
        };
        
        let mandatory_fair_ball_direction = || {
            Ok(self
                .fair_ball_direction
                .ok_or_else(|| ToParsedError::MissingFairBallDirection { event_type: self.detail_type })?
                .try_into()
                .map_err(|invalid_direction| ToParsedError::InvalidFairBallDirection { 
                    event_type: self.detail_type,
                    invalid_direction
                })?)
        };

        let mandatory_fielding_error_type = || {
            Ok(self
                .fielding_error_type
                .ok_or_else(|| ToParsedError::MissingFieldingErrorType { event_type: self.detail_type })?
                .into())
        };

        Ok(match self.detail_type {
            TaxaEventType::Ball => ParsedEventMessage::Ball {
                steals: self.steals(),
                count: self.count(),
            },
            TaxaEventType::CalledStrike => {
                let steals = self.steals();
                let caught_steals = steals.iter().filter(|s| s.caught).count();
                if self.outs_after > self.outs_before + (caught_steals as i32) {
                    ParsedEventMessage::StrikeOut {
                        foul: None,
                        batter: self.batter_name.as_ref(),
                        strike: StrikeType::Looking,
                        steals: self.steals(),
                    }
                } else {
                    ParsedEventMessage::Strike {
                        strike: StrikeType::Looking,
                        steals: self.steals(),
                        count: self.count(),
                    }
                }
            }
            TaxaEventType::SwingingStrike => {
                let steals = self.steals();
                let caught_steals = steals.iter().filter(|s| s.caught).count();
                if self.outs_after > self.outs_before + (caught_steals as i32) {
                    ParsedEventMessage::StrikeOut {
                        foul: None,
                        batter: self.batter_name.as_ref(),
                        strike: StrikeType::Swinging,
                        steals,
                    }
                } else {
                    ParsedEventMessage::Strike {
                        strike: StrikeType::Swinging,
                        steals,
                        count: self.count(),
                    }
                }
            }
            TaxaEventType::FoulTip => {
                let steals = self.steals();
                let caught_steals = steals.iter().filter(|s| s.caught).count();
                if self.outs_after > self.outs_before + (caught_steals as i32) {
                    ParsedEventMessage::StrikeOut {
                        foul: Some(FoulType::Tip),
                        batter: self.batter_name.as_ref(),
                        strike: StrikeType::Swinging,
                        steals,
                    }
                } else {
                    ParsedEventMessage::Foul {
                        foul: FoulType::Tip,
                        steals: self.steals(),
                        count: self.count(),
                    }
                }
            }
            TaxaEventType::FoulBall => ParsedEventMessage::Foul {
                foul: FoulType::Ball,
                steals: self.steals(),
                count: self.count(),
            },
            TaxaEventType::Hit => {
                ParsedEventMessage::BatterToBase {
                    batter: self.batter_name.as_ref(),
                    distance: match self.hit_type {
                        None => {
                            return Err(ToParsedError::MissingHitType {
                                event_type: self.detail_type,
                            })
                        }
                        Some(TaxaHitType::Single) => Distance::Single,
                        Some(TaxaHitType::Double) => Distance::Double,
                        Some(TaxaHitType::Triple) => Distance::Triple,
                    },
                    fair_ball_type: mandatory_fair_ball_type()?,
                    fielder: exactly_one_fielder()?,
                    scores: self.scores(),
                    advances: self.advances(false),
                }
            }
            TaxaEventType::ForceOut => {
                let (runner_out_name, runner_out_at_base) = exactly_one_runner_out()?;

                ParsedEventMessage::ForceOut {
                    batter: self.batter_name.as_ref(),
                    fielders: self.fielders(),
                    fair_ball_type: mandatory_fair_ball_type()?,
                    out: RunnerOut {
                        runner: runner_out_name,
                        base: runner_out_at_base,
                    },
                    scores: self.scores(),
                    advances: self.advances(true),
                }
            }
            TaxaEventType::CaughtOut => {
                let scores = self.scores();
                let fair_ball_type = mandatory_fair_ball_type()?;
                let is_fly = fair_ball_type != FairBallType::GroundBall;
                let sacrifice = is_fly && !scores.is_empty();

                let fielder = self.fielders.iter().exactly_one()
                    .map_err(|e| ToParsedError::WrongNumberOfFielders {
                        event_type: self.detail_type,
                        required: 1,
                        actual: e.len(),
                    })?;

                let caught_by = positioned_player_as_ref(&fielder);
                let perfect = fielder.is_perfect_catch
                    .ok_or_else(|| ToParsedError::MissingPerfectCatch {
                        event_type: self.detail_type,
                    })?;
                
                ParsedEventMessage::CaughtOut {
                    batter: self.batter_name.as_ref(),
                    fair_ball_type,
                    caught_by,
                    scores,
                    advances: self.advances(false),
                    sacrifice,
                    perfect,
                }
            }
            TaxaEventType::GroundedOut => ParsedEventMessage::GroundedOut {
                batter: self.batter_name.as_ref(),
                fielders: self.fielders(),
                scores: self.scores(),
                advances: self.advances(false),
            },
            TaxaEventType::Walk => ParsedEventMessage::Walk {
                batter: self.batter_name.as_ref(),
                scores: self.scores(),
                advances: self.advances(false),
            },
            TaxaEventType::HomeRun => {
                let scores = self.scores();
                let grand_slam = scores.len() == 3;
                ParsedEventMessage::HomeRun {
                    batter: self.batter_name.as_ref(),
                    fair_ball_type: mandatory_fair_ball_type()?,
                    destination: mandatory_fair_ball_direction()?,
                    scores,
                    grand_slam,
                }
            }
            TaxaEventType::FieldingError => {
                ParsedEventMessage::ReachOnFieldingError {
                    batter: self.batter_name.as_ref(),
                    fielder: exactly_one_fielder()?,
                    error: mandatory_fielding_error_type()?,
                    scores: self.scores(),
                    advances: self.advances(false),
                }
            }
            TaxaEventType::HitByPitch => ParsedEventMessage::HitByPitch {
                batter: self.batter_name.as_ref(),
                scores: self.scores(),
                advances: self.advances(false),
            },
            TaxaEventType::DoublePlay => {
                let scores = self.scores();
                let sacrifice = !scores.is_empty();
                let runners_out = self.runners_out().map_err(ToParsedError::MissingBaseDescriptionFormat)?;
                match &runners_out.as_slice() {
                    [(name, at_base)] => ParsedEventMessage::DoublePlayCaught {
                        batter: self.batter_name.as_ref(),
                        fair_ball_type: mandatory_fair_ball_type()?,
                        fielders: self.fielders(),
                        out_two: RunnerOut {
                            runner: name,
                            base: *at_base,
                        },
                        scores,
                        advances: self.advances(false),
                    },
                    [(name_one, base_one), (name_two, base_two)] => {
                        ParsedEventMessage::DoublePlayGrounded {
                            batter: self.batter_name.as_ref(),
                            fielders: self.fielders(),
                            out_one: RunnerOut {
                                runner: name_one,
                                base: *base_one,
                            },
                            out_two: RunnerOut {
                                runner: name_two,
                                base: *base_two,
                            },
                            scores,
                            advances: self.advances(true),
                            sacrifice,
                        }
                    }
                    _ => return Err(ToParsedError::WrongNumberOfRunnersOutInDoublePlay {
                        event_type: self.detail_type,
                        actual: runners_out.len(),
                    }),
                }
            }
            TaxaEventType::FieldersChoice => {
                let (runner_out_name, runner_out_at_base) = exactly_one_runner_out()?;

                ParsedEventMessage::ReachOnFieldersChoice {
                    batter: self.batter_name.as_ref(),
                    fielders: self.fielders(),
                    result: FieldingAttempt::Out {
                        out: RunnerOut {
                            runner: runner_out_name,
                            base: runner_out_at_base,
                        },
                    },
                    scores: self.scores(),
                    advances: self.advances(false),
                }
            }
            TaxaEventType::ErrorOnFieldersChoice => {
                let fielders = self.fielders();
                let fielder = fielders
                    .iter()
                    .exactly_one()
                    .map_err(|e| ToParsedError::WrongNumberOfFielders {
                        event_type: self.detail_type,
                        required: 1,
                        actual: e.len(),
                    })?
                    .name;

                ParsedEventMessage::ReachOnFieldersChoice {
                    batter: self.batter_name.as_ref(),
                    fielders,
                    result: FieldingAttempt::Error {
                        fielder,
                        error: mandatory_fielding_error_type()?,
                    },
                    scores: self.scores(),
                    advances: self.advances(true),
                }
            }
        })
    }

    pub fn to_parsed_contact(&self) -> Result<ParsedEventMessage<&str>, ToParsedContactError> {
        let mandatory_fair_ball_type = || {
            Ok(self
                .fair_ball_type
                .ok_or_else(|| ToParsedContactError::MissingFairBallType)?
                .into())
        };

        let mandatory_fair_ball_direction = || {
            Ok(self
                .fair_ball_direction
                .ok_or_else(|| ToParsedContactError::MissingFairBallDirection)?
                .try_into()
                .map_err(|invalid_direction| ToParsedContactError::InvalidFairBallDirection {
                    invalid_direction
                })?)
        };

        // We're going to construct a FairBall for this no matter
        // whether we had the type.
        Ok(ParsedEventMessage::FairBall {
            batter: self.batter_name.as_ref(),
            fair_ball_type: mandatory_fair_ball_type()?,
            destination: mandatory_fair_ball_direction()?,
        })
    }
}
