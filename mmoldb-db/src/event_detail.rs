use crate::taxa::{
    AsInsertable, TaxaBase, TaxaBaseDescriptionFormat, TaxaBaseWithDescriptionFormat,
    TaxaEventType, TaxaFairBallType, TaxaFielderLocation, TaxaFieldingErrorType, TaxaPitchType,
    TaxaSlot,
};
use itertools::Itertools;
use miette::Diagnostic;
use mmolb_parsing::ParsedEventMessage;
use mmolb_parsing::enums::{
    Base, BaseNameVariant, Distance, FairBallDestination, FieldingErrorType, FoulType, StrikeType,
};
use mmolb_parsing::parsed_event::{
    BaseSteal, Cheer, Ejection, FieldingAttempt, KnownBug, PlacedPlayer, RunnerAdvance, RunnerOut,
    SnappedPhotos,
};
use std::fmt::Formatter;
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct EventDetailRunner<StrT: Clone> {
    pub name: StrT,
    pub base_before: Option<TaxaBase>,
    pub base_after: TaxaBase,
    pub is_out: bool,
    pub base_description_format: Option<TaxaBaseDescriptionFormat>,
    pub is_steal: bool,
    pub source_event_index: Option<i32>,
    pub is_earned: bool,
}

#[derive(Debug, Clone)]
pub struct EventDetailFielder<StrT: Clone> {
    pub name: StrT,
    pub slot: TaxaSlot,
}

#[derive(Debug, Clone)]
pub struct EventDetail<StrT: Clone> {
    pub game_event_index: usize,
    pub fair_ball_event_index: Option<usize>,
    pub inning: u8,
    pub top_of_inning: bool,
    pub balls_before: u8,
    pub strikes_before: u8,
    pub outs_before: i32,
    pub outs_after: i32,
    pub errors_before: i32,
    pub errors_after: i32,
    pub away_team_score_before: u8,
    pub away_team_score_after: u8,
    pub home_team_score_before: u8,
    pub home_team_score_after: u8,
    pub pitcher_name: StrT,
    pub batter_name: StrT,
    pub fielders: Vec<EventDetailFielder<StrT>>,

    pub detail_type: TaxaEventType,
    pub hit_base: Option<TaxaBase>,
    pub fair_ball_type: Option<TaxaFairBallType>,
    pub fair_ball_direction: Option<TaxaFielderLocation>,
    pub fielding_error_type: Option<TaxaFieldingErrorType>,
    pub pitch_type: Option<TaxaPitchType>,
    pub pitch_speed: Option<f64>,
    pub pitch_zone: Option<i32>,
    pub described_as_sacrifice: Option<bool>,
    pub is_toasty: Option<bool>,

    pub baserunners: Vec<EventDetailRunner<StrT>>,
    pub pitcher_count: i32,
    pub batter_count: i32,
    pub batter_subcount: i32,
    pub cheer: Option<Cheer>,
    pub aurora_photos: Option<SnappedPhotos<StrT>>,
    pub ejection: Option<Ejection<StrT>>,
    pub fair_ball_ejection: Option<Ejection<StrT>>,
}

#[derive(Debug)]
pub struct IngestLog {
    pub game_event_index: i32,
    pub log_level: i32,
    pub log_text: String,
}

fn placed_player_as_ref<StrT: AsRef<str> + Clone>(
    p: &EventDetailFielder<StrT>,
) -> PlacedPlayer<&str> {
    PlacedPlayer {
        name: p.name.as_ref(),
        place: p.slot.into(),
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
            None => MaybeBase::NoBase,
            Some(b) => MaybeBase::Base(b),
        }
    }
}

#[derive(Debug, Error, Diagnostic)]
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

#[derive(Debug, Error, Diagnostic)]
pub enum ToParsedError<'g> {
    #[error(transparent)]
    // Note: Can't use #[from] because of the lifetime
    MissingBaseDescriptionFormat(MissingBaseDescriptionFormat<'g>),

    #[error("{event_type} must have a fair_ball_type")]
    MissingFairBallType { event_type: TaxaEventType },

    #[error("{event_type} must have a fair_ball_direction")]
    MissingFairBallDirection { event_type: TaxaEventType },

    #[error("{event_type} must have a fielding_error_type")]
    MissingFieldingErrorType { event_type: TaxaEventType },

    #[error("{event_type} must have a hit_base")]
    MissingHitBase { event_type: TaxaEventType },

    #[error("{event_type} hit_base must be {expected}, but it was {hit_base}")]
    InvalidHitBase {
        event_type: TaxaEventType,
        hit_base: TaxaBase,
        expected: &'static str,
    },

    #[error("{event_type} must have Some described_as_sacrifice")]
    MissingDescribedAsSacrifice { event_type: TaxaEventType },

    #[error("{event_type} with {runners_out} runners out must have Some described_as_sacrifice")]
    MissingDescribedAsSacrificeForRunnersOut {
        event_type: TaxaEventType,
        runners_out: usize,
    },

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

    #[error("{event_type} must have a non-null is_toasty")]
    MissingIsToasty { event_type: TaxaEventType },
}

#[derive(Debug, Error, Diagnostic)]
pub enum ToParsedContactError {
    #[error("Event with a fair_ball_index must have a fair_ball_type")]
    MissingFairBallType,

    #[error("Event with a fair_ball_index must have a fair_ball_direction")]
    MissingFairBallDirection,
}

impl<StrT: AsRef<str> + Clone> EventDetail<StrT> {
    fn count(&self) -> (u8, u8) {
        let props = self.detail_type.as_insertable();
        if props.ends_plate_appearance {
            (0, 0)
        } else if self.detail_type == TaxaEventType::FoulBall && self.strikes_before == 2 {
            (self.balls_before, self.strikes_before)
        } else {
            (
                self.balls_before + if props.is_ball { 1 } else { 0 },
                self.strikes_before + if props.is_strike { 1 } else { 0 },
            )
        }
    }

    fn fielders_iter(&self) -> impl Iterator<Item = PlacedPlayer<&str>> {
        self.fielders.iter().map(placed_player_as_ref)
    }

    fn fielders(&self) -> Vec<PlacedPlayer<&str>> {
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
    fn runners_out_iter(
        &self,
    ) -> impl Iterator<Item = Result<(&str, BaseNameVariant), MissingBaseDescriptionFormat>> {
        self.baserunners
            .iter()
            .filter(|runner| runner.is_out)
            .map(|runner| {
                let base_format = runner.base_description_format.ok_or_else(|| {
                    MissingBaseDescriptionFormat::Out {
                        runner_name: runner.name.as_ref(),
                        prev_base: runner.base_before.into(),
                        out_at_base: runner.base_after,
                    }
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
            let runners_out = self
                .runners_out()
                .map_err(ToParsedError::MissingBaseDescriptionFormat)?;

            match <[_; 1]>::try_from(runners_out) {
                Ok([runner]) => Ok(runner),
                Err(runners_out) => Err(ToParsedError::WrongNumberOfRunnersOut {
                    event_type: self.detail_type,
                    required: 1,
                    actual: runners_out.len(),
                }),
            }
        };

        // Kind of a roundabout way to do it, but it works
        let at_most_one_runner_out = || match exactly_one_runner_out() {
            Ok(runner) => Ok(Some(runner)),
            Err(ToParsedError::WrongNumberOfRunnersOut { actual, .. }) if actual == 0 => Ok(None),
            Err(err) => Err(err),
        };

        let exactly_one_fielder = || -> Result<PlacedPlayer<&str>, ToParsedError> {
            match <[_; 1]>::try_from(self.fielders()) {
                Ok([fielder]) => Ok(fielder),
                Err(fielders) => Err(ToParsedError::WrongNumberOfFielders {
                    event_type: self.detail_type,
                    required: 1,
                    actual: fielders.len(),
                }),
            }
        };

        let mandatory_fair_ball_type = || {
            Ok(self
                .fair_ball_type
                .ok_or_else(|| ToParsedError::MissingFairBallType {
                    event_type: self.detail_type,
                })?
                .into())
        };

        let mandatory_fair_ball_direction = || -> Result<FairBallDestination, ToParsedError> {
            Ok((self.fair_ball_direction.ok_or_else(|| {
                ToParsedError::MissingFairBallDirection {
                    event_type: self.detail_type,
                }
            })?)
            .into())
        };

        let mandatory_fielding_error_type = || -> Result<FieldingErrorType, ToParsedError> {
            Ok(self
                .fielding_error_type
                .ok_or_else(|| ToParsedError::MissingFieldingErrorType {
                    event_type: self.detail_type,
                })?
                .into())
        };

        Ok(match self.detail_type {
            TaxaEventType::Ball => ParsedEventMessage::Ball {
                steals: self.steals(),
                count: self.count(),
                cheer: self.cheer.clone(),
                aurora_photos: self.aurora_photos.as_ref().map(SnappedPhotos::as_ref),
                ejection: self.ejection.as_ref().map(Ejection::as_ref),
            },
            TaxaEventType::CalledStrike => ParsedEventMessage::Strike {
                strike: StrikeType::Looking,
                steals: self.steals(),
                count: self.count(),
                cheer: self.cheer.clone(),
                aurora_photos: self.aurora_photos.as_ref().map(SnappedPhotos::as_ref),
                ejection: self.ejection.as_ref().map(Ejection::as_ref),
            },
            TaxaEventType::CalledStrikeout => ParsedEventMessage::StrikeOut {
                foul: None,
                batter: self.batter_name.as_ref(),
                strike: StrikeType::Looking,
                steals: self.steals(),
                cheer: self.cheer.clone(),
                aurora_photos: self.aurora_photos.as_ref().map(SnappedPhotos::as_ref),
                ejection: self.ejection.as_ref().map(Ejection::as_ref),
            },
            TaxaEventType::SwingingStrike => ParsedEventMessage::Strike {
                strike: StrikeType::Swinging,
                steals: self.steals(),
                count: self.count(),
                cheer: self.cheer.clone(),
                aurora_photos: self.aurora_photos.as_ref().map(SnappedPhotos::as_ref),
                ejection: self.ejection.as_ref().map(Ejection::as_ref),
            },
            TaxaEventType::SwingingStrikeout => ParsedEventMessage::StrikeOut {
                foul: None,
                batter: self.batter_name.as_ref(),
                strike: StrikeType::Swinging,
                steals: self.steals(),
                cheer: self.cheer.clone(),
                aurora_photos: self.aurora_photos.as_ref().map(SnappedPhotos::as_ref),
                ejection: self.ejection.as_ref().map(Ejection::as_ref),
            },
            TaxaEventType::FoulTip => ParsedEventMessage::Foul {
                foul: FoulType::Tip,
                steals: self.steals(),
                count: self.count(),
                cheer: self.cheer.clone(),
                aurora_photos: self.aurora_photos.as_ref().map(SnappedPhotos::as_ref),
            },
            TaxaEventType::FoulTipStrikeout => ParsedEventMessage::StrikeOut {
                foul: Some(FoulType::Tip),
                batter: self.batter_name.as_ref(),
                strike: StrikeType::Swinging,
                steals: self.steals(),
                cheer: self.cheer.clone(),
                aurora_photos: self.aurora_photos.as_ref().map(SnappedPhotos::as_ref),
                ejection: self.ejection.as_ref().map(Ejection::as_ref),
            },
            TaxaEventType::FoulBall => ParsedEventMessage::Foul {
                foul: FoulType::Ball,
                steals: self.steals(),
                count: self.count(),
                cheer: self.cheer.clone(),
                aurora_photos: self.aurora_photos.as_ref().map(SnappedPhotos::as_ref),
            },
            TaxaEventType::Hit => ParsedEventMessage::BatterToBase {
                batter: self.batter_name.as_ref(),
                distance: match self.hit_base {
                    None => {
                        return Err(ToParsedError::MissingHitBase {
                            event_type: self.detail_type,
                        });
                    }
                    Some(TaxaBase::First) => Distance::Single,
                    Some(TaxaBase::Second) => Distance::Double,
                    Some(TaxaBase::Third) => Distance::Triple,
                    Some(other) => {
                        return Err(ToParsedError::InvalidHitBase {
                            event_type: self.detail_type,
                            hit_base: other,
                            expected: "First, Second, or Third",
                        });
                    }
                },
                fair_ball_type: mandatory_fair_ball_type()?,
                fielder: exactly_one_fielder()?,
                scores: self.scores(),
                advances: self.advances(false),
                ejection: self.ejection.as_ref().map(Ejection::as_ref),
            },
            TaxaEventType::ForceOut => {
                let (runner_out_name, runner_out_at_base) = exactly_one_runner_out()?;

                let any_existing_runner_advanced = self.baserunners.iter().any(|r| {
                    r.base_before.is_some() && // Exclude the batter-runner
                        r.base_before != Some(r.base_after) && // Exclude anyone who stayed still
                        !r.is_out && // Exclude anyone who got out
                        r.base_after != TaxaBase::Home // Exclude anyone who scored
                });

                ParsedEventMessage::ForceOut {
                    batter: self.batter_name.as_ref(),
                    fielders: self.fielders(),
                    fair_ball_type: mandatory_fair_ball_type()?,
                    out: RunnerOut {
                        runner: runner_out_name,
                        base: runner_out_at_base,
                    },
                    scores: self.scores(),
                    // The batter-runner is listed in advances if any other runner also advanced,
                    // unless the force out was at 2nd
                    advances: self.advances(
                        any_existing_runner_advanced && Base::Second != runner_out_at_base.into(),
                    ),
                    ejection: self.ejection.as_ref().map(Ejection::as_ref),
                }
            }
            TaxaEventType::CaughtOut => {
                let scores = self.scores();
                let fair_ball_type = mandatory_fair_ball_type()?;
                let sacrifice = self.described_as_sacrifice.ok_or_else(|| {
                    ToParsedError::MissingDescribedAsSacrifice {
                        event_type: self.detail_type,
                    }
                })?;

                let fielder = self.fielders.iter().exactly_one().map_err(|e| {
                    ToParsedError::WrongNumberOfFielders {
                        event_type: self.detail_type,
                        required: 1,
                        actual: e.len(),
                    }
                })?;

                let caught_by = placed_player_as_ref(&fielder);
                let perfect = self
                    .is_toasty
                    .ok_or_else(|| ToParsedError::MissingIsToasty {
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
                    ejection: self.ejection.as_ref().map(Ejection::as_ref),
                }
            }
            TaxaEventType::GroundedOut => ParsedEventMessage::GroundedOut {
                batter: self.batter_name.as_ref(),
                fielders: self.fielders(),
                scores: self.scores(),
                advances: self.advances(false),
                perfect: self
                    .is_toasty
                    .ok_or_else(|| ToParsedError::MissingIsToasty {
                        event_type: self.detail_type,
                    })?,
                ejection: self.ejection.as_ref().map(Ejection::as_ref),
            },
            TaxaEventType::Walk => ParsedEventMessage::Walk {
                batter: self.batter_name.as_ref(),
                scores: self.scores(),
                advances: self.advances(false),
                cheer: self.cheer.clone(),
                aurora_photos: self.aurora_photos.as_ref().map(SnappedPhotos::as_ref),
                ejection: self.ejection.as_ref().map(Ejection::as_ref),
            },
            TaxaEventType::HomeRun => {
                let mut scores = self.scores();
                // I add the batter to `scores` (because they do), but MMOLDB doesn't list
                // them, so I need to take them out to round-trip successfully. The batter
                // is always the last entry in scores.
                // If I supported emitting warnings from this function, I would emit one if
                // there's no scores or if the last score doesn't match the batter's name.
                scores.pop();

                match self.hit_base {
                    None => {
                        return Err(ToParsedError::MissingHitBase {
                            event_type: self.detail_type,
                        });
                    }
                    Some(TaxaBase::Home) => {}
                    Some(other) => {
                        return Err(ToParsedError::InvalidHitBase {
                            event_type: self.detail_type,
                            hit_base: other,
                            expected: "Home",
                        });
                    }
                }

                let grand_slam = scores.len() == 3;
                ParsedEventMessage::HomeRun {
                    batter: self.batter_name.as_ref(),
                    fair_ball_type: mandatory_fair_ball_type()?,
                    destination: mandatory_fair_ball_direction()?,
                    scores,
                    grand_slam,
                    ejection: self.ejection.as_ref().map(Ejection::as_ref),
                }
            }
            TaxaEventType::FieldingError => ParsedEventMessage::ReachOnFieldingError {
                batter: self.batter_name.as_ref(),
                fielder: exactly_one_fielder()?,
                error: mandatory_fielding_error_type()?,
                scores: self.scores(),
                advances: self.advances(false),
                ejection: self.ejection.as_ref().map(Ejection::as_ref),
            },
            TaxaEventType::HitByPitch => ParsedEventMessage::HitByPitch {
                batter: self.batter_name.as_ref(),
                scores: self.scores(),
                advances: self.advances(false),
                cheer: self.cheer.clone(),
                aurora_photos: self.aurora_photos.as_ref().map(SnappedPhotos::as_ref),
                ejection: self.ejection.as_ref().map(Ejection::as_ref),
            },
            TaxaEventType::DoublePlay => {
                let scores = self.scores();
                let runners_out = self
                    .runners_out()
                    .map_err(ToParsedError::MissingBaseDescriptionFormat)?;
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
                        ejection: self.ejection.as_ref().map(Ejection::as_ref),
                    },
                    [(name_one, base_one), (name_two, base_two)] => {
                        let sacrifice = self.described_as_sacrifice.ok_or_else(|| {
                            ToParsedError::MissingDescribedAsSacrificeForRunnersOut {
                                event_type: self.detail_type,
                                runners_out: 2,
                            }
                        })?;

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
                            ejection: self.ejection.as_ref().map(Ejection::as_ref),
                        }
                    }
                    _ => {
                        return Err(ToParsedError::WrongNumberOfRunnersOutInDoublePlay {
                            event_type: self.detail_type,
                            actual: runners_out.len(),
                        });
                    }
                }
            }
            TaxaEventType::FieldersChoice => {
                if let Some((runner_out_name, runner_out_at_base)) = at_most_one_runner_out()? {
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
                        ejection: self.ejection.as_ref().map(Ejection::as_ref),
                    }
                } else {
                    let fielder = exactly_one_fielder()?;

                    ParsedEventMessage::KnownBug {
                        bug: KnownBug::FirstBasemanChoosesAGhost {
                            batter: self.batter_name.as_ref(),
                            first_baseman: fielder.name,
                        },
                    }
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
                    advances: self.advances(false),
                    ejection: self.ejection.as_ref().map(Ejection::as_ref),
                }
            }
            TaxaEventType::Balk => ParsedEventMessage::Balk {
                pitcher: self.pitcher_name.as_ref(),
                scores: self.scores(),
                advances: self.advances(false),
            },
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
                .into())
        };

        // We're going to construct a FairBall for this no matter
        // whether we had the type.
        Ok(ParsedEventMessage::FairBall {
            batter: self.batter_name.as_ref(),
            fair_ball_type: mandatory_fair_ball_type()?,
            destination: mandatory_fair_ball_direction()?,
            cheer: self.cheer.clone(),
            aurora_photos: self.aurora_photos.as_ref().map(SnappedPhotos::as_ref),
            ejection: self.fair_ball_ejection.as_ref().map(Ejection::as_ref),
        })
    }
}
