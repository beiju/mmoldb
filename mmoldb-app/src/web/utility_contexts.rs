use chrono::{NaiveDateTime, TimeZone, Utc};
use chrono_humanize::HumanTime;
use log::warn;
use rocket::serde::Serialize;
use mmoldb_db::db::GameWithIssueCounts;

#[derive(Debug, Clone, Serialize)]
pub struct FormattedDateContext {
    relative: String,
    absolute: String,
}

impl From<&NaiveDateTime> for FormattedDateContext {
    fn from(value: &NaiveDateTime) -> Self {
        let value_utc = Utc.from_utc_datetime(value);
        let human = HumanTime::from(value_utc);

        Self {
            relative: human.to_string(),
            absolute: value_utc.to_string(),
        }
    }
}

#[derive(Serialize)]
pub struct DayContext {
    day: Option<i32>,
    is_superstar_day: bool,
}

// From a (day, superstar_day) pair
impl From<(Option<i32>, Option<i32>)> for DayContext {
    fn from(value: (Option<i32>, Option<i32>)) -> Self {
        match value {
            (None, None) => DayContext {
                day: None,
                is_superstar_day: false,
            },
            (Some(day), None) => DayContext {
                day: Some(day),
                is_superstar_day: false,
            },
            (None, Some(day)) => DayContext {
                day: Some(day),
                is_superstar_day: true,
            },
            (Some(normal_day), Some(superstar_day)) => {
                warn!("Game had both `day` {normal_day} and `superstar_day` {superstar_day} set.");
                // I guess use the superstar day? For no particular reason.
                DayContext {
                    day: Some(superstar_day),
                    is_superstar_day: true,
                }
            }
        }
    }
}

#[derive(Serialize)]
pub struct GameContext {
    uri: String,
    season: i32,
    day: DayContext,
    away_team_emoji: String,
    away_team_name: String,
    away_team_mmolb_id: String,
    home_team_emoji: String,
    home_team_name: String,
    home_team_mmolb_id: String,
    num_warnings: i64,
    num_errors: i64,
    num_critical: i64,
}

impl GameContext {
    pub fn from_db(
        games: impl IntoIterator<Item = GameWithIssueCounts>,
        uri_builder: impl Fn(&str) -> String,
    ) -> Vec<Self> {
        games
            .into_iter()
            .map(|g| GameContext {
                uri: uri_builder(&g.game.mmolb_game_id),
                season: g.game.season,
                day: (g.game.day, g.game.superstar_day).into(),
                away_team_emoji: g.game.away_team_emoji,
                away_team_name: g.game.away_team_name,
                away_team_mmolb_id: g.game.away_team_mmolb_id,
                home_team_emoji: g.game.home_team_emoji,
                home_team_name: g.game.home_team_name,
                home_team_mmolb_id: g.game.home_team_mmolb_id,
                num_warnings: g.warnings_count,
                num_errors: g.errors_count,
                num_critical: g.critical_count,
            })
            .collect()
    }
}
