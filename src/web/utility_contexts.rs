use crate::models::DbGame;
use chrono::{NaiveDateTime, TimeZone, Utc};
use chrono_humanize::HumanTime;
use rocket::serde::Serialize;

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
pub struct GameContext {
    uri: String,
    season: i32,
    day: i32,
    away_team_emoji: String,
    away_team_name: String,
    away_team_id: String,
    home_team_emoji: String,
    home_team_name: String,
    home_team_id: String,
    num_warnings: i64,
    num_errors: i64,
    num_critical: i64,
}

impl GameContext {
    pub fn from_db(
        games: impl IntoIterator<Item = (DbGame, i64, i64, i64)>,
        uri_builder: impl Fn(i64) -> String,
    ) -> Vec<Self> {
        games
            .into_iter()
            .map(
                |(game, num_warnings, num_errors, num_critical)| GameContext {
                    uri: uri_builder(game.id),
                    season: game.season,
                    day: game.day,
                    away_team_emoji: game.away_team_emoji,
                    away_team_name: game.away_team_name,
                    away_team_id: game.away_team_id,
                    home_team_emoji: game.home_team_emoji,
                    home_team_name: game.home_team_name,
                    home_team_id: game.home_team_id,
                    num_warnings,
                    num_errors,
                    num_critical,
                },
            )
            .collect()
    }
}
