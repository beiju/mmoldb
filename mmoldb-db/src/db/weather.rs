use crate::db::GameForDb;
use crate::models::{DbWeather, NewWeather};
use diesel::connection::DefaultLoadingMode;
use diesel::prelude::*;
use diesel::{QueryResult, RunQueryDsl};
use hashbrown::{Equivalent, HashMap};
use itertools::Itertools;
use log::warn;

// Uses hashbrown::HashMap because the std HashMap has a limitation on
// tuples of references
#[derive(Debug, Hash, Eq, PartialEq)]
pub(super) struct UniqueWeather {
    name: String,
    emoji: String,
    tooltip: String,
}

impl Into<UniqueWeather> for DbWeather {
    fn into(self) -> UniqueWeather {
        UniqueWeather {
            name: self.name,
            emoji: self.emoji,
            tooltip: self.tooltip,
        }
    }
}

impl Equivalent<UniqueWeather> for (&str, &str, &str) {
    fn equivalent(&self, key: &UniqueWeather) -> bool {
        self.0 == key.name && self.1 == key.emoji && self.2 == key.tooltip
    }
}

pub(super) type WeatherTable = HashMap<UniqueWeather, i64>;

pub(super) fn create_weather_table(
    conn: &mut PgConnection,
    games: &[GameForDb],
) -> QueryResult<WeatherTable> {
    let weather_table = loop {
        match create_weather_table_inner(conn, games)? {
            OrRetry::Result(weather_table) => break weather_table,
            OrRetry::Retry => {
                warn!("Unique constraint violation while adding a new weather. Trying again.");
            }
        };
    };

    Ok(weather_table)
}

enum OrRetry<T> {
    Result(T),
    Retry,
}

fn create_weather_table_inner(
    conn: &mut PgConnection,
    games: &[GameForDb],
) -> QueryResult<OrRetry<WeatherTable>> {
    use crate::data_schema::data::weather::dsl as weather_dsl;
    // Get or create weather
    // Note: Based on what I read online, trying an insert and catching unique
    // violations causes db bloat. There are (surprisingly complicated) ways to
    // do a get-or-create in one query, but for simplicity I'm just going to
    // do it as two queries and rely on the transaction to handle conflicts

    // Get all weathers. Weather table shouldn't be too big so this
    // should be fine. TODO Cache weather LUT
    let mut weather_table = weather_dsl::weather
        .select(DbWeather::as_select())
        .load_iter::<DbWeather, DefaultLoadingMode>(conn)?
        .map_ok(|weather| {
            let id = weather.id; // Lifetime reasons
            (weather.into(), id)
        })
        .collect::<QueryResult<HashMap<UniqueWeather, i64>>>()?;

    let new_weathers = games
        .iter()
        .map(GameForDb::raw)
        .filter_map(|(_, raw_game)| {
            if weather_table.contains_key(&(
                raw_game.weather.name.as_str(),
                raw_game.weather.emoji.as_str(),
                raw_game.weather.tooltip.as_str(),
            )) {
                None
            } else {
                Some(NewWeather {
                    name: &raw_game.weather.name,
                    emoji: &raw_game.weather.emoji,
                    tooltip: &raw_game.weather.tooltip,
                })
            }
        })
        .unique_by(|w| (w.name, w.emoji, w.tooltip))
        .collect_vec();

    // Note: This check not just for optimization, it's also necessary for
    // correctness. The code inside the if block assumes that an empty
    // inserted_weathers indicates a constraint conflict, which is not true if
    // new_weathers is also empty.
    if !new_weathers.is_empty() {
        let inserted_weathers = diesel::insert_into(weather_dsl::weather)
            .values(&new_weathers)
            .on_conflict_do_nothing()
            .returning(DbWeather::as_returning())
            .get_results::<DbWeather>(conn)?;

        // The behavior of "on conflict do nothing" is to return no values when
        // there was a conflict
        if inserted_weathers.is_empty() {
            return Ok(OrRetry::Retry);
        }

        weather_table.extend(inserted_weathers.into_iter().map(|weather| {
            let id = weather.id; // Lifetime reasons
            (weather.into(), id)
        }));
    }

    Ok(OrRetry::Result(weather_table))
}
