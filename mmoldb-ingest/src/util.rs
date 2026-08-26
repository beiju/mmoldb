use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use mmolb_parsing::NotRecognized;
use mmolb_parsing::enums::Day;
use mmoldb_db::taxa::{Taxa, TaxaDayType};
use tracing::error;

pub(crate) const fn datetime_from_parts(
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    min: u32,
    sec: u32,
    micro: u32,
) -> DateTime<Utc> {
    NaiveDateTime::new(
        NaiveDate::from_ymd_opt(year, month, day).unwrap(),
        NaiveTime::from_hms_micro_opt(hour, min, sec, micro).unwrap(),
    )
    .and_utc()
}

pub fn day_to_db(
    day: Option<&Result<Day, NotRecognized>>,
    taxa: &Taxa,
) -> (Option<i64>, Option<i32>, Option<i32>) {
    match day {
        None => (None, None, None),
        Some(Ok(Day::Preseason)) => (Some(taxa.day_type_id(TaxaDayType::Preseason)), None, None),
        Some(Ok(Day::SuperstarBreak)) => (
            Some(taxa.day_type_id(TaxaDayType::SuperstarBreak)),
            None,
            None,
        ),
        Some(Ok(Day::PostseasonPreview)) => (
            Some(taxa.day_type_id(TaxaDayType::PostseasonPreview)),
            None,
            None,
        ),
        Some(Ok(Day::PostseasonRound(1))) => (
            Some(taxa.day_type_id(TaxaDayType::PostseasonRound1)),
            None,
            None,
        ),
        Some(Ok(Day::PostseasonRound(2))) => (
            Some(taxa.day_type_id(TaxaDayType::PostseasonRound2)),
            None,
            None,
        ),
        Some(Ok(Day::PostseasonRound(3))) => (
            Some(taxa.day_type_id(TaxaDayType::PostseasonRound3)),
            None,
            None,
        ),
        Some(Ok(Day::PostseasonRound(other))) => {
            error!("Unexpected postseason day {other} (expected 1-3)");
            (None, None, None)
        }
        Some(Ok(Day::Election)) => (Some(taxa.day_type_id(TaxaDayType::Election)), None, None),
        Some(Ok(Day::Holiday)) => (Some(taxa.day_type_id(TaxaDayType::Holiday)), None, None),
        Some(Ok(Day::Day(day))) => (
            Some(taxa.day_type_id(TaxaDayType::RegularDay)),
            Some(*day as i32),
            None,
        ),
        Some(Ok(Day::SuperstarGame)) => (
            Some(taxa.day_type_id(TaxaDayType::SuperstarDay)),
            None,
            None,
        ),
        Some(Ok(Day::SuperstarDay(day))) => (
            Some(taxa.day_type_id(TaxaDayType::SuperstarDay)),
            None,
            Some(*day as i32),
        ),
        Some(Ok(Day::Event)) => (Some(taxa.day_type_id(TaxaDayType::Event)), None, None),
        Some(Ok(Day::SpecialEvent)) => (
            Some(taxa.day_type_id(TaxaDayType::SpecialEvent)),
            None,
            None,
        ),
        Some(Ok(Day::Offseason)) => (
            Some(taxa.day_type_id(TaxaDayType::Offseason)),
            None,
            None, // In this context, offseason day isn't available
        ),
        Some(Err(err)) => {
            error!("Unrecognized day {err}");
            (None, None, None)
        }
    }
}