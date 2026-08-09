use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};

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