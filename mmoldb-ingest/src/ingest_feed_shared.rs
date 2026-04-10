use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use mmolb_parsing::feed_event::FeedEvent;
use mmolb_parsing::player::Deserialize;

pub(crate) const fn datetime_from_parts(year: i32, month: u32, day: u32, hour: u32, min: u32, sec: u32, micro: u32) -> DateTime<Utc> {
    NaiveDateTime::new(
        NaiveDate::from_ymd_opt(year, month, day).unwrap(),
        NaiveTime::from_hms_micro_opt(hour, min, sec, micro).unwrap()
    ).and_utc()
}

pub(crate) const FEED_INVERSION_EVENT_START: DateTime<Utc> = datetime_from_parts(2026, 03, 29, 06, 32, 29, 494640);
pub(crate) const FEED_INVERSION_EVENT_END: DateTime<Utc> = datetime_from_parts(2026, 03, 29, 09, 15, 58, 247522);

#[derive(Debug, Deserialize)]
pub struct FeedItemContainer {
    pub feed_event_index: i32,
    pub data: FeedEvent,
    pub prev_valid_from: Option<DateTime<Utc>>,
    pub prev_data: Option<FeedEvent>,
}