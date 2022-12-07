//! List of events(errors).

use crate::count::Count;

/// List of request handle results.
#[derive(Debug)]
pub enum Event {
    /// Connection Error to external db.
    ConnectError(String),

    /// Invalid bucket string.
    InvalidBucket(String),

    /// Invalid year.
    InvalidYear(String),

    /// Invalid month.
    InvalidMonth(String),

    /// Invalid day.
    InvalidDay(String),

    /// Invalid Date/Time
    InvalidDateTime(String),

    /// Count cache writer error
    UnableToUpdateCache(Count),

    /// Stale count cache
    CountCacheStale(Count),

    UnexpectedError(String),
}
