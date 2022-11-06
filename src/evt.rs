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

    UnexpectedError(String),
}
