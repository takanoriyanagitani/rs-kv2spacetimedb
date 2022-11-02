/// List of request handle results.
#[derive(Debug)]
pub enum Event {
    /// Connection Error to external db.
    ConnectError(String),

    UnexpectedError(String),
}
