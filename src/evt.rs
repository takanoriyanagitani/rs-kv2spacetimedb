#[derive(Debug)]
pub enum Event {
    ConnectError(String),
    UnexpectedError(String),
}
