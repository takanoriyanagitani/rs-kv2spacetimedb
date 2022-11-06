use crate::evt::Event;

/// Simple "Day" like number(1..=31) which does not care a month nor a leap year.
pub struct Day {
    d: u8,
}

impl Day {
    pub fn as_raw(&self) -> u8 {
        self.d
    }
}

/// Simple conversion from u8 which does not care the month.
impl TryFrom<u8> for Day {
    type Error = Event;
    fn try_from(d: u8) -> Result<Self, Self::Error> {
        match d {
            1..=31 => Ok(Self { d }),
            _ => Err(Event::InvalidDay(format!("Invalid day number: {}", d))),
        }
    }
}
