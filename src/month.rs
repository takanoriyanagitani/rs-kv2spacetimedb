use crate::evt::Event;

#[derive(Clone, Copy)]
pub enum Month {
    Jan,
    Feb,
    Mar,
    Apr,
    May,
    Jun,
    Jul,
    Aug,
    Sep,
    Oct,
    Nov,
    Dec,
}

impl Month {
    pub fn as_raw(&self) -> u8 {
        u8::from(*self)
    }
}

impl From<Month> for u8 {
    fn from(m: Month) -> u8 {
        match m {
            Month::Jan => 1,
            Month::Feb => 2,
            Month::Mar => 3,
            Month::Apr => 4,
            Month::May => 5,
            Month::Jun => 6,
            Month::Jul => 7,
            Month::Aug => 8,
            Month::Sep => 9,
            Month::Oct => 10,
            Month::Nov => 11,
            Month::Dec => 12,
        }
    }
}

impl TryFrom<u8> for Month {
    type Error = Event;
    fn try_from(u: u8) -> Result<Self, Self::Error> {
        match u {
            1 => Ok(Self::Jan),
            2 => Ok(Self::Feb),
            3 => Ok(Self::Mar),
            4 => Ok(Self::Apr),
            5 => Ok(Self::May),
            6 => Ok(Self::Jun),
            7 => Ok(Self::Jul),
            8 => Ok(Self::Aug),
            9 => Ok(Self::Sep),
            10 => Ok(Self::Oct),
            11 => Ok(Self::Nov),
            12 => Ok(Self::Dec),
            _ => Err(Event::InvalidMonth(format!("Invalid month number: {}", u))),
        }
    }
}
