//! Simple Year(1,2,3, ... , 65535)

use crate::evt::Event;

pub struct Year {
    y: u16,
}

impl Year {
    pub fn as_raw(&self) -> u16 {
        self.y
    }
}

impl TryFrom<u16> for Year {
    type Error = Event;

    fn try_from(y: u16) -> Result<Self, Self::Error> {
        match y {
            0 => Err(Event::InvalidYear(format!("Invalid year number: {}", y))),
            _ => Ok(Year { y }),
        }
    }
}

#[cfg(test)]
mod test_year {

    mod try_from {

        use crate::year;

        #[test]
        fn test_invalid() {
            let r: Result<_, _> = year::Year::try_from(0);
            assert_eq!(r.is_err(), true);
        }
    }
}
