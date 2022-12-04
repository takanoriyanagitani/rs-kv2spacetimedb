//! Simple Day(1,2,3, ..., 31)

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

#[cfg(test)]
mod test_day {

    mod day {

        use crate::day::Day;

        #[test]
        fn test_min() {
            let d: Day = Day::try_from(1).unwrap();
            assert_eq!(d.as_raw(), 1);
        }

        #[test]
        fn test_max() {
            let d: Day = Day::try_from(31).unwrap();
            assert_eq!(d.as_raw(), 31);
        }

        #[test]
        fn test_under() {
            let r: Result<_, _> = Day::try_from(0);
            assert_eq!(r.is_err(), true);
        }

        #[test]
        fn test_over() {
            let r: Result<_, _> = Day::try_from(32);
            assert_eq!(r.is_err(), true);
        }
    }
}
