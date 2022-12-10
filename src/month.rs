//! Simple Month(Jan, Feb, Mar, ... , Dec)

use crate::evt::Event;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
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

#[cfg(test)]
mod test_month {

    mod month {
        use crate::month::Month;

        #[test]
        fn test_min() {
            let m: Month = Month::try_from(1).unwrap();
            assert_eq!(m, Month::Jan);
        }

        #[test]
        fn test_max() {
            let m: Month = Month::try_from(12).unwrap();
            assert_eq!(m, Month::Dec);
        }

        #[test]
        fn test_conv() {
            assert_eq!(Month::try_from(1).unwrap(), Month::Jan);
            assert_eq!(Month::try_from(2).unwrap(), Month::Feb);
            assert_eq!(Month::try_from(3).unwrap(), Month::Mar);
            assert_eq!(Month::try_from(4).unwrap(), Month::Apr);
            assert_eq!(Month::try_from(5).unwrap(), Month::May);
            assert_eq!(Month::try_from(6).unwrap(), Month::Jun);
            assert_eq!(Month::try_from(7).unwrap(), Month::Jul);
            assert_eq!(Month::try_from(8).unwrap(), Month::Aug);
            assert_eq!(Month::try_from(9).unwrap(), Month::Sep);
            assert_eq!(Month::try_from(10).unwrap(), Month::Oct);
            assert_eq!(Month::try_from(11).unwrap(), Month::Nov);
            assert_eq!(Month::try_from(12).unwrap(), Month::Dec);

            assert_eq!(Month::Jan.as_raw(), 1);
            assert_eq!(Month::Feb.as_raw(), 2);
            assert_eq!(Month::Mar.as_raw(), 3);
            assert_eq!(Month::Apr.as_raw(), 4);
            assert_eq!(Month::May.as_raw(), 5);
            assert_eq!(Month::Jun.as_raw(), 6);
            assert_eq!(Month::Jul.as_raw(), 7);
            assert_eq!(Month::Aug.as_raw(), 8);
            assert_eq!(Month::Sep.as_raw(), 9);
            assert_eq!(Month::Oct.as_raw(), 10);
            assert_eq!(Month::Nov.as_raw(), 11);
            assert_eq!(Month::Dec.as_raw(), 12);
        }

        #[test]
        fn test_under() {
            let r: Result<_, _> = Month::try_from(0);
            assert_eq!(r.is_err(), true);
        }

        #[test]
        fn test_over() {
            let r: Result<_, _> = Month::try_from(13);
            assert_eq!(r.is_err(), true);
        }
    }
}
