//! Simple Date/Time.

use crate::evt::Event;

/// Non-monotonic Date/Time which counts micro seconds from the unix epoch.
#[derive(Debug, Default, Clone, Copy, PartialEq, PartialOrd, Eq, Ord)]
pub struct DateTime {
    unixtime_us: u64,
}

impl DateTime {
    /// Gets this Date/Time as unixtime in micro seconds.
    pub fn as_unixtime_us(&self) -> u64 {
        self.unixtime_us
    }

    /// Creates new Date/Time from unixtime in micro seconds.
    pub fn from_unixtime_us(unixtime_us: u64) -> Self {
        Self { unixtime_us }
    }

    /// Creates new Date/Time after specified duration.
    pub fn add(&self, duration_us: u64) -> Result<Self, Event> {
        let neo: u64 = self.unixtime_us.checked_add(duration_us).ok_or_else(|| {
            Event::InvalidDateTime(format!("Date/Time out of range: {}", self.unixtime_us))
        })?;
        Ok(Self::from_unixtime_us(neo))
    }

    /// Creates new Date/Time before specified duration.
    pub fn sub(&self, duration_us: u64) -> Result<Self, Event> {
        let neo: u64 = self.unixtime_us.checked_sub(duration_us).ok_or_else(|| {
            Event::InvalidDateTime(format!("Date/Time out of range: {}", self.unixtime_us))
        })?;
        Ok(Self::from_unixtime_us(neo))
    }
}

impl DateTime {
    fn from_std_time(t: std::time::SystemTime) -> Result<Self, Event> {
        let d: std::time::Duration = t
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map_err(|e| Event::InvalidDateTime(format!("Unable to compute unixtime: {}", e)))?;
        let us: u128 = d.as_micros();
        let unixtime_us: u64 = us
            .try_into()
            .map_err(|e| Event::InvalidDateTime(format!("Date/Time out of range: {}", e)))?;
        Ok(Self::from_unixtime_us(unixtime_us))
    }

    /// Creates new Date/Time which gets current time using standard library.
    pub fn now_std() -> Result<Self, Event> {
        let t = std::time::SystemTime::now();
        Self::try_from(t)
    }

    pub fn time_source_new_std() -> impl Fn() -> Result<Self, Event> {
        Self::now_std
    }
}

impl TryFrom<std::time::SystemTime> for DateTime {
    type Error = Event;
    fn try_from(t: std::time::SystemTime) -> Result<Self, Self::Error> {
        Self::from_std_time(t)
    }
}

#[cfg(test)]
mod test_datetime {

    mod datetime {
        use crate::datetime::DateTime;

        #[test]
        fn test_epoch() {
            let dt: DateTime = DateTime::from_unixtime_us(0);
            let dt_after_1us: DateTime = dt.add(1).unwrap();
            let dt_epoch: DateTime = dt_after_1us.sub(1).unwrap();
            assert_eq!(dt, dt_epoch);
        }

        #[test]
        fn test_greater() {
            let dt: DateTime = DateTime::from_unixtime_us(u64::MAX);
            let dtadd: Result<_, _> = dt.add(1);
            assert_eq!(dtadd.is_err(), true);
        }

        #[test]
        fn test_less() {
            let dt: DateTime = DateTime::from_unixtime_us(0);
            let dtsub: Result<_, _> = dt.sub(1);
            assert_eq!(dtsub.is_err(), true);
        }

        #[test]
        fn test_unixtime() {
            let dt: DateTime = DateTime::from_unixtime_us(1);
            let u: u64 = dt.as_unixtime_us();
            assert_eq!(u, 1);
        }
    }

    mod datetime_system_time {
        use std::time::{Duration, SystemTime};

        use crate::datetime::DateTime;

        #[test]
        fn test_epoch() {
            let epoch: SystemTime = SystemTime::UNIX_EPOCH;
            let d: DateTime = DateTime::try_from(epoch).unwrap();
            assert_eq!(d.as_unixtime_us(), 0);
        }

        #[test]
        fn test_before_epoch() {
            let epoch: SystemTime = SystemTime::UNIX_EPOCH;
            let e_sub: SystemTime = epoch.checked_sub(Duration::from_micros(1)).unwrap();
            let r: Result<_, _> = DateTime::from_std_time(e_sub);
            assert_eq!(r.is_err(), true);
        }

        #[test]
        fn test_upper() {
            let epoch: SystemTime = SystemTime::UNIX_EPOCH;
            let dur_max: Duration = Duration::from_micros(u64::MAX);
            let dt_max: SystemTime = epoch.checked_add(dur_max).unwrap();
            let dt_add: SystemTime = dt_max.checked_add(Duration::from_micros(1)).unwrap();
            let r: Result<_, _> = DateTime::try_from(dt_add);
            assert_eq!(r.is_err(), true);
        }
    }

    mod time_source_new_std {
        use crate::datetime::DateTime;

        #[test]
        #[ignore]
        fn test_ok() {
            let f = DateTime::time_source_new_std();
            let r: Result<_, _> = f();
            assert_eq!(r.is_ok(), true);
        }
    }
}
