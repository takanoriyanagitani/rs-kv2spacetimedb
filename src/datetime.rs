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
