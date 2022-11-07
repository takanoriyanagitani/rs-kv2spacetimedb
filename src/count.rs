use crate::datetime::DateTime;

/// A Counter with updated Date/Time.
#[derive(Debug, Clone, Copy, Default)]
pub struct Count {
    count: u64,
    updated: DateTime,
}

impl Count {
    /// Creates new counter.
    pub fn new(count: u64, updated: DateTime) -> Self {
        Self { count, updated }
    }

    pub fn update(&mut self, neo: u64, updated: DateTime) {
        self.count = neo;
        self.updated = updated;
    }

    pub fn replace(&mut self, other: &Count) {
        self.update(other.count, other.updated)
    }

    /// Gets the count value.
    pub fn as_count(&self) -> u64 {
        self.count
    }

    /// Gets updated Date/Time.
    pub fn as_datetime(&self) -> DateTime {
        self.updated
    }

    pub fn to_be_bytes(&self) -> [u8; 16] {
        let u: u128 = u128::from(*self);
        u.to_be_bytes()
    }

    pub fn is_stale(&self, lbi: &DateTime, ubi: &DateTime) -> bool {
        let fresh: bool = lbi.le(&self.updated) && self.updated.le(ubi);
        !fresh
    }

    pub fn is_stale_by_duration_us(&self, now: &DateTime, duration_us: u64) -> bool {
        let lbi: Option<DateTime> = now.sub(duration_us).ok();
        let ubi: Option<DateTime> = now.add(duration_us).ok();
        lbi.and_then(|l| ubi.map(|u| self.is_stale(&l, &u)))
            .unwrap_or(true)
    }
}

pub fn count_builder_new<T, E>(time_source: T) -> impl Fn(u64) -> Result<Count, E>
where
    T: Fn() -> Result<DateTime, E>,
{
    move |cnt: u64| {
        let updated: DateTime = time_source()?;
        Ok(Count::new(cnt, updated))
    }
}

pub fn stale_checker_new_by_duration_us(duration_us: u64) -> impl Fn(&Count, &DateTime) -> bool {
    move |c: &Count, now: &DateTime| c.is_stale_by_duration_us(now, duration_us)
}

pub fn stale_checker_builder_new<T, E>(
    time_source: T,
    duration_us: u64,
) -> impl Fn(&Count) -> Result<bool, E>
where
    T: Fn() -> Result<DateTime, E>,
{
    move |cnt: &Count| {
        let now: DateTime = time_source()?;
        Ok(cnt.is_stale_by_duration_us(&now, duration_us))
    }
}

impl From<Count> for u128 {
    fn from(c: Count) -> Self {
        let cnt: u128 = c.count.into();
        let dt: u128 = c.updated.as_unixtime_us().into();
        (cnt << 64) | dt
    }
}

impl From<u128> for Count {
    fn from(u: u128) -> Self {
        let hi: u128 = u >> 64;
        let lo: u128 = u & 0xffff_ffff_ffff_ffff;
        let h: u64 = hi as u64;
        let l: u64 = lo as u64;
        let d: DateTime = DateTime::from_unixtime_us(l);
        Self::new(h, d)
    }
}
