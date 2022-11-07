use crate::{day::Day, month::Month, year::Year};

/// Date info container which contains year/month/date.
#[derive(PartialEq, PartialOrd, Eq, Ord)]
pub struct Date {
    date: String, // 2022/11/01 => 2022_11_01
}

impl Date {
    /// Creates new `Date` from `String`.
    ///
    /// Provided `String` must be "valid"; can be used as a part of table name.
    ///
    /// # Example
    /// ```
    /// use rs_kv2spacetimedb::date::Date;
    ///
    /// let d = Date::new_unchecked("2022_11_01".into());
    /// assert_eq!(d.as_str(), "2022_11_01");
    /// ```
    pub fn new_unchecked(date: String) -> Self {
        Self { date }
    }

    fn from_raw(y: u16, m: u8, d: u8) -> Self {
        let date: String = format!("{:04}_{:02}_{:02}", y, m, d);
        Self { date }
    }

    /// Creates new `Date` which can be invalid.
    ///
    /// # Arguments
    /// - y: Year. Always valid.
    /// - m: Month. Always valid.
    /// - d: Day. Can be invalid(does not care a month nor a leap year).
    pub fn new(y: Year, m: Month, d: Day) -> Self {
        let yu: u16 = y.as_raw();
        let mu: u8 = m.as_raw();
        let du: u8 = d.as_raw();
        Self::from_raw(yu, mu, du)
    }

    /// Gets the date as str.
    pub fn as_str(&self) -> &str {
        self.date.as_str()
    }

    /// Gets the date as bytes.
    pub fn as_bytes(&self) -> &[u8] {
        self.as_str().as_bytes()
    }
}
