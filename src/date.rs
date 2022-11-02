/// Date info container which contains year/month/date.
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

    /// Gets the date as str.
    pub fn as_str(&self) -> &str {
        self.date.as_str()
    }

    /// Gets the date as bytes.
    pub fn as_bytes(&self) -> &[u8] {
        self.as_str().as_bytes()
    }
}
