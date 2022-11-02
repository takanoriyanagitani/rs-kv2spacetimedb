use crate::{date::Date, device::Device};

/// ID(name) of a container which may contain many key/value pairs.
#[derive(Debug, PartialEq, PartialOrd, Eq, Ord)]
pub struct Bucket {
    name: String,
}

impl Bucket {
    /// Gets the name as str reference.
    pub fn as_str(&self) -> &str {
        self.name.as_str()
    }

    /// Creates new `Bucket` for data.
    ///
    /// # Example
    ///
    /// ```
    /// use rs_kv2spacetimedb::{date::Date, device::Device, bucket::Bucket};
    ///
    /// let dev = Device::new_unchecked("cafef00ddeadbeafface864299792458".into());
    /// let date = Date::new_unchecked("2022_11_02".into());
    ///
    /// let bucket = Bucket::new_data_bucket(&dev, &date);
    /// assert_eq!(
    ///     bucket.as_str(),
    ///     "data_2022_11_02_cafef00ddeadbeafface864299792458",
    /// );
    /// ```
    pub fn new_data_bucket(dev: &Device, date: &Date) -> Self {
        let device_id: &str = dev.as_str();
        let date_str: &str = date.as_str();
        let name: String = format!("data_{}_{}", date_str, device_id);
        Self { name }
    }

    /// Creates new `Bucket` for dates master.
    ///
    /// # Example
    ///
    /// ```
    /// use rs_kv2spacetimedb::{device::Device, bucket::Bucket};
    ///
    /// let dev = Device::new_unchecked("cafef00ddeadbeafface864299792458".into());
    ///
    /// let bucket = Bucket::new_dates_master_for_device(&dev);
    /// assert_eq!(
    ///     bucket.as_str(),
    ///     "dates_cafef00ddeadbeafface864299792458",
    /// );
    /// ```
    pub fn new_dates_master_for_device(dev: &Device) -> Self {
        let device_id: &str = dev.as_str();
        let name: String = format!("dates_{}", device_id);
        Self { name }
    }

    /// Creates new `Bucket` for devices master.
    ///
    /// # Example
    ///
    /// ```
    /// use rs_kv2spacetimedb::{date::Date, bucket::Bucket};
    ///
    /// let date = Date::new_unchecked("2022_11_02".into());
    ///
    /// let bucket = Bucket::new_devices_master_for_date(&date);
    /// assert_eq!(
    ///     bucket.as_str(),
    ///     "devices_2022_11_02",
    /// );
    /// ```
    pub fn new_devices_master_for_date(date: &Date) -> Self {
        let date_str: &str = date.as_str();
        let name: String = format!("devices_{}", date_str);
        Self { name }
    }

    /// Creates new `Bucket` for dates master.
    ///
    /// # Example
    ///
    /// ```
    /// use rs_kv2spacetimedb::bucket::Bucket;
    ///
    /// let bucket = Bucket::new_dates_master();
    /// assert_eq!(
    ///     bucket.as_str(),
    ///     "dates",
    /// );
    /// ```
    pub fn new_dates_master() -> Self {
        let name = String::from("dates");
        Self { name }
    }

    /// Creates new `Bucket` for devices master.
    ///
    /// # Example
    ///
    /// ```
    /// use rs_kv2spacetimedb::bucket::Bucket;
    ///
    /// let bucket = Bucket::new_devices_master();
    /// assert_eq!(
    ///     bucket.as_str(),
    ///     "devices",
    /// );
    /// ```
    pub fn new_devices_master() -> Self {
        let name = String::from("devices");
        Self { name }
    }
}

impl From<Bucket> for String {
    fn from(b: Bucket) -> Self {
        b.name
    }
}

impl From<String> for Bucket {
    fn from(name: String) -> Self {
        Self { name }
    }
}
