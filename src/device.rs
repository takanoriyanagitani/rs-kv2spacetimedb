/// Device info container.
pub struct Device {
    id: String, // cafef00d-dead-beaf-face-864299792458 => cafef00ddeadbeafface864299792458
}

impl From<u128> for Device {
    fn from(u: u128) -> Self {
        let id: String = format!("{:032x}", u);
        Self { id }
    }
}

impl Device {
    /// Creates new `Device` from `String`.
    ///
    /// Provided `String` must be "valid"; can be used as a part of table name.
    ///
    /// # Example
    /// ```
    /// use rs_kv2spacetimedb::device::Device;
    ///
    /// let d = Device::new_unchecked("cafef00ddeadbeafface864299792458".into());
    /// assert_eq!(d.as_str(), "cafef00ddeadbeafface864299792458");
    /// ```
    pub fn new_unchecked(id: String) -> Self {
        Self { id }
    }

    /// Gets the device id as str.
    pub fn as_str(&self) -> &str {
        self.id.as_str()
    }

    /// Gets the device id as bytes.
    pub fn as_bytes(&self) -> &[u8] {
        self.as_str().as_bytes()
    }
}
