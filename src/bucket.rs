use crate::{date::Date, device::Device};

#[derive(PartialEq, PartialOrd, Eq, Ord)]
pub struct Bucket {
    name: String,
}

impl Bucket {
    pub fn as_str(&self) -> &str {
        self.name.as_str()
    }

    pub fn new_data_bucket(dev: &Device, date: &Date) -> Self {
        let device_id: &str = dev.as_str();
        let date_str: &str = date.as_str();
        let name: String = format!("data_{}_{}", date_str, device_id);
        Self { name }
    }

    pub fn new_dates_master_for_device(dev: &Device) -> Self {
        let device_id: &str = dev.as_str();
        let name: String = format!("dates_{}", device_id);
        Self { name }
    }

    pub fn new_devices_master_for_date(date: &Date) -> Self {
        let date_str: &str = date.as_str();
        let name: String = format!("devices_{}", date_str);
        Self { name }
    }

    pub fn new_dates_master() -> Self {
        let name = String::from("dates");
        Self { name }
    }

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
