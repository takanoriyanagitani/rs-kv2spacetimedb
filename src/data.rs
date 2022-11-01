use crate::{date::Date, device::Device, item::Item};

pub struct Data<K, V> {
    device: Device,
    date: Date,
    item: Item<K, V>,
}

impl<K, V> Data<K, V> {
    pub fn new(device: Device, date: Date, item: Item<K, V>) -> Self {
        Self { device, date, item }
    }

    pub fn as_device(&self) -> &Device {
        &self.device
    }
    pub fn as_date(&self) -> &Date {
        &self.date
    }
    pub fn as_item(&self) -> &Item<K, V> {
        &self.item
    }

    pub fn into_item(self) -> Item<K, V> {
        self.item
    }
}

pub type RawData = Data<Vec<u8>, Vec<u8>>;
