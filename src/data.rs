use crate::{date::Date, device::Device, item::Item};

/// A single Key/Value Data with ID(`Device`) and `Date`.
pub struct Data<K, V> {
    device: Device,
    date: Date,
    item: Item<K, V>,
}

impl<K, V> Data<K, V> {
    /// Creates new data with `Device`, `Date` and `Item<K,V>`.
    pub fn new(device: Device, date: Date, item: Item<K, V>) -> Self {
        Self { device, date, item }
    }

    /// Gets `Device` reference.
    pub fn as_device(&self) -> &Device {
        &self.device
    }

    /// Gets `Date` reference.
    pub fn as_date(&self) -> &Date {
        &self.date
    }

    /// Gets `Item<K,V>` reference.
    pub fn as_item(&self) -> &Item<K, V> {
        &self.item
    }

    /// Converts into `Item<K, V>`.
    pub fn into_item(self) -> Item<K, V> {
        self.item
    }
}

pub type RawData = Data<Vec<u8>, Vec<u8>>;
