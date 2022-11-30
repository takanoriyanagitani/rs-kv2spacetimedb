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

#[cfg(test)]
mod test_data {

    mod data_as_item {

        use crate::{data::Data, date::Date, device::Device, item::Item};

        #[test]
        fn test_bytes() {
            let dev: Device = Device::new_unchecked("cafef00ddeadbeafface864299792458".into());
            let d: Date = Date::new_unchecked("2022_12_01".into());
            let i: Item<_, _> = Item::new(b"07:58:48".to_vec(), b"42".to_vec());

            let dat: Data<_, _> = Data::new(dev, d, i);
            let ri: &Item<_, _> = dat.as_item();
            let k: &[u8] = ri.as_key();
            let v: &[u8] = ri.as_val();
            assert_eq!(k, b"07:58:48");
            assert_eq!(v, b"42");
        }
    }
}
