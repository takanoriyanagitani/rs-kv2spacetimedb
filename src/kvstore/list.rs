use crate::{bucket::Bucket, date::Date, device::Device, evt::Event};

/// Gets all buckets.
pub trait ListBuckets {
    fn list(&mut self) -> Result<Vec<Bucket>, Event>;
}

/// Gets all keys from a bucket.
pub trait ListKeys<K> {
    fn list(&mut self, b: &Bucket) -> Result<Vec<K>, Event>;
}

/// Gets all keys from a data bucket.
///
/// # Arguments
/// - list: Gets all keys from a bucket.
/// - date: Target date.
/// - device: Target device.
pub fn list_keys4data<L>(list: &mut L, date: &Date, device: &Device) -> Result<Vec<Vec<u8>>, Event>
where
    L: FnMut(&Bucket) -> Result<Vec<Vec<u8>>, Event>,
{
    let b: Bucket = Bucket::new_data_bucket(device, date);
    list(&b)
}

/// Creates new list getter which gets all keys from a data bucket.
///
/// Missing bucket will be ignored(returns empty vec).
/// # Arguments
/// - list: Gets all keys from a bucket.
/// - check: Checks if the bucket exists.
/// - shared: Vendor specific shared resource used by list/check.
pub fn list_keys4data_ignore_missing_bucket_new<L, C, R>(
    list: L,
    check: C,
    mut shared: R,
) -> impl FnMut(&Date, &Device) -> Result<Vec<Vec<u8>>, Event>
where
    L: Fn(&mut R, &Bucket) -> Result<Vec<Vec<u8>>, Event>,
    C: Fn(&mut R, &Bucket) -> Result<bool, Event>,
{
    move |d: &Date, dev: &Device| {
        let b: Bucket = Bucket::new_data_bucket(dev, d);
        let bucket_exists: bool = check(&mut shared, &b)?;
        match bucket_exists {
            false => Ok(vec![]),
            true => list(&mut shared, &b),
        }
    }
}
