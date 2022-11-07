use std::ops::DerefMut;
use std::sync::Mutex;

use crate::item::{Item, RawItem};
use crate::{bucket::Bucket, date::Date, device::Device, evt::Event};

/// Tries to get a raw item from specified bucket.
///
/// # Arguments
/// - getter: Tries to get bytes from the bucket.
/// - b:      Target bucket.
/// - key:    Bytes key.
pub fn get_raw_direct<G>(getter: &mut G, b: &Bucket, key: &[u8]) -> Result<Option<RawItem>, Event>
where
    G: FnMut(&Bucket, &[u8]) -> Result<Option<Vec<u8>>, Event>,
{
    getter(b, key).map(|o: Option<_>| {
        o.map(|v: Vec<u8>| {
            let vk: Vec<_> = key.into();
            Item::new(vk, v)
        })
    })
}

/// Tries to get a raw item from the data bucket.
///
/// # Arguments
/// - getter: Tries to get bytes from the bucket.
/// - dev:    Target device.
/// - date:   Target date.
/// - key:    Bytes key.
pub fn get_raw<G>(
    getter: &mut G,
    dev: &Device,
    date: &Date,
    key: &[u8],
) -> Result<Option<RawItem>, Event>
where
    G: FnMut(&Bucket, &[u8]) -> Result<Option<Vec<u8>>, Event>,
{
    let b: Bucket = Bucket::new_data_bucket(dev, date);
    get_raw_direct(getter, &b, key)
}

/// Tries to get a raw item from the data bucket which ignores missing bucket.
///
/// # Arguments
/// - getter:        Tries to get bytes from the bucket.
/// - dev:           Target device.
/// - date:          Target date.
/// - key:           Bytes key.
/// - bucket_exists: Checks if the bucket exists.
pub fn get_raw_ignore_missing_bucket<G, C>(
    getter: &mut G,
    dev: &Device,
    date: &Date,
    key: &[u8],
    bucket_exists: &mut C,
) -> Result<Option<RawItem>, Event>
where
    G: FnMut(&Bucket, &[u8]) -> Result<Option<Vec<u8>>, Event>,
    C: FnMut(&Bucket) -> Result<bool, Event>,
{
    let b: Bucket = Bucket::new_data_bucket(dev, date);
    let exists: bool = bucket_exists(&b)?;
    exists
        .then(|| get_raw_direct(getter, &b, key))
        .unwrap_or(Ok(None))
}

/// Creates new getter which uses closures to get an item and check the bucket.
/// # Arguments
/// - getter:        Tries to get bytes from the bucket.
/// - bucket_exists: Checks if the bucket exists.
/// - resource:      Vendor specific shared resource required to call getter/checker.
pub fn get_raw_ignore_missing_bucket_shared_new<G, C, R>(
    mut getter: G,
    mut bucket_exists: C,
    resource: R,
) -> impl FnMut(&Device, &Date, &[u8]) -> Result<Option<RawItem>, Event>
where
    G: FnMut(&mut R, &Bucket, &[u8]) -> Result<Option<Vec<u8>>, Event>,
    C: FnMut(&mut R, &Bucket) -> Result<bool, Event>,
{
    let mr: Mutex<R> = Mutex::new(resource);

    move |dev: &Device, date: &Date, key: &[u8]| {
        let mut get = |b: &Bucket, key: &[u8]| {
            let mut g = mr.lock().map_err(|e| {
                Event::UnexpectedError(format!("Unable to get a shared resource: {}", e))
            })?;
            let r: &mut R = g.deref_mut();
            getter(r, b, key)
        };
        let mut chk = |b: &Bucket| {
            let mut g = mr.lock().map_err(|e| {
                Event::UnexpectedError(format!("Unable to get a shared resource: {}", e))
            })?;
            let r: &mut R = g.deref_mut();
            bucket_exists(r, b)
        };
        get_raw_ignore_missing_bucket(&mut get, dev, date, key, &mut chk)
    }
}
