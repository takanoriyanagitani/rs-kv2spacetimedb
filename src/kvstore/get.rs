use std::ops::DerefMut;
use std::sync::Mutex;

use crate::item::{Item, RawItem};
use crate::{bucket::Bucket, date::Date, device::Device, evt::Event};

/// Tries to get a value from a bucket which ignores missing bucket.
pub trait GetRaw {
    /// Gets a value.
    fn get(&mut self, b: &Bucket, key: &[u8]) -> Result<Option<Vec<u8>>, Event>;

    /// Checks if the bucket exists.
    fn chk(&mut self, b: &Bucket) -> Result<bool, Event>;
}

struct GetRawShared<G, C, R> {
    get: G,
    chk: C,
    shared: R,
}

/// Creates new data getter which uses a closure to try to get a value from the bucket.
pub fn get_raw_new<G>(
    mut getter: G,
) -> impl FnMut(&Device, &Date, &[u8]) -> Result<Option<RawItem>, Event>
where
    G: FnMut(&Bucket, &[u8]) -> Result<Option<Vec<u8>>, Event>,
{
    move |dev: &Device, d: &Date, key: &[u8]| {
        let b: Bucket = Bucket::new_data_bucket(dev, d);
        get_raw_direct(&mut getter, &b, key)
    }
}

/// Creates new getter which ignores missing bucket.
///
/// # Arguments
/// - get: Tries to get a value from the bucket using shared resource.
/// - chk: Checks if the bucket exists.
/// - shared: Vendor specific shared resource for get/chk.
pub fn get_raw_ignore_missing_bucket_new_func_shared_direct<G, C, R>(
    get: G,
    chk: C,
    shared: R,
) -> impl FnMut(&Bucket, &[u8]) -> Result<Option<Vec<u8>>, Event>
where
    G: Fn(&mut R, &Bucket, &[u8]) -> Result<Option<Vec<u8>>, Event>,
    C: Fn(&mut R, &Bucket) -> Result<bool, Event>,
{
    let grs = GetRawShared { get, chk, shared };
    get_raw_ignore_missing_bucket_new_func(grs)
}

/// Creates new data getter which uses closures to try to get a value from the bucket.
///
/// # Arguments
/// - get: Tries to get a value from the bucket using shared resource.
/// - chk: Checks if the bucket exists.
/// - shared: Vendor specific shared resource for get/chk.
pub fn get_raw_ignore_missing_bucket_new_func_shared<G, C, R>(
    get: G,
    chk: C,
    shared: R,
) -> impl FnMut(&Device, &Date, &[u8]) -> Result<Option<RawItem>, Event>
where
    G: Fn(&mut R, &Bucket, &[u8]) -> Result<Option<Vec<u8>>, Event>,
    C: Fn(&mut R, &Bucket) -> Result<bool, Event>,
{
    let getter = get_raw_ignore_missing_bucket_new_func_shared_direct(get, chk, shared);
    get_raw_new(getter)
}

impl<G, C, R> GetRaw for GetRawShared<G, C, R>
where
    G: Fn(&mut R, &Bucket, &[u8]) -> Result<Option<Vec<u8>>, Event>,
    C: Fn(&mut R, &Bucket) -> Result<bool, Event>,
{
    fn get(&mut self, b: &Bucket, key: &[u8]) -> Result<Option<Vec<u8>>, Event> {
        (self.get)(&mut self.shared, b, key)
    }

    fn chk(&mut self, b: &Bucket) -> Result<bool, Event> {
        (self.chk)(&mut self.shared, b)
    }
}

/// Creates new getter function using getter implementation.
pub fn get_raw_ignore_missing_bucket_new_func<G>(
    mut getter: G,
) -> impl FnMut(&Bucket, &[u8]) -> Result<Option<Vec<u8>>, Event>
where
    G: GetRaw,
{
    move |b: &Bucket, key: &[u8]| {
        let bucket_exists: bool = getter.chk(b)?;
        bucket_exists
            .then(|| getter.get(b, key))
            .unwrap_or(Ok(None))
    }
}

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
