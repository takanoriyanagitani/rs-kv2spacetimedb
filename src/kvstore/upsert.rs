use std::collections::BTreeMap;
use std::ops::DerefMut;
use std::sync::Mutex;

use crate::item::{Item, RawItem};
use crate::{bucket::Bucket, data::RawData, date::Date, device::Device, evt::Event};

struct UpsertRequest<K, V> {
    bucket: Bucket,
    item: Item<K, V>,
}

impl<K, V> UpsertRequest<K, V> {
    fn new(bucket: Bucket, item: Item<K, V>) -> Self {
        Self { bucket, item }
    }
}

impl UpsertRequest<Vec<u8>, Vec<u8>> {
    fn from_data(d: RawData) -> Vec<Self> {
        let dev: &Device = d.as_device();
        let date: &Date = d.as_date();

        let i_dates4device: RawItem = Item::new(date.as_bytes().to_vec(), vec![]);
        let i_devices4date: RawItem = Item::new(dev.as_bytes().to_vec(), vec![]);
        let i_dates: RawItem = Item::new(date.as_bytes().to_vec(), vec![]);
        let i_devices: RawItem = Item::new(dev.as_bytes().to_vec(), vec![]);

        let b_data: Bucket = Bucket::new_data_bucket(dev, date);
        let b_dates4device: Bucket = Bucket::new_dates_master_for_device(dev);
        let b_devices4date: Bucket = Bucket::new_devices_master_for_date(date);
        let b_dates: Bucket = Bucket::new_dates_master();
        let b_devices: Bucket = Bucket::new_devices_master();

        let item: RawItem = d.into_item();

        let i_data: RawItem = item;

        vec![
            Self::new(b_data, i_data),
            Self::new(b_dates4device, i_dates4device),
            Self::new(b_devices4date, i_devices4date),
            Self::new(b_dates, i_dates),
            Self::new(b_devices, i_devices),
        ]
    }

    fn bulkdata2map<I>(bulk: I) -> BTreeMap<Bucket, Vec<RawItem>>
    where
        I: Iterator<Item = RawData>,
    {
        let i = bulk.map(Self::from_data).flat_map(|v| v.into_iter());
        i.fold(BTreeMap::new(), |mut m, req| {
            let b: Bucket = req.bucket;
            let i: RawItem = req.item;
            match m.get_mut(&b) {
                None => {
                    let v = vec![i];
                    m.insert(b, v);
                    m
                }
                Some(v) => {
                    v.push(i);
                    m
                }
            }
        })
    }
}

fn rawdata2requests<I>(i: I) -> impl Iterator<Item = (Bucket, Vec<RawItem>)>
where
    I: Iterator<Item = RawData>,
{
    let m: BTreeMap<Bucket, Vec<RawItem>> = UpsertRequest::bulkdata2map(i);
    m.into_iter()
}

fn upsert_into_bucket<U>(b: &Bucket, items: &[RawItem], upsert: &mut U) -> Result<u64, Event>
where
    U: FnMut(&Bucket, &RawItem) -> Result<u64, Event>,
{
    items
        .iter()
        .try_fold(0, |tot, item| upsert(b, item).map(|cnt| cnt + tot))
}

/// Saves data got from source which uses a closure to actually save data.
///
/// Duplicates will be ignored.
///
/// # Arguments
/// - source: `RawData` source iterator.
/// - upsert: Data saver which saves data into specified bucket.
pub fn upsert_all<I, U>(source: I, upsert: &mut U) -> Result<u64, Event>
where
    I: Iterator<Item = RawData>,
    U: FnMut(&Bucket, &RawItem) -> Result<u64, Event>,
{
    let mut requests = rawdata2requests(source);
    requests.try_fold(0, |tot, req| {
        let (bucket, v) = req;
        let uniq: Vec<RawItem> = Item::uniq(v);
        upsert_into_bucket(&bucket, &uniq, upsert).map(|cnt| cnt + tot)
    })
}

/// Creates new upsert handler which uses closures to do create/upsert.
///
/// # Arguments
/// - create: Creates a bucket.
/// - upsert: Handles upsert request.
pub fn create_upsert_new<C, U>(
    mut create: C,
    mut upsert: U,
) -> impl FnMut(&Bucket, &RawItem) -> Result<u64, Event>
where
    C: FnMut(&Bucket) -> Result<u64, Event>,
    U: FnMut(&Bucket, &RawItem) -> Result<u64, Event>,
{
    move |b: &Bucket, i: &RawItem| {
        let cnt_c: u64 = create(b)?;
        let cnt_u: u64 = upsert(b, i)?;
        Ok(cnt_c + cnt_u)
    }
}

/// Handles create/upsert requests which uses shared resource protected by mutex.
///
/// # Arguments
/// - source: Data to be upserted.
/// - create: Handles create request.
/// - upsert: Handles upsert request.
/// - shared_resource: Vendor specific shared resource to be protected by mutex.
/// - finalize: Vendor specific finalization for the shared resource.
pub fn create_upsert_all_shared<I, C, U, T, F>(
    source: I,
    create: C,
    upsert: U,
    shared_resource: T,
    finalize: F,
) -> Result<u64, Event>
where
    I: Iterator<Item = RawData>,
    C: Fn(&mut T, &Bucket) -> Result<u64, Event>,
    U: Fn(&mut T, &Bucket, &RawItem) -> Result<u64, Event>,
    F: Fn(T) -> Result<(), Event>,
{
    let mt: Mutex<T> = Mutex::new(shared_resource);

    let c = |b: &Bucket| {
        let mut l = mt
            .lock()
            .map_err(|e| Event::UnexpectedError(format!("Unable to create while upsert: {}", e)))?;
        let t: &mut T = l.deref_mut();
        create(t, b)
    };
    let u = |b: &Bucket, i: &RawItem| {
        let mut l = mt
            .lock()
            .map_err(|e| Event::UnexpectedError(format!("Unable to create while upsert: {}", e)))?;
        let t: &mut T = l.deref_mut();
        upsert(t, b, i)
    };
    let mut cu = create_upsert_new(c, u);
    let cnt: u64 = upsert_all(source, &mut cu)?;
    drop(cu);

    let t: T = mt
        .into_inner()
        .map_err(|e| Event::UnexpectedError(format!("Unable to prepare finalization: {}", e)))?;
    finalize(t)?;
    Ok(cnt)
}

/// Creates new create handler which uses closures to create or skip bucket creation.
///
/// - create: Creates a bucket.
/// - cache:  Returns Ok(0) when a bucket already exists, Err otherwise.
pub fn create_cached_new<C, M>(mut create: C, cache: M) -> impl FnMut(&Bucket) -> Result<u64, Event>
where
    C: FnMut(&Bucket) -> Result<u64, Event>,
    M: Fn(&Bucket) -> Result<u64, Event>,
{
    move |b: &Bucket| cache(b).or_else(|_| create(b))
}

#[cfg(test)]
mod test_upsert {

    mod upsert_all {

        use crate::bucket::Bucket;
        use crate::item::RawItem;
        use crate::kvstore::upsert;

        #[test]
        fn test_empty() {
            let mut upst = |_b: &Bucket, _i: &RawItem| Ok(1);
            let source = vec![].into_iter();
            let upserted: u64 = upsert::upsert_all(source, &mut upst).unwrap();
            assert_eq!(0, upserted);
        }
    }

    mod create_cached_new {
        use crate::bucket::Bucket;
        use crate::evt::Event;
        use crate::kvstore::upsert;

        #[test]
        fn test_table_exists() {
            let m = |_: &Bucket| Ok(0);
            let c = |_: &Bucket| Err(Event::UnexpectedError(String::from("Must not call me")));
            let mut f = upsert::create_cached_new(c, m);
            let b: Bucket = Bucket::from(String::from(""));
            let cnt: u64 = f(&b).unwrap();
            assert_eq!(cnt, 0);
        }
    }
}
