use std::collections::{BTreeMap, BTreeSet};
use std::ops::DerefMut;
use std::sync::Mutex;

use crate::item::{Item, RawItem};
use crate::{bucket::Bucket, data::RawData, date::Date, device::Device, evt::Event};

use crate::kvstore::create::Create;

/// Upserts an item into a bucket and finalizes after upserts(optional).
pub trait UpsertRaw {
    /// Upserts an item into a bucket.
    fn upsert(&mut self, b: &Bucket, i: &RawItem) -> Result<u64, Event>;

    /// (Optional) Finalize.
    ///
    /// Use nop method if no finalization required.
    fn finalize(self) -> Result<(), Event>;
}

/// Creates new upsert using `UpsertRaw`.
pub fn upsert_raw_new_func<U>(mut u: U) -> impl FnMut(&Bucket, &RawItem) -> Result<u64, Event>
where
    U: UpsertRaw,
{
    move |b: &Bucket, i: &RawItem| u.upsert(b, i)
}

/// Upserts data which creates a bucket before upsert.
///
/// # Arguments
/// - upsert: Upserts an item into a bucket using shared resource.
/// - create: Creates a bucket using shared resource.
/// - shared: Vendor specific shared resource for upsert/create.
/// - finalize: Finalizes the shared resource.
/// - requests: Data to be upserted.
pub fn upsert_all_shared<U, C, T, F, I>(
    upsert: U,
    create: C,
    shared: T,
    finalize: F,
    requests: I,
) -> Result<u64, Event>
where
    U: Fn(&mut T, &Bucket, &RawItem) -> Result<u64, Event>,
    C: Fn(&mut T, &Bucket) -> Result<u64, Event>,
    F: Fn(T) -> Result<(), Event>,
    I: Iterator<Item = RawData>,
{
    let mut upsert_raw = UpsertAfterCreateShared {
        upsert,
        create,
        shared,
        finalize,
    };
    let mut upst = |b: &Bucket, i: &RawItem| create_upsert(&mut upsert_raw, b, i);
    let cnt: u64 = upsert_all(requests, &mut upst)?;
    upsert_raw.finalize()?;
    Ok(cnt)
}

struct UpsertAfterCreateShared<U, C, T, F> {
    upsert: U,
    create: C,
    shared: T,
    finalize: F,
}

impl<U, C, T, F> UpsertRaw for UpsertAfterCreateShared<U, C, T, F>
where
    U: Fn(&mut T, &Bucket, &RawItem) -> Result<u64, Event>,
    F: Fn(T) -> Result<(), Event>,
{
    fn upsert(&mut self, b: &Bucket, i: &RawItem) -> Result<u64, Event> {
        (self.upsert)(&mut self.shared, b, i)
    }

    fn finalize(self) -> Result<(), Event> {
        (self.finalize)(self.shared)
    }
}

impl<U, C, T, F> Create for UpsertAfterCreateShared<U, C, T, F>
where
    C: Fn(&mut T, &Bucket) -> Result<u64, Event>,
{
    fn create(&mut self, b: &Bucket) -> Result<u64, Event> {
        (self.create)(&mut self.shared, b)
    }
}

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

pub fn create_upsert<CU>(cu: &mut CU, b: &Bucket, i: &RawItem) -> Result<u64, Event>
where
    CU: UpsertRaw + Create,
{
    let cnt_c: u64 = cu.create(b)?;
    let cnt_u: u64 = cu.upsert(b, i)?;
    Ok(cnt_c + cnt_u)
}

struct CreateBeforeUpsert<C, U> {
    create: C,
    upsert: U,
}

impl<C, U> Create for CreateBeforeUpsert<C, U>
where
    C: FnMut(&Bucket) -> Result<u64, Event>,
{
    fn create(&mut self, b: &Bucket) -> Result<u64, Event> {
        (self.create)(b)
    }
}

impl<C, U> UpsertRaw for CreateBeforeUpsert<C, U>
where
    U: FnMut(&Bucket, &RawItem) -> Result<u64, Event>,
{
    fn upsert(&mut self, b: &Bucket, i: &RawItem) -> Result<u64, Event> {
        (self.upsert)(b, i)
    }
    fn finalize(self) -> Result<(), Event> {
        Err(Event::UnexpectedError(String::from("Not implemented")))
    }
}

/// Creates new upsert handler which uses closures to do create/upsert.
///
/// # Arguments
/// - create: Creates a bucket.
/// - upsert: Handles upsert request.
pub fn create_upsert_new<C, U>(
    create: C,
    upsert: U,
) -> impl FnMut(&Bucket, &RawItem) -> Result<u64, Event>
where
    C: FnMut(&Bucket) -> Result<u64, Event>,
    U: FnMut(&Bucket, &RawItem) -> Result<u64, Event>,
{
    let mut cu = CreateBeforeUpsert { create, upsert };
    move |b: &Bucket, i: &RawItem| create_upsert(&mut cu, b, i)
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

pub fn create_skip_new_std_set(s: BTreeSet<Bucket>) -> impl Fn(&Bucket) -> Result<(), Event> {
    move |b: &Bucket| {
        s.get(b)
            .map(|_| ())
            .ok_or_else(|| Event::UnexpectedError(String::from("Should not skip")))
    }
}

pub fn create_or_skip<C, S>(create: &mut C, skip: &S, b: &Bucket) -> Result<u64, Event>
where
    C: FnMut(&Bucket) -> Result<u64, Event>,
    S: Fn(&Bucket) -> Result<(), Event>,
{
    skip(b).map(|_| 0).or_else(|_| create(b))
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
