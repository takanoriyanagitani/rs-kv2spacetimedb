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

/// Gets shared resource.
pub trait IntoShared<S> {
    fn into_inner(self) -> Result<S, Event>;
}

/// Creates new upsert using `UpsertRaw`.
pub fn upsert_raw_new_func<U>(mut u: U) -> impl FnMut(&Bucket, &RawItem) -> Result<u64, Event>
where
    U: UpsertRaw,
{
    move |b: &Bucket, i: &RawItem| u.upsert(b, i)
}

/// Requests upserts and returns shared resource which can be used for metrics collection.
///
/// # Arguments
/// - upsert: Upserts an item into a bucket using shared resource.
/// - create: Creates a bucket using shared resource.
/// - shared: Vendor specific shared resource for upsert/create.
/// - requests: Data to be upserted.
/// - upsert_value_gen: Value generator for master buckets.
pub fn request_upsert_all_shared_ex<U, C, T, I, G>(
    upsert: U,
    create: C,
    shared: T,
    requests: I,
    upsert_value_gen: G,
) -> Result<T, Event>
where
    U: Fn(&mut T, &Bucket, &RawItem) -> Result<u64, Event>,
    C: Fn(&mut T, &Bucket) -> Result<u64, Event>,
    I: Iterator<Item = RawData>,
    G: UpsertValueGenerator,
{
    let nop_finalize = |_: T| Ok(());
    let mut upsert_raw = UpsertAfterCreateShared {
        upsert,
        create,
        shared,
        finalize: nop_finalize,
    };
    let mut upst = |b: &Bucket, i: &RawItem| create_upsert(&mut upsert_raw, b, i);
    upsert_all_ex(requests, &mut upst, upsert_value_gen)?;
    let shared: T = upsert_raw.into_inner()?;
    Ok(shared)
}

/// Upserts data which creates a bucket before upsert.
///
/// # Arguments
/// - upsert: Upserts an item into a bucket using shared resource.
/// - create: Creates a bucket using shared resource.
/// - shared: Vendor specific shared resource for upsert/create.
/// - finalize: Finalizes the shared resource.
/// - requests: Data to be upserted.
/// - upsert_value_gen: Value generator for master buckets.
pub fn upsert_all_shared_ex<U, C, T, F, I, G>(
    upsert: U,
    create: C,
    shared: T,
    finalize: F,
    requests: I,
    upsert_value_gen: G,
) -> Result<u64, Event>
where
    U: Fn(&mut T, &Bucket, &RawItem) -> Result<u64, Event>,
    C: Fn(&mut T, &Bucket) -> Result<u64, Event>,
    F: Fn(T) -> Result<(), Event>,
    I: Iterator<Item = RawData>,
    G: UpsertValueGenerator,
{
    let mut upsert_raw = UpsertAfterCreateShared {
        upsert,
        create,
        shared,
        finalize,
    };
    let mut upst = |b: &Bucket, i: &RawItem| create_upsert(&mut upsert_raw, b, i);
    let cnt: u64 = upsert_all_ex(requests, &mut upst, upsert_value_gen)?;
    upsert_raw.finalize()?;
    Ok(cnt)
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
    let upsert_value_gen = upsert_value_generator_new_func_default();
    upsert_all_shared_ex(upsert, create, shared, finalize, requests, upsert_value_gen)
}

struct UpsertAfterCreateShared<U, C, T, F> {
    upsert: U,
    create: C,
    shared: T,
    finalize: F,
}

impl<U, C, T, F> IntoShared<T> for UpsertAfterCreateShared<U, C, T, F> {
    fn into_inner(self) -> Result<T, Event> {
        Ok(self.shared)
    }
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

/// Generates value for master buckets.
pub trait UpsertValueGenerator {
    /// Generates value for device master bucket.
    fn devices(&self, d: &Device) -> RawItem;

    /// Generates value for date master bucket.
    fn dates(&self, d: &Date) -> RawItem;

    /// Generates value for date master bucket for device.
    fn dates4device(&self, d: &Date) -> RawItem;

    /// Generates value for device master bucket for date.
    fn devices4date(&self, d: &Device) -> RawItem;
}

/// Creates new value generator using closures.
///
/// # Arguments
/// - dates: Value generator for dates bucket.
/// - devices: Value generator for devices bucket.
/// - dates4device: Value generator for date master bucket for device.
/// - devices4date: Value generator for device master bucket for date.
pub fn upsert_value_generator_new_func<Dates, Devices, Dates4, Devices4>(
    dates: Dates,
    devices: Devices,
    dates4device: Dates4,
    devices4date: Devices4,
) -> impl UpsertValueGenerator
where
    Dates: Fn(&Date) -> RawItem,
    Devices: Fn(&Device) -> RawItem,
    Dates4: Fn(&Date) -> RawItem,
    Devices4: Fn(&Device) -> RawItem,
{
    UpsertValueGenF {
        dates,
        devices,
        dates4device,
        devices4date,
    }
}

/// Creates default value generator which uses empty bytes as value.
pub fn upsert_value_generator_new_func_default() -> impl UpsertValueGenerator {
    upsert_value_generator_new_func(
        |d: &Date| Item::new(d.as_bytes().to_vec(), vec![]),
        |d: &Device| Item::new(d.as_bytes().to_vec(), vec![]),
        |d: &Date| Item::new(d.as_bytes().to_vec(), vec![]),
        |d: &Device| Item::new(d.as_bytes().to_vec(), vec![]),
    )
}

struct UpsertValueGenF<Dates, Devices, Dates4, Devices4> {
    dates: Dates,
    devices: Devices,
    dates4device: Dates4,
    devices4date: Devices4,
}

impl<Dates, Devices, Dates4, Devices4> UpsertValueGenerator
    for UpsertValueGenF<Dates, Devices, Dates4, Devices4>
where
    Devices: Fn(&Device) -> RawItem,
    Dates: Fn(&Date) -> RawItem,
    Dates4: Fn(&Date) -> RawItem,
    Devices4: Fn(&Device) -> RawItem,
{
    fn devices(&self, d: &Device) -> RawItem {
        (self.devices)(d)
    }
    fn dates(&self, d: &Date) -> RawItem {
        (self.dates)(d)
    }

    fn dates4device(&self, d: &Date) -> RawItem {
        (self.dates4device)(d)
    }
    fn devices4date(&self, d: &Device) -> RawItem {
        (self.devices4date)(d)
    }
}

impl UpsertRequest<Vec<u8>, Vec<u8>> {
    fn from_data<G>(d: RawData, upsert_value_gen: &G) -> Vec<Self>
    where
        G: UpsertValueGenerator,
    {
        let dev: &Device = d.as_device();
        let date: &Date = d.as_date();

        let i_dates4device: RawItem = upsert_value_gen.dates4device(date);
        let i_devices4date: RawItem = upsert_value_gen.devices4date(dev);
        let i_dates: RawItem = upsert_value_gen.dates(date);
        let i_devices: RawItem = upsert_value_gen.devices(dev);

        let b_data: Bucket = Bucket::new_data_bucket(dev, date);
        let b_dates4device: Bucket = Bucket::new_dates_master_for_device(dev);
        let b_devices4date: Bucket = Bucket::new_devices_master_for_date(date);
        let b_dates: Bucket = Bucket::new_dates_master();
        let b_devices: Bucket = Bucket::new_devices_master();

        let item: RawItem = d.into_item();

        let i_data: RawItem = item;

        vec![
            Self::new(b_data, i_data),                 // data_2022_11_09_cafef00d....
            Self::new(b_dates4device, i_dates4device), // dates_cafef00d....
            Self::new(b_devices4date, i_devices4date), // devices_2022_11_09
            Self::new(b_dates, i_dates),               // dates
            Self::new(b_devices, i_devices),           // devices
        ]
    }

    fn bulkdata2map<I, G>(bulk: I, upsert_value_gen: G) -> BTreeMap<Bucket, Vec<RawItem>>
    where
        I: Iterator<Item = RawData>,
        G: UpsertValueGenerator,
    {
        let i = bulk
            .map(|d: RawData| Self::from_data(d, &upsert_value_gen))
            .flat_map(|v| v.into_iter());
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

fn rawdata2requests<I, G>(i: I, upsert_value_gen: G) -> impl Iterator<Item = (Bucket, Vec<RawItem>)>
where
    I: Iterator<Item = RawData>,
    G: UpsertValueGenerator,
{
    let m: BTreeMap<Bucket, Vec<RawItem>> = UpsertRequest::bulkdata2map(i, upsert_value_gen);
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

/// Upserts after conversion.
///
/// # Arguments
/// - source: Input data before conversion.
/// - upsert: Upserts converted item.
/// - upsert_value_gen: Generates value to be upserted.
/// - conv: Converts input data.
/// - inspect: Handle conversion result(can be used to update metrics).
pub fn upsert_all_converted<I, U, G, C, T, M>(
    source: I,
    upsert: &mut U,
    upsert_value_gen: G,
    conv: &C,
    inspect: &mut M,
) -> Result<u64, Event>
where
    I: Iterator<Item = T>,
    U: FnMut(&Bucket, &RawItem) -> Result<u64, Event>,
    G: UpsertValueGenerator,
    C: Fn(T) -> Result<RawData, Event>,
    M: FnMut(&Result<RawData, Event>),
{
    let mapd = source.map(conv);
    let inspected = mapd.inspect(inspect);
    let noerr = inspected.flat_map(|r: Result<RawData, _>| r.ok());
    let flat =
        noerr.flat_map(|r: RawData| UpsertRequest::from_data(r, &upsert_value_gen).into_iter());
    let mut pairs = flat.map(|u: UpsertRequest<_, _>| (u.bucket, u.item));
    pairs.try_fold(0, |tot, (bucket, item)| {
        upsert(&bucket, &item).map(|cnt| cnt + tot)
    })
}

/// Saves data got from source which uses a closure to actually save data.
///
/// Duplicates will be ignored.
///
/// # Arguments
/// - source: `RawData` source iterator.
/// - upsert: Data saver which saves data into specified bucket.
/// - upsert_value_gen: Value generator for master buckets.
pub fn upsert_all_ex<I, U, G>(source: I, upsert: &mut U, upsert_value_gen: G) -> Result<u64, Event>
where
    I: Iterator<Item = RawData>,
    U: FnMut(&Bucket, &RawItem) -> Result<u64, Event>,
    G: UpsertValueGenerator,
{
    let mut requests = rawdata2requests(source, upsert_value_gen);
    requests.try_fold(0, |tot, req| {
        let (bucket, v) = req;
        let uniq: Vec<RawItem> = Item::uniq(v);
        upsert_into_bucket(&bucket, &uniq, upsert).map(|cnt| cnt + tot)
    })
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
    let upsert_value_gen = upsert_value_generator_new_func_default();
    upsert_all_ex(source, upsert, upsert_value_gen)
}

/// Creates a bucket before upsert.
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

/// Creates new bucket checker using set.
pub fn create_skip_new_std_set(s: BTreeSet<Bucket>) -> impl Fn(&Bucket) -> Result<(), Event> {
    move |b: &Bucket| {
        s.get(b)
            .map(|_| ())
            .ok_or_else(|| Event::UnexpectedError(String::from("Should not skip")))
    }
}

/// Creates a bucket if not exists.
///
/// # Arguments
/// - create: Creates a bucket.
/// - skip:   Checks if a bucket exists(must return Err on bucket missing).
/// - b:      Bucket to be created.
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
