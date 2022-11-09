use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

use std::collections::BTreeMap;

use crate::{
    bucket::Bucket, count::Count, date::Date, datetime::DateTime, device::Device, evt::Event,
};

/// Counts number of rows in a data bucket.
///
/// # Arguments
/// - counter: Counts number of rows in a bucket.
/// - dev: Target device.
/// - date: Target date.
/// - time_source: Gets current date/time.
pub fn count_data_bucket4date<C, T>(
    counter: &mut C,
    dev: &Device,
    date: &Date,
    time_source: &T,
) -> Result<Count, Event>
where
    C: FnMut(&Bucket) -> Result<u64, Event>,
    T: Fn() -> Result<DateTime, Event>,
{
    let b: Bucket = Bucket::new_data_bucket(dev, date);
    let cnt: u64 = counter(&b)?;
    let dt: DateTime = time_source()?;
    Ok(Count::new(cnt, dt))
}

/// Creates new counter which counts number of rows of a data bucket.
///
/// # Arguments
/// - counter: Counts number of rows in a bucket.
/// - time_source: Gets current date/time.
pub fn count_data_bucket4date_new<C, T>(
    mut counter: C,
    time_source: T,
) -> impl FnMut(&Device, &Date) -> Result<Count, Event>
where
    C: FnMut(&Bucket) -> Result<u64, Event>,
    T: Fn() -> Result<DateTime, Event>,
{
    move |dev: &Device, d: &Date| count_data_bucket4date(&mut counter, dev, d, &time_source)
}

/// Creates new counter which uses default time source.
///
/// # Arguments
/// - counter: Counts number of rows in a bucket.
pub fn count_data_bucket4date_new_default_std<C>(
    counter: C,
) -> impl FnMut(&Device, &Date) -> Result<Count, Event>
where
    C: FnMut(&Bucket) -> Result<u64, Event>,
{
    count_data_bucket4date_new(counter, DateTime::time_source_new_std())
}

pub trait Cache {
    fn read(&mut self, b: &Bucket) -> Result<Count, Event>;
    fn write(&mut self, b: &Bucket, c: &Count) -> Result<(), Event>;
}

pub trait Counter {
    fn count(&mut self, b: &Bucket) -> Result<Count, Event>;
}

pub fn counter_new_func<C>(mut c: C) -> impl FnMut(&Bucket) -> Result<Count, Event>
where
    C: Counter,
{
    move |b: &Bucket| c.count(b)
}

struct CounterCached<F, S> {
    fast: F,
    slow: S,
}

struct CounterF<C> {
    counter: C,
}

impl<C> Counter for CounterF<C>
where
    C: FnMut(&Bucket) -> Result<Count, Event>,
{
    fn count(&mut self, b: &Bucket) -> Result<Count, Event> {
        (self.counter)(b)
    }
}

pub fn counter_cached_new<F, S>(cache: F, slow: S) -> impl Counter
where
    F: Cache,
    S: Counter,
{
    CounterCached { fast: cache, slow }
}

pub fn counter_new_from_func<C>(counter: C) -> impl Counter
where
    C: FnMut(&Bucket) -> Result<Count, Event>,
{
    CounterF { counter }
}

pub fn counter_cached_new_default_std<S>(slow: S) -> impl Counter
where
    S: Counter,
{
    let cache = cache_new_std_btree_map();
    counter_cached_new(cache, slow)
}

impl<F, S> Counter for CounterCached<F, S>
where
    F: Cache,
    S: Counter,
{
    fn count(&mut self, b: &Bucket) -> Result<Count, Event> {
        self.fast.read(b).or_else(|_| {
            let cnt: Count = self.slow.count(b)?;
            self.fast.write(b, &cnt)?;
            Ok(cnt)
        })
    }
}

struct CacheShared<T, R, W> {
    shared: T,
    read: R,
    write: W,
}

pub fn cache_new_shared<T, R, W>(shared: T, read: R, write: W) -> impl Cache
where
    R: FnMut(&mut T, &Bucket) -> Result<Count, Event>,
    W: FnMut(&mut T, &Bucket, &Count) -> Result<(), Event>,
{
    CacheShared {
        shared,
        read,
        write,
    }
}

pub fn cache_new_std_btree_map() -> impl Cache {
    let shared: BTreeMap<Bucket, Count> = BTreeMap::new();
    let read = |m: &mut BTreeMap<Bucket, Count>, b: &Bucket| {
        m.get(b)
            .copied()
            .ok_or_else(|| Event::UnexpectedError(String::from("No entry")))
    };
    let write = move |m: &mut BTreeMap<Bucket, Count>, b: &Bucket, c: &Count| match m.get_mut(b) {
        Some(cnt) => {
            cnt.replace(c);
            Ok(())
        }
        None => {
            m.insert(b.clone(), *c);
            Ok(())
        }
    };
    cache_new_shared(shared, read, write)
}

impl<T, R, W> Cache for CacheShared<T, R, W>
where
    R: FnMut(&mut T, &Bucket) -> Result<Count, Event>,
    W: FnMut(&mut T, &Bucket, &Count) -> Result<(), Event>,
{
    fn read(&mut self, b: &Bucket) -> Result<Count, Event> {
        (self.read)(&mut self.shared, b)
    }
    fn write(&mut self, b: &Bucket, c: &Count) -> Result<(), Event> {
        (self.write)(&mut self.shared, b, c)
    }
}

struct CacheF<R, W> {
    read: R,
    write: W,
}

pub fn cache_new<R, W>(read: R, write: W) -> impl Cache
where
    R: FnMut(&Bucket) -> Result<Count, Event>,
    W: FnMut(&Bucket, &Count) -> Result<(), Event>,
{
    CacheF { read, write }
}

impl<R, W> Cache for CacheF<R, W>
where
    R: FnMut(&Bucket) -> Result<Count, Event>,
    W: FnMut(&Bucket, &Count) -> Result<(), Event>,
{
    fn read(&mut self, b: &Bucket) -> Result<Count, Event> {
        (self.read)(b)
    }
    fn write(&mut self, b: &Bucket, c: &Count) -> Result<(), Event> {
        (self.write)(b, c)
    }
}

pub fn count_keys_cached<R, W, S>(
    cache_read: &mut R,
    cache_write: &mut W,
    slow_get: &mut S,
    b: &Bucket,
) -> Result<Count, Event>
where
    R: FnMut(&Bucket) -> Result<Count, Event>,
    W: FnMut(&Bucket, &Count) -> Result<(), Event>,
    S: FnMut(&Bucket) -> Result<Count, Event>,
{
    cache_read(b).or_else(|_| {
        let c: Count = slow_get(b)?;
        cache_write(b, &c)?;
        Ok(c)
    })
}

pub fn count_cached_fast_new<C, D>(
    mut cache_faster: C,
    mut cache_slower: D,
) -> impl FnMut(&Bucket) -> Result<Count, Event>
where
    C: FnMut(&Bucket) -> Result<Count, Event>,
    D: FnMut(&Bucket) -> Result<Count, Event>,
{
    move |b: &Bucket| cache_faster(b).or_else(|_| cache_slower(b))
}

pub fn count_cache_writer_new<C, D>(
    mut cache_faster: C,
    mut cache_slower: D,
) -> impl FnMut(&Bucket, &Count) -> Result<(), Event>
where
    C: FnMut(&Bucket, &Count) -> Result<(), Event>,
    D: FnMut(&Bucket, &Count) -> Result<(), Event>,
{
    move |b: &Bucket, c: &Count| {
        cache_faster(b, c)?;
        cache_slower(b, c)?;
        Ok(())
    }
}

pub fn count_cached_checked<C, S>(
    cache: &mut C,
    stale_checker: &S,
    b: &Bucket,
) -> Result<Count, Event>
where
    C: FnMut(&Bucket) -> Result<Count, Event>,
    S: Fn(&Count) -> bool,
{
    let c: Count = cache(b)?;
    stale_checker(&c)
        .then_some(c)
        .ok_or(Event::CountCacheStale(c))
}

pub fn count_cached_checked_new<C, S>(
    mut cache: C,
    stale_checker: S,
) -> impl FnMut(&Bucket) -> Result<Count, Event>
where
    C: FnMut(&Bucket) -> Result<Count, Event>,
    S: Fn(&Count) -> bool,
{
    move |b: &Bucket| {
        let c: Count = cache(b)?;
        let is_stale: bool = stale_checker(&c);
        let is_fresh: bool = !is_stale;
        is_fresh.then_some(c).ok_or(Event::CountCacheStale(c))
    }
}

pub fn count_cache_fs_writer_new<P>(dirname: P) -> impl FnMut(&Bucket, &Count) -> Result<(), Event>
where
    P: AsRef<Path>,
{
    move |b: &Bucket, c: &Count| {
        let bs: &str = b.as_str();
        let p: &Path = dirname.as_ref();
        let filename = p.join(bs);
        let mut f: File = File::create(filename)
            .map_err(|e| Event::UnexpectedError(format!("Unable to create a file: {}", e)))?;
        let u: [u8; 16] = c.to_be_bytes();
        f.write(&u)
            .map_err(|e| Event::UnexpectedError(format!("Unable to write: {}", e)))?;
        f.flush()
            .map_err(|e| Event::UnexpectedError(format!("Unable to flush: {}", e)))?;
        Ok(())
    }
}

pub fn count_cache_fs_reader_new<P>(dirname: P) -> impl FnMut(&Bucket) -> Result<Count, Event>
where
    P: AsRef<Path>,
{
    move |b: &Bucket| {
        let bs: &str = b.as_str();
        let p: &Path = dirname.as_ref();
        let filename = p.join(bs);
        let mut f: File = File::open(filename)
            .map_err(|e| Event::UnexpectedError(format!("Unable to open cache: {}", e)))?;
        let mut buf: [u8; 16] = [0; 16];
        f.read_exact(&mut buf)
            .map_err(|e| Event::UnexpectedError(format!("Unable to read: {}", e)))?;
        let u: u128 = u128::from_be_bytes(buf);
        Ok(Count::from(u))
    }
}
