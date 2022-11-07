use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

use crate::{bucket::Bucket, count::Count, evt::Event};

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
