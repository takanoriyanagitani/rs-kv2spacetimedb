use std::ops::DerefMut;
use std::sync::Mutex;

use crate::remove::{is_delete_target, is_drop_target, is_drop_target_stale};
use crate::{bucket::Bucket, date::Date, evt::Event};

use crate::kvstore::list::ListBuckets;

pub trait DropBucket {
    fn drop(&mut self, b: &Bucket) -> Result<u64, Event>;
}

pub trait DeleteRow {
    fn delete(&mut self, b: &Bucket, key: &[u8]) -> Result<u64, Event>;
    fn finalize(self) -> Result<(), Event>;
}

pub fn delete_stale_data<D, T, R>(
    mut drop_del_list: D,
    drop_target: &T,
    remove_target: &R,
    lbi: Date,
) -> Result<u64, Event>
where
    D: DropBucket + DeleteRow + ListBuckets,
    T: Fn(&Bucket, &Date) -> bool,
    R: Fn(&Bucket) -> bool,
{
    let vb: Vec<Bucket> = drop_del_list.list()?;
    let drop_cnt: u64 = vb.iter().try_fold(0, |tot, b| {
        let tgt: bool = drop_target(b, &lbi);
        match tgt {
            true => drop_del_list.drop(b).map(|cnt| cnt + tot),
            false => Ok(tot),
        }
    })?;
    let del_cnt: u64 = vb.iter().try_fold(0, |tot, b| {
        let tgt: bool = remove_target(b);
        match tgt {
            true => drop_del_list.delete(b, lbi.as_bytes()).map(|cnt| cnt + tot),
            false => Ok(tot),
        }
    })?;
    drop_del_list.finalize()?;
    Ok(drop_cnt + del_cnt)
}

pub fn delete_stale_data_default<D>(drop_del_list: D, lbi: Date) -> Result<u64, Event>
where
    D: DropBucket + DeleteRow + ListBuckets,
{
    delete_stale_data(drop_del_list, &is_drop_target_stale, &is_delete_target, lbi)
}

/// Drops stale bucket and deletes stale rows from buckets.
///
/// # Arguments
/// - drp: Drops a bucket.
/// - del: Deletes rows from a bucket.
/// - lst: Gets list of buckets.
/// - shared: Vendor specific shared resource required to drop/delete/finalize/get list.
/// - finalize: Vendor specific finalization for the shared resource.
/// - lbi: Date threshold(fresh data lower bound, inclusive)
pub fn delete_stale_data_default_func<D, E, L, T, F>(
    drp: D,
    del: E,
    lst: L,
    shared: T,
    finalize: F,
    lbi: Date,
) -> Result<u64, Event>
where
    D: FnMut(&mut T, &Bucket) -> Result<u64, Event>,
    E: FnMut(&mut T, &Bucket, &[u8]) -> Result<u64, Event>,
    L: FnMut(&mut T) -> Result<Vec<Bucket>, Event>,
    F: Fn(T) -> Result<(), Event>,
{
    let dds = DelDropShared {
        drp,
        del,
        lst,
        shared,
        finalize,
    };
    delete_stale_data_default(dds, lbi)
}

struct DelDropShared<D, E, L, T, F> {
    drp: D,
    del: E,
    lst: L,
    shared: T,
    finalize: F,
}

impl<D, E, L, T, F> DropBucket for DelDropShared<D, E, L, T, F>
where
    D: FnMut(&mut T, &Bucket) -> Result<u64, Event>,
{
    fn drop(&mut self, b: &Bucket) -> Result<u64, Event> {
        (self.drp)(&mut self.shared, b)
    }
}

impl<D, E, L, T, F> DeleteRow for DelDropShared<D, E, L, T, F>
where
    E: FnMut(&mut T, &Bucket, &[u8]) -> Result<u64, Event>,
    F: Fn(T) -> Result<(), Event>,
{
    fn delete(&mut self, b: &Bucket, key: &[u8]) -> Result<u64, Event> {
        (self.del)(&mut self.shared, b, key)
    }
    fn finalize(self) -> Result<(), Event> {
        (self.finalize)(self.shared)
    }
}

impl<D, E, L, T, F> ListBuckets for DelDropShared<D, E, L, T, F>
where
    L: FnMut(&mut T) -> Result<Vec<Bucket>, Event>,
{
    fn list(&mut self) -> Result<Vec<Bucket>, Event> {
        (self.lst)(&mut self.shared)
    }
}

/// Drops stale buckets and deletes stale rows from buckets.
///
/// # Arguments
/// - sel: Gets all buckets.
/// - drp: Drops a bucket.
/// - del: Deletes stale rows.
/// - lbi: Date threshold(lower bound, inclusive)
/// - flt: Checks the bucket name to drop.
/// - flu: Checks the bucket name to delete.
pub fn remove_stale_data<S, F, G, D, R>(
    sel: &mut S,
    drp: &mut D,
    del: &mut R,
    lbi: &Date,
    flt: &F, // bucket to drop
    flu: &G, // bucket to modify(delete a row)
) -> Result<u64, Event>
where
    S: FnMut() -> Result<Vec<Bucket>, Event>,
    F: Fn(&Bucket, &Date) -> bool,
    G: Fn(&Bucket) -> bool,
    D: FnMut(&Bucket) -> Result<u64, Event>,
    R: FnMut(&Bucket, &[u8]) -> Result<u64, Event>,
{
    let date_bytes: &[u8] = lbi.as_bytes();

    let buckets_all: Vec<Bucket> = sel()?;

    let mut del_targets = buckets_all.iter().filter(|b| flu(b));
    let del_cnt: u64 =
        del_targets.try_fold(0, |tot, tgt| del(tgt, date_bytes).map(|cnt| cnt + tot))?;

    let mut drp_targets = buckets_all.iter().filter(|b| flt(b, lbi));
    let drp_cnt: u64 = drp_targets.try_fold(0, |tot, tgt| drp(tgt).map(|cnt| cnt + tot))?;
    Ok(del_cnt + drp_cnt)
}

/// Drops stale buckets and deletes stale rows from buckets which uses default checkers.
///
/// # Arguments
/// - sel: Gets all buckets.
/// - drp: Drops a bucket.
/// - del: Deletes stale rows.
/// - lbi: Date threshold(lower bound, inclusive)
pub fn remove_stale_data_default<S, D, R>(
    sel: &mut S,
    drp: &mut D,
    del: &mut R,
    lbi: &Date,
) -> Result<u64, Event>
where
    S: FnMut() -> Result<Vec<Bucket>, Event>,
    D: FnMut(&Bucket) -> Result<u64, Event>,
    R: FnMut(&Bucket, &[u8]) -> Result<u64, Event>,
{
    remove_stale_data(sel, drp, del, lbi, &is_drop_target_stale, &is_delete_target)
}

/// Drops stale buckets and deletes stale rows using shared resource.
///
/// # Arguments
/// - sel: Gets all buckets.
/// - drp: Drops a bucket.
/// - del: Deletes stale rows.
/// - lbi: Date threshold(lower bound, inclusive).
/// - t:   Vendor specific shared resource(example: Transaction object).
/// - f:   Finalizes the shared resource.
pub fn remove_stale_data_default_shared<S, D, R, T, F>(
    sel: &S,
    drp: &D,
    del: &R,
    lbi: &Date,
    t: T,
    finalize: &F,
) -> Result<u64, Event>
where
    S: Fn(&mut T) -> Result<Vec<Bucket>, Event>,
    D: Fn(&mut T, &Bucket) -> Result<u64, Event>,
    R: Fn(&mut T, &Bucket, &[u8]) -> Result<u64, Event>,
    F: Fn(T) -> Result<(), Event>,
{
    let mt: Mutex<T> = Mutex::new(t);
    let rsd = || {
        let mut fsel = || {
            let mut g = mt.lock().map_err(|e| {
                Event::UnexpectedError(format!("Unable to get a shared resource: {}", e))
            })?;
            let m: &mut T = g.deref_mut();
            sel(m)
        };
        let mut fdrp = |b: &Bucket| {
            let mut g = mt.lock().map_err(|e| {
                Event::UnexpectedError(format!("Unable to get a shared resource: {}", e))
            })?;
            let m: &mut T = g.deref_mut();
            drp(m, b)
        };
        let mut fdel = |b: &Bucket, key: &[u8]| {
            let mut g = mt.lock().map_err(|e| {
                Event::UnexpectedError(format!("Unable to get a shared resource: {}", e))
            })?;
            let m: &mut T = g.deref_mut();
            del(m, b, key)
        };
        remove_stale_data_default(&mut fsel, &mut fdrp, &mut fdel, lbi)
    };
    let cnt: u64 = rsd()?;
    let t: T = mt
        .into_inner()
        .map_err(|e| Event::UnexpectedError(format!("Unable to get shared resource: {}", e)))?;
    finalize(t)?;
    Ok(cnt)
}

/// Drops buckets and deletes rows from buckets for specified date.
///
/// # Arguments
/// - sel: Gets all buckets.
/// - drp: Drops a bucket.
/// - del: Deletes a row from a bucket.
/// - d:   Target `Date`.
/// - flt: Checks bucket name to drop.
/// - flu: Checks bucket name to delete.
pub fn remove_by_date<S, F, G, D, R>(
    sel: &mut S,
    drp: &mut D,
    del: &mut R,
    d: &Date,
    flt: &F, // bucket to drop
    flu: &G, // bucket to modify(delete a row)
) -> Result<u64, Event>
where
    S: FnMut() -> Result<Vec<Bucket>, Event>,
    F: Fn(&Bucket, &Date) -> bool,
    G: Fn(&Bucket) -> bool,
    D: FnMut(&Bucket) -> Result<u64, Event>,
    R: FnMut(&Bucket, &[u8]) -> Result<u64, Event>,
{
    let b: &[u8] = d.as_bytes();

    let buckets_all: Vec<Bucket> = sel()?;

    let mut del_targets = buckets_all.iter().filter(|b| flu(b));
    let del_cnt: u64 = del_targets.try_fold(0, |tot, tgt| del(tgt, b).map(|cnt| cnt + tot))?;

    let mut drp_targets = buckets_all.iter().filter(|b| flt(b, d));
    let drp_cnt: u64 = drp_targets.try_fold(0, |tot, tgt| drp(tgt).map(|cnt| cnt + tot))?;
    Ok(del_cnt + drp_cnt)
}

/// Drops buckets and deletes rows which uses default closures to check bucket names.
///
/// # Arguments
/// - sel: Gets all buckets.
/// - drp: Drop a bucket.
/// - del: Delete a row from a bucket.
/// - d:   Target `Date`.
pub fn remove_by_date_default<S, D, R>(
    sel: &mut S,
    drp: &mut D,
    del: &mut R,
    d: &Date,
) -> Result<u64, Event>
where
    S: FnMut() -> Result<Vec<Bucket>, Event>,
    D: FnMut(&Bucket) -> Result<u64, Event>,
    R: FnMut(&Bucket, &[u8]) -> Result<u64, Event>,
{
    remove_by_date(sel, drp, del, d, &is_drop_target, &is_delete_target)
}

/// Drops buckets and deletes rows which uses default closures with shared resource.
///
/// # Arguments
/// - sel: Gets all buckets.
/// - drp: Drop a bucket.
/// - del: Delete a row from a bucket.
/// - d:   Target `Date`.
/// - t:   Vendor specific shared resource(example: Transaction object).
/// - finalize: Commit changes using the shared resource
pub fn remove_by_date_default_shared<S, D, R, T, F>(
    sel: &mut S,
    drp: &mut D,
    del: &mut R,
    d: &Date,
    t: T,
    finalize: &F,
) -> Result<u64, Event>
where
    S: FnMut(&mut T) -> Result<Vec<Bucket>, Event>,
    D: FnMut(&mut T, &Bucket) -> Result<u64, Event>,
    R: FnMut(&mut T, &Bucket, &[u8]) -> Result<u64, Event>,
    F: Fn(T) -> Result<(), Event>,
{
    let mt: Mutex<T> = Mutex::new(t);
    let mut rbd = || {
        let mut fsel = || {
            let mut g = mt.lock().map_err(|e| {
                Event::UnexpectedError(format!("Unable to get a shared resource: {}", e))
            })?;
            let m: &mut T = g.deref_mut();
            sel(m)
        };
        let mut fdrp = |b: &Bucket| {
            let mut g = mt.lock().map_err(|e| {
                Event::UnexpectedError(format!("Unable to get a shared resource: {}", e))
            })?;
            let m: &mut T = g.deref_mut();
            drp(m, b)
        };
        let mut fdel = |b: &Bucket, date: &[u8]| {
            let mut g = mt.lock().map_err(|e| {
                Event::UnexpectedError(format!("Unable to get a shared resource: {}", e))
            })?;
            let m: &mut T = g.deref_mut();
            del(m, b, date)
        };
        remove_by_date_default(&mut fsel, &mut fdrp, &mut fdel, d)
    };
    let cnt: u64 = rbd()?;
    let t: T = mt
        .into_inner()
        .map_err(|e| Event::UnexpectedError(format!("Unable to get shared resource: {}", e)))?;
    finalize(t)?;
    Ok(cnt)
}
