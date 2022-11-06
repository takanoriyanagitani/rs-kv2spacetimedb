use std::ops::DerefMut;
use std::sync::Mutex;

use crate::remove::{is_delete_target, is_drop_target};
use crate::{bucket::Bucket, date::Date, evt::Event};

/// Drops buckets and deletes rows from buckets for specified date.
///
/// # Arguments
/// - sel: Gets all buckets.
/// - drp: Drop a bucket.
/// - del: Delete a row from a bucket.
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
