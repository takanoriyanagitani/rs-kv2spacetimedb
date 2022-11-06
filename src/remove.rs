use crate::{bucket::Bucket, date::Date, evt::Event};

fn is_drop_target_str(bs: &str, ds: &str) -> Result<bool, Event> {
    let pat = format!("data_{}", ds);
    Ok(bs.starts_with(&pat))
}

/// Checks if the bucket must be dropped.
pub fn is_drop_target(b: &Bucket, d: &Date) -> bool {
    let bs: &str = b.as_str();
    let ds: &str = d.as_str();

    format!("devices_{}", ds)
        .eq(bs)
        .then_some(true)
        .unwrap_or_else(|| is_drop_target_str(bs, ds).unwrap_or(false))
}

fn is_delete_target_str(bs: &str) -> Result<bool, Event> {
    Ok(bs.starts_with("dates_"))
}

/// Checks if the bucket can have rows to be deleted.
pub fn is_delete_target(b: &Bucket) -> bool {
    let bs: &str = b.as_str();

    "dates"
        .eq(bs)
        .then_some(true)
        .unwrap_or_else(|| is_delete_target_str(bs).unwrap_or(false))
}
