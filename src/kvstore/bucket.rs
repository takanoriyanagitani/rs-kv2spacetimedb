use std::collections::BTreeSet;

use crate::{bucket::Bucket, evt::Event};

pub fn list_bucket<L>(mut list: L) -> impl FnMut() -> Result<BTreeSet<Bucket>, Event>
where
    L: FnMut() -> Result<Vec<String>, Event>,
{
    move || {
        let buckets: Vec<String> = list()?;
        let mapd = buckets.into_iter().map(Bucket::from);
        Ok(BTreeSet::from_iter(mapd))
    }
}
