use crate::{bucket::Bucket, evt::Event};

/// Gets all buckets.
pub trait ListBuckets {
    fn list(&mut self) -> Result<Vec<Bucket>, Event>;
}

/// Gets all keys from a bucket.
pub trait ListKeys<K> {
    fn list(&mut self, b: &Bucket) -> Result<Vec<K>, Event>;
}
