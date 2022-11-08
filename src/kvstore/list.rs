use crate::{bucket::Bucket, evt::Event};

pub trait ListBuckets {
    fn list(&mut self) -> Result<Vec<Bucket>, Event>;
}
