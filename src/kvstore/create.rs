use crate::{bucket::Bucket, evt::Event};

pub trait Create {
    fn create(&mut self, b: &Bucket) -> Result<u64, Event>;
}
