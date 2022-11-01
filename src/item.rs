use std::collections::BTreeSet;

#[derive(PartialEq, PartialOrd, Eq, Ord)]
pub struct Item<K, V> {
    key: K,
    val: V,
}

pub type RawItem = Item<Vec<u8>, Vec<u8>>;

impl<K, V> Item<K, V>
where
    K: Ord,
    V: Ord,
{
    pub fn new(key: K, val: V) -> Self {
        Self { key, val }
    }

    pub fn into_pair(self) -> (K, V) {
        (self.key, self.val)
    }

    pub fn uniq(v: Vec<Self>) -> Vec<Self> {
        let s = BTreeSet::from_iter(v.into_iter());
        s.into_iter().collect()
    }

    pub fn as_key(&self) -> &K {
        &self.key
    }
    pub fn as_val(&self) -> &V {
        &self.val
    }
}
