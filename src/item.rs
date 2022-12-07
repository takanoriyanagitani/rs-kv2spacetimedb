//! An Item(key/value pair)

use std::collections::BTreeSet;

/// A Key/Value pair.
#[derive(Debug, PartialEq, PartialOrd, Eq, Ord)]
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
    /// Creates new key/value pair.
    pub fn new(key: K, val: V) -> Self {
        Self { key, val }
    }

    /// Unpacks key/value pair.
    pub fn into_pair(self) -> (K, V) {
        (self.key, self.val)
    }

    /// Removes duplicates.
    pub fn uniq(v: Vec<Self>) -> Vec<Self> {
        let s = BTreeSet::from_iter(v.into_iter());
        s.into_iter().collect()
    }

    /// Gets the key reference.
    pub fn as_key(&self) -> &K {
        &self.key
    }

    /// Gets the value reference.
    pub fn as_val(&self) -> &V {
        &self.val
    }
}

#[cfg(test)]
mod test_item {

    mod uniq {
        use crate::item::Item;

        #[test]
        fn test_empty() {
            let items: Vec<Item<u8, u8>> = vec![];
            let unq = Item::uniq(items);
            assert_eq!(unq, vec![]);
        }

        #[test]
        fn test_integers() {
            let items: Vec<Item<u8, u8>> = vec![
                Item::new(0x42, 0x42),
                Item::new(0x42, 0x43),
                Item::new(0x42, 0x43),
                Item::new(0x43, 0x43),
            ];
            let unq = Item::uniq(items);
            assert_eq!(
                unq,
                vec![
                    Item::new(0x42, 0x42),
                    Item::new(0x42, 0x43),
                    Item::new(0x43, 0x43),
                ]
            );
        }
    }

    mod raw {
        use crate::item::{Item, RawItem};

        #[test]
        fn test_ascii() {
            let i: RawItem = Item::new(b"key".to_vec(), b"42".to_vec());

            let kr: &[u8] = i.as_key();
            assert_eq!(kr, b"key");

            let vr: &[u8] = i.as_val();
            assert_eq!(vr, b"42");

            let (k, v) = i.into_pair();
            assert_eq!(&k, b"key");
            assert_eq!(&v, b"42");
        }
    }
}
