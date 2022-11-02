use std::collections::BTreeMap;

use crate::item::{Item, RawItem};
use crate::{bucket::Bucket, data::RawData, date::Date, device::Device, evt::Event};

struct UpsertRequest<K, V> {
    bucket: Bucket,
    item: Item<K, V>,
}

impl<K, V> UpsertRequest<K, V> {
    fn new(bucket: Bucket, item: Item<K, V>) -> Self {
        Self { bucket, item }
    }
}

impl UpsertRequest<Vec<u8>, Vec<u8>> {
    fn from_data(d: RawData) -> Vec<Self> {
        let dev: &Device = d.as_device();
        let date: &Date = d.as_date();

        let i_dates4device: RawItem = Item::new(date.as_bytes().to_vec(), vec![]);
        let i_devices4date: RawItem = Item::new(dev.as_bytes().to_vec(), vec![]);
        let i_dates: RawItem = Item::new(date.as_bytes().to_vec(), vec![]);
        let i_devices: RawItem = Item::new(dev.as_bytes().to_vec(), vec![]);

        let b_data: Bucket = Bucket::new_data_bucket(dev, date);
        let b_dates4device: Bucket = Bucket::new_dates_master_for_device(dev);
        let b_devices4date: Bucket = Bucket::new_devices_master_for_date(date);
        let b_dates: Bucket = Bucket::new_dates_master();
        let b_devices: Bucket = Bucket::new_devices_master();

        let item: RawItem = d.into_item();

        let i_data: RawItem = item;

        vec![
            Self::new(b_data, i_data),
            Self::new(b_dates4device, i_dates4device),
            Self::new(b_devices4date, i_devices4date),
            Self::new(b_dates, i_dates),
            Self::new(b_devices, i_devices),
        ]
    }

    fn bulkdata2map<I>(bulk: I) -> BTreeMap<Bucket, Vec<RawItem>>
    where
        I: Iterator<Item = RawData>,
    {
        let i = bulk.map(Self::from_data).flat_map(|v| v.into_iter());
        i.fold(BTreeMap::new(), |mut m, req| {
            let b: Bucket = req.bucket;
            let i: RawItem = req.item;
            match m.get_mut(&b) {
                None => {
                    let v = vec![i];
                    m.insert(b, v);
                    m
                }
                Some(v) => {
                    v.push(i);
                    m
                }
            }
        })
    }
}

fn rawdata2requests<I>(i: I) -> impl Iterator<Item = (Bucket, Vec<RawItem>)>
where
    I: Iterator<Item = RawData>,
{
    let m: BTreeMap<Bucket, Vec<RawItem>> = UpsertRequest::bulkdata2map(i);
    m.into_iter()
}

fn upsert_into_bucket<U>(b: &Bucket, items: &[RawItem], upsert: &mut U) -> Result<u64, Event>
where
    U: FnMut(&Bucket, &RawItem) -> Result<u64, Event>,
{
    items
        .iter()
        .try_fold(0, |tot, item| upsert(b, item).map(|cnt| cnt + tot))
}

pub fn upsert_all<I, U>(source: I, upsert: &mut U) -> Result<u64, Event>
where
    I: Iterator<Item = RawData>,
    U: FnMut(&Bucket, &RawItem) -> Result<u64, Event>,
{
    let mut requests = rawdata2requests(source);
    requests.try_fold(0, |tot, req| {
        let (bucket, v) = req;
        let uniq: Vec<RawItem> = Item::uniq(v);
        upsert_into_bucket(&bucket, &uniq, upsert).map(|cnt| cnt + tot)
    })
}
