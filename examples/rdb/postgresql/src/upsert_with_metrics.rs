use std::collections::BTreeSet;
use std::env;

use rs_kv2spacetimedb::compose::compose;
use rs_kv2spacetimedb::data::{Data, RawData};
use rs_kv2spacetimedb::item::{Item, RawItem};
use rs_kv2spacetimedb::{
    bucket::Bucket, date::Date, datetime::DateTime, device::Device, evt::Event,
};

use rs_kv2spacetimedb::kvstore::upsert::{
    create_or_skip, create_skip_new_std_set, request_upsert_all_shared_ex,
    upsert_value_generator_new_func, UpsertValueGenerator,
};

use postgres::{Client, Config, NoTls, Transaction};

fn dt2us(d: DateTime) -> u64 {
    d.as_unixtime_us()
}

fn u2vec(u: u64) -> Vec<u8> {
    u.to_be_bytes().into()
}

fn dt2us2bytes_new() -> impl Fn(DateTime) -> Vec<u8> {
    compose(dt2us, u2vec)
}

fn time2bytes<T, C>(t: &T, c: &C) -> Result<Vec<u8>, Event>
where
    T: Fn() -> Result<DateTime, Event>,
    C: Fn(DateTime) -> Vec<u8>,
{
    let d: DateTime = t()?;
    Ok(c(d))
}

fn time2bytes_new<T, C>(t: T, c: C) -> impl Fn() -> Vec<u8>
where
    T: Fn() -> Result<DateTime, Event>,
    C: Fn(DateTime) -> Vec<u8>,
{
    move || time2bytes(&t, &c).unwrap_or_default()
}

fn time2bytes_new_default() -> impl Fn() -> Vec<u8> {
    time2bytes_new(DateTime::time_source_new_std(), dt2us2bytes_new())
}

fn upsert_val_gen_new() -> impl UpsertValueGenerator {
    let v1 = time2bytes_new_default();
    let v2 = time2bytes_new_default();
    let v3 = time2bytes_new_default();
    let v4 = time2bytes_new_default();

    upsert_value_generator_new_func(
        move |d: &Date| Item::new(d.as_bytes().to_vec(), v1()),
        move |d: &Device| Item::new(d.as_bytes().to_vec(), v2()),
        move |d: &Date| Item::new(d.as_bytes().to_vec(), v3()),
        move |d: &Device| Item::new(d.as_bytes().to_vec(), v4()),
    )
}

fn pg_upsert_t(t: &mut Transaction, b: &Bucket, i: &RawItem) -> Result<u64, Event> {
    let query: String = format!(
        r#"
            INSERT INTO {} AS tgt
            VALUES ($1::BYTEA, $2::BYTEA)
            ON CONFLICT ON CONSTRAINT {}_pkc
            DO UPDATE
            SET val = EXCLUDED.val
            WHERE tgt.val <> EXCLUDED.val
        "#,
        b.as_str(),
        b.as_str(),
    );
    let key: &[u8] = i.as_key();
    let val: &[u8] = i.as_val();
    t.execute(query.as_str(), &[&key, &val])
        .map_err(|e| Event::UnexpectedError(format!("Unable to upsert: {}", e)))
}

fn pg_create_t(t: &mut Transaction, b: &Bucket) -> Result<u64, Event> {
    let query: String = format!(
        r#"
            CREATE TABLE IF NOT EXISTS {}(
                key BYTEA,
                val BYTEA,
                CONSTRAINT {}_pkc PRIMARY KEY(key)
            )
        "#,
        b.as_str(),
        b.as_str(),
    );
    t.execute(query.as_str(), &[])
        .map_err(|e| Event::UnexpectedError(format!("Unable to create a bucket: {}", e)))
}

fn pg_finalize_t(t: Transaction) -> Result<(), Event> {
    t.commit()
        .map_err(|e| Event::UnexpectedError(format!("Unable to commit changes: {}", e)))
}

#[derive(Debug)]
struct Metrics {
    upserted: u64,
    created: u64,
}

impl Metrics {
    fn total(&self) -> u64 {
        self.upserted + self.created
    }
}

fn pg_upsert_all_with_metrics<I, S>(source: I, t: Transaction, skip: &S) -> Result<Metrics, Event>
where
    I: Iterator<Item = RawData>,
    S: Fn(&Bucket) -> Result<(), Event>,
{
    let m = Metrics {
        upserted: 0,
        created: 0,
    };
    let pair: (Metrics, Transaction) = (m, t);

    let create = |mt: &mut (Metrics, Transaction), b: &Bucket| {
        let (m, t) = mt;
        let mut c = |_: &Bucket| pg_create_t(t, b);
        create_or_skip(&mut c, skip, b).map(|cnt: u64| {
            m.created += cnt;
            cnt
        })
    };
    let upsert = |mt: &mut (Metrics, Transaction), b: &Bucket, i: &RawItem| {
        let (m, t) = mt;
        pg_upsert_t(t, b, i).map(|cnt: u64| {
            m.upserted += cnt;
            cnt
        })
    };
    let upst_valgen = upsert_val_gen_new();
    let pair_neo = request_upsert_all_shared_ex(upsert, create, pair, source, upst_valgen)?;
    let (met, tx) = pair_neo;
    pg_finalize_t(tx)?;
    Ok(met)
}

fn pg_upsert<I, S>(source: I, c: &mut Client, skip: &S) -> Result<u64, Event>
where
    I: Iterator<Item = RawData>,
    S: Fn(&Bucket) -> Result<(), Event>,
{
    let tx: Transaction = c
        .transaction()
        .map_err(|e| Event::UnexpectedError(format!("Unable to start transaction: {}", e)))?;
    let m: Metrics = pg_upsert_all_with_metrics(source, tx, skip)?;
    println!("metrics: {:#?}", m);
    Ok(m.total())
}

pub fn upsert_with_metrics() -> Result<(), Event> {
    let mut c: Client = Config::new()
        .host(env::var("PGHOST").unwrap().as_str())
        .dbname(env::var("PGDATABASE").unwrap().as_str())
        .user(env::var("PGUSER").unwrap().as_str())
        .password(env::var("PGPASSWORD").unwrap_or_default())
        .connect(NoTls)
        .map_err(|e| Event::ConnectError(format!("Unable to connect: {}", e)))?;

    let mut m: BTreeSet<Bucket> = BTreeSet::new();

    m.insert(Bucket::from(String::from("devices_2022_11_02")));

    let skip = create_skip_new_std_set(m);

    let raws = vec![Data::new(
        Device::new_unchecked(String::from("cafef00ddeadbeafface864299792458")),
        Date::new_unchecked(String::from("2022_11_01")),
        Item::new(
            String::from("08:26:26.0Z").into_bytes(),
            String::from(
                r#"{
                    "timestamp": "2022-11-01T08:26:26.0Z",
                    "data" [
                    ]
                }"#,
            )
            .into_bytes(),
        ),
    )];

    let cnt: u64 = pg_upsert(raws.into_iter(), &mut c, &skip)?;
    println!("upserted: {}", cnt);
    Ok(())
}
