use std::env;

use rs_kv2spacetimedb::compose::compose;
use rs_kv2spacetimedb::data::Data;
use rs_kv2spacetimedb::item::{Item, RawItem};
use rs_kv2spacetimedb::{
    bucket::Bucket, date::Date, datetime::DateTime, device::Device, evt::Event,
};

use rs_kv2spacetimedb::kvstore::upsert::{
    upsert_all_converted, upsert_value_generator_new_func, UpsertValueGenerator,
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

fn make_err_fn<F, E, T, U>(f: F) -> impl Fn(T) -> Result<U, E>
where
    F: Fn(T) -> U,
{
    move |t: T| Ok(f(t))
}

#[derive(Default, Debug)]
struct Metrics {
    upserted: u64,
    created: u64,
    converted: u64,
    invalid: u64,
}

impl Metrics {
    fn merge(self, other: Self) -> Self {
        Self {
            upserted: self.upserted + other.upserted,
            created: self.created + other.created,
            converted: self.converted + other.converted,
            invalid: self.invalid + other.invalid,
        }
    }
}

fn pg_upsert_converted<I>(source: I, mut t: Transaction) -> Result<Metrics, Event>
where
    I: Iterator<Item = (Device, Date, RawItem)>,
{
    let conv = |d2r: (Device, Date, RawItem)| {
        let (dev, date, item) = d2r;
        Data::new(dev, date, item)
    };
    let conv_err = make_err_fn(conv);
    let mut met = Metrics::default();
    let mut upsert = |b: &Bucket, i: &RawItem| {
        let cnt_i: u64 = pg_create_t(&mut t, b)?;
        pg_upsert_t(&mut t, b, i).map(|cnt_u: u64| {
            met.created += cnt_i;
            met.upserted += cnt_u;
            cnt_u + cnt_i
        })
    };
    let mut conv_met = Metrics::default();
    let upst_valgen = upsert_val_gen_new();
    let mut inspect = |r: &Result<_, _>| match r {
        Ok(_) => {
            conv_met.converted += 1;
        }
        Err(e) => {
            eprintln!("Unable to convert: {:#?}", e);
            conv_met.invalid += 1;
        }
    };
    upsert_all_converted(source, &mut upsert, upst_valgen, &conv_err, &mut inspect)?;
    pg_finalize_t(t)?;
    Ok(met.merge(conv_met))
}

pub fn upsert_convert() -> Result<(), Event> {
    let mut c: Client = Config::new()
        .host(env::var("PGHOST").unwrap().as_str())
        .dbname(env::var("PGDATABASE").unwrap().as_str())
        .user(env::var("PGUSER").unwrap().as_str())
        .password(env::var("PGPASSWORD").unwrap_or_default())
        .connect(NoTls)
        .map_err(|e| Event::ConnectError(format!("Unable to connect: {}", e)))?;

    let tups = vec![(
        Device::new_unchecked("cafef00ddeadbeafface864299792458".into()),
        Date::new_unchecked("2022_11_11".into()),
        Item::new(
            String::from("07:48:32.0Z").into_bytes(),
            String::from("").into_bytes(),
        ),
    )];

    let tx: Transaction = c.transaction().unwrap();

    let m: Metrics = pg_upsert_converted(tups.into_iter(), tx)?;

    println!("met: {:#?}", m);

    Ok(())
}
