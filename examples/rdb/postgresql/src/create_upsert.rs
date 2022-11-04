use std::env;
use std::ops::DerefMut;
use std::sync::Mutex;

use rs_kv2spacetimedb::data::{Data, RawData};
use rs_kv2spacetimedb::item::{Item, RawItem};
use rs_kv2spacetimedb::{bucket::Bucket, date::Date, device::Device, evt::Event};

use rs_kv2spacetimedb::kvstore::upsert::{create_upsert_new, upsert_all};

use postgres::{Client, Config, NoTls, Transaction};

fn pg_create_upsert<I, C, U>(
    requests: I,
    create: C,
    upsert: U,
    t: Transaction,
) -> Result<u64, Event>
where
    I: Iterator<Item = RawData>,
    C: Fn(&Bucket) -> Result<String, Event>,
    U: Fn(&Bucket) -> Result<String, Event>,
{
    let mt: Mutex<Transaction> = Mutex::new(t);

    let c = |b: &Bucket| {
        let mut l = mt
            .lock()
            .map_err(|e| Event::UnexpectedError(format!("Unable to insert while upsert: {}", e)))?;
        let t: &mut Transaction = l.deref_mut();
        let query: String = create(b)?;
        t.execute(query.as_str(), &[])
            .map_err(|e| Event::UnexpectedError(format!("Unable to create a bucket: {}", e)))
    };

    let u = |b: &Bucket, i: &RawItem| {
        let mut l = mt
            .lock()
            .map_err(|e| Event::UnexpectedError(format!("Unable to upsert while insert: {}", e)))?;
        let t: &mut Transaction = l.deref_mut();
        let query: String = upsert(b)?;
        let key: &[u8] = i.as_key();
        let val: &[u8] = i.as_val();
        t.execute(query.as_str(), &[&key, &val])
            .map_err(|e| Event::UnexpectedError(format!("Unable to create a bucket: {}", e)))
    };

    let mut cu = create_upsert_new(c, u);

    let cnt: u64 = upsert_all(requests, &mut cu)?;

    drop(cu);

    let t: Transaction = mt
        .into_inner()
        .map_err(|_| Event::UnexpectedError("Unable to get transaction".into()))?;
    t.commit()
        .map_err(|e| Event::UnexpectedError(format!("Unable to commit changes: {}", e)))?;
    Ok(cnt)
}

fn create_query_new() -> impl Fn(&Bucket) -> Result<String, Event> {
    move |b: &Bucket| {
        Ok(format!(
            r#"
                CREATE TABLE IF NOT EXISTS {} (
                    key BYTEA,
                    val BYTEA,
                    CONSTRAINT {}_pkc PRIMARY KEY(key)
                )
            "#,
            b.as_str(),
            b.as_str(),
        ))
    }
}

fn upsert_query_new() -> impl Fn(&Bucket) -> Result<String, Event> {
    move |b: &Bucket| {
        Ok(format!(
            r#"
                INSERT INTO {} AS tgt
                VALUES($1::BYTEA, $2::BYTEA)
                ON CONFLICT ON CONSTRAINT {}_pkc
                DO UPDATE
                SET val = EXCLUDED.val
                WHERE tgt.val <> EXCLUDED.val
            "#,
            b.as_str(),
            b.as_str(),
        ))
    }
}

pub fn create_upsert() -> Result<(), Event> {
    let mut c: Client = Config::new()
        .host(env::var("PGHOST").unwrap().as_str())
        .dbname(env::var("PGDATABASE").unwrap().as_str())
        .user(env::var("PGUSER").unwrap().as_str())
        .password(env::var("PGPASSWORD").unwrap_or_default())
        .connect(NoTls)
        .map_err(|e| Event::ConnectError(format!("Unable to connect: {}", e)))?;

    let bc = create_query_new();
    let bu = upsert_query_new();

    let raws = vec![Data::new(
        Device::new_unchecked(String::from("dafef00ddeadbeafface864299792458")),
        Date::new_unchecked(String::from("2022_11_04")),
        Item::new(
            String::from("03:46:58.0Z").into_bytes(),
            String::from(
                r#"{
                    "timestamp": "2022-11-04T03:46:58.0Z",
                    "data" [
                    ]
                }"#,
            )
            .into_bytes(),
        ),
    )];

    let t: Transaction = c
        .transaction()
        .map_err(|e| Event::UnexpectedError(format!("Unable to start transaction: {}", e)))?;

    let cnt: u64 = pg_create_upsert(raws.into_iter(), bc, bu, t)?;
    println!("upserted: {}", cnt);
    Ok(())
}
