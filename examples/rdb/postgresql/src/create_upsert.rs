use std::env;

use rs_kv2spacetimedb::data::{Data, RawData};
use rs_kv2spacetimedb::item::{Item, RawItem};
use rs_kv2spacetimedb::{bucket::Bucket, date::Date, device::Device, evt::Event};

use rs_kv2spacetimedb::kvstore::upsert::create_upsert_all_shared;

use postgres::{Client, Config, NoTls, Transaction};

fn pg_create_upsert<I, C, U>(source: I, create: C, upsert: U, t: Transaction) -> Result<u64, Event>
where
    I: Iterator<Item = RawData>,
    C: Fn(&Bucket) -> Result<String, Event>,
    U: Fn(&Bucket) -> Result<String, Event>,
{
    let c = |t: &mut Transaction, b: &Bucket| {
        let query: String = create(b)?;
        t.execute(query.as_str(), &[])
            .map_err(|e| Event::UnexpectedError(format!("Unable to create a bucket: {}", e)))
    };
    let u = |t: &mut Transaction, b: &Bucket, i: &RawItem| {
        let key: &[u8] = i.as_key();
        let val: &[u8] = i.as_val();
        let query: String = upsert(b)?;
        t.execute(query.as_str(), &[&key, &val])
            .map_err(|e| Event::UnexpectedError(format!("Unable to upsert: {}", e)))
    };
    let commit = |t: Transaction| {
        t.commit()
            .map_err(|e| Event::UnexpectedError(format!("Unable to commit changes: {}", e)))
    };
    create_upsert_all_shared(source, c, u, t, commit)
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
