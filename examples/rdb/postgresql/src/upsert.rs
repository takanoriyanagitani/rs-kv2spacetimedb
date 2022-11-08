use std::collections::BTreeSet;
use std::env;

use rs_kv2spacetimedb::data::{Data, RawData};
use rs_kv2spacetimedb::item::{Item, RawItem};
use rs_kv2spacetimedb::{bucket::Bucket, date::Date, device::Device, evt::Event};

use rs_kv2spacetimedb::kvstore::upsert::{
    create_or_skip, create_skip_new_std_set, upsert_all_shared,
};

use postgres::{Client, Config, NoTls, Transaction};

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

fn pg_upsert_all<I, S>(source: I, t: Transaction, skip: &S) -> Result<u64, Event>
where
    I: Iterator<Item = RawData>,
    S: Fn(&Bucket) -> Result<(), Event>,
{
    let create = |t: &mut Transaction, b: &Bucket| {
        let mut c = |_: &Bucket| pg_create_t(t, b);
        create_or_skip(&mut c, skip, b)
    };
    upsert_all_shared(pg_upsert_t, create, t, pg_finalize_t, source)
}

fn pg_upsert<I, S>(source: I, c: &mut Client, skip: &S) -> Result<u64, Event>
where
    I: Iterator<Item = RawData>,
    S: Fn(&Bucket) -> Result<(), Event>,
{
    let tx: Transaction = c
        .transaction()
        .map_err(|e| Event::UnexpectedError(format!("Unable to start transaction: {}", e)))?;
    pg_upsert_all(source, tx, skip)
}

pub fn upsert() -> Result<(), Event> {
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
