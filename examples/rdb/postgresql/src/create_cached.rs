use std::collections::BTreeSet;
use std::env;

use rs_kv2spacetimedb::data::{Data, RawData};
use rs_kv2spacetimedb::item::{Item, RawItem};
use rs_kv2spacetimedb::{bucket::Bucket, date::Date, device::Device, evt::Event};

use rs_kv2spacetimedb::kvstore::upsert::{create_cached_new, create_upsert_all_shared};

use postgres::{Client, Config, NoTls, Row, Transaction};

fn pg_create(t: &mut Transaction, query: &str) -> Result<u64, Event> {
    t.execute(query, &[])
        .map_err(|e| Event::UnexpectedError(format!("Unable to create a bucket: {}", e)))
}

fn pg_upsert(t: &mut Transaction, query: &str, i: &RawItem) -> Result<u64, Event> {
    let key: &[u8] = i.as_key();
    let val: &[u8] = i.as_val();
    t.execute(query, &[&key, &val])
        .map_err(|e| Event::UnexpectedError(format!("Unable to upsert: {}", e)))
}

fn pg_create_upsert<I, C, U>(
    source: I,
    create: C,
    upsert: U,
    t: Transaction,
    table_set: BTreeSet<String>,
) -> Result<u64, Event>
where
    I: Iterator<Item = RawData>,
    C: Fn(&Bucket) -> Result<String, Event>,
    U: Fn(&Bucket) -> Result<String, Event>,
{
    let table_cache = |b: &Bucket| {
        let n: &str = b.as_str();
        table_set
            .get(n)
            .ok_or_else(|| Event::UnexpectedError(String::from("This error must be ignored")))
            .map(|_| 0)
    };

    let c = |t: &mut Transaction, b: &Bucket| {
        let query: String = create(b)?;
        let bucket2create = |_: &Bucket| pg_create(t, query.as_str());
        let mut cached_create = create_cached_new(bucket2create, table_cache);
        cached_create(b)
    };

    let u = |t: &mut Transaction, b: &Bucket, i: &RawItem| {
        let query: String = upsert(b)?;
        pg_upsert(t, query.as_str(), i)
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

fn pg_row2name(r: &Row) -> Result<String, Event> {
    r.try_get(0)
        .map_err(|e| Event::UnexpectedError(format!("Unable to get table name from row: {}", e)))
}

fn pg_get_tablenames_new<F>(row2name: F) -> impl Fn(&mut Client) -> Result<Vec<String>, Event>
where
    F: Fn(&Row) -> Result<String, Event>,
{
    move |c: &mut Client| {
        let rows: Vec<Row> = c
            .query(
                r#"
                    SELECT table_name::TEXT
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                "#,
                &[],
            )
            .map_err(|e| Event::UnexpectedError(format!("Unable to get table names: {}", e)))?;
        let l = rows.len();
        rows.into_iter()
            .try_fold(Vec::with_capacity(l), |mut v, row| {
                row2name(&row).map(|n| v.push(n)).map(|_| v)
            })
    }
}

fn pg_get_tablenames_new_default() -> impl Fn(&mut Client) -> Result<Vec<String>, Event> {
    pg_get_tablenames_new(pg_row2name)
}

pub fn create_cached() -> Result<(), Event> {
    let mut c: Client = Config::new()
        .host(env::var("PGHOST").unwrap().as_str())
        .dbname(env::var("PGDATABASE").unwrap().as_str())
        .user(env::var("PGUSER").unwrap().as_str())
        .password(env::var("PGPASSWORD").unwrap_or_default())
        .connect(NoTls)
        .map_err(|e| Event::ConnectError(format!("Unable to connect: {}", e)))?;

    let table_names: Vec<String> = pg_get_tablenames_new_default()(&mut c)?;
    let table_set: BTreeSet<_> = BTreeSet::from_iter(table_names);

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

    let cnt: u64 = pg_create_upsert(raws.into_iter(), bc, bu, t, table_set)?;
    println!("upserted: {}", cnt);
    Ok(())
}
