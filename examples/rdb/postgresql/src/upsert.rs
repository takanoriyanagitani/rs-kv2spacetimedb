use std::env;

use rs_kv2spacetimedb::data::{Data, RawData};
use rs_kv2spacetimedb::item::{Item, RawItem};
use rs_kv2spacetimedb::{bucket::Bucket, date::Date, device::Device, evt::Event};

use rs_kv2spacetimedb::kvstore::upsert::upsert_all;

use postgres::{Client, Config, NoTls, Transaction};

fn pg_upsert_all<I>(source: I, mut t: Transaction) -> Result<u64, Event>
where
    I: Iterator<Item = RawData>,
{
    let mut upst = |b: &Bucket, i: &RawItem| {
        let key: &[u8] = i.as_key();
        let val: &[u8] = i.as_val();
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
        t.execute(query.as_str(), &[&key, &val])
            .map_err(|e| Event::UnexpectedError(format!("Unable to upsert: {}", e)))
    };
    let cnt: u64 = upsert_all(source, &mut upst)?;
    t.commit()
        .map_err(|e| Event::UnexpectedError(format!("Unable to commit changes: {}", e)))?;
    Ok(cnt)
}

fn pg_upsert<I>(source: I, c: &mut Client) -> Result<u64, Event>
where
    I: Iterator<Item = RawData>,
{
    let tx: Transaction = c
        .transaction()
        .map_err(|e| Event::UnexpectedError(format!("Unable to start transaction: {}", e)))?;
    pg_upsert_all(source, tx)
}

pub fn upsert() -> Result<(), Event> {
    let mut c: Client = Config::new()
        .host(env::var("PGHOST").unwrap().as_str())
        .dbname(env::var("PGDATABASE").unwrap().as_str())
        .user(env::var("PGUSER").unwrap().as_str())
        .password(env::var("PGPASSWORD").unwrap_or_default())
        .connect(NoTls)
        .map_err(|e| Event::ConnectError(format!("Unable to connect: {}", e)))?;

    let mut create = |tablename: &str| {
        c.execute(
            &format!(
                r#"
                    CREATE TABLE IF NOT EXISTS {}(
                        key BYTEA,
                        val BYTEA,
                        CONSTRAINT {}_pkc PRIMARY KEY(key)
                    )
                "#,
                tablename, tablename,
            ),
            &[],
        )
        .map_err(|e| Event::UnexpectedError(format!("Unable to create a bucket: {}", e)))
    };

    create("dates")?;
    create("devices")?;
    create("dates_cafef00ddeadbeafface864299792458")?;
    create("devices_2022_11_01")?;
    create("data_2022_11_01_cafef00ddeadbeafface864299792458")?;

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

    let cnt: u64 = pg_upsert(raws.into_iter(), &mut c)?;
    println!("upserted: {}", cnt);
    Ok(())
}
