use std::env;

use rs_kv2spacetimedb::item::RawItem;
use rs_kv2spacetimedb::{bucket::Bucket, date::Date, device::Device, evt::Event};

use rs_kv2spacetimedb::kvstore::get::get_raw_ignore_missing_bucket_new_func_shared;

use postgres::{Client, Config, NoTls, Row};

fn row2bytes(r: &Row) -> Result<Vec<u8>, Event> {
    r.try_get(0)
        .map_err(|e| Event::UnexpectedError(format!("Unable to get bytes from a row: {}", e)))
}

fn pg_select(c: &mut Client, b: &Bucket, key: &[u8]) -> Result<Option<Row>, Event> {
    let query: String = format!(
        r#"
            SELECT val FROM {}
            WHERE key=$1::BYTEA
            LIMIT 1
        "#,
        b.as_str(),
    );
    c.query_opt(query.as_str(), &[&key])
        .map_err(|e| Event::UnexpectedError(format!("Unable to try to get a row: {}", e)))
}

fn pg_bucket_exists(c: &mut Client, b: &Bucket) -> Result<bool, Event> {
    let bs: &str = b.as_str();
    let query: &str = r#"
        SELECT 1::INTEGER
        FROM information_schema.tables
        WHERE
            table_schema='public'
            AND table_name=$1::TEXT
        LIMIT 1
    "#;
    c.query_opt(query, &[&bs])
        .map_err(|e| Event::UnexpectedError(format!("Unable to check table count: {}", e)))
        .map(|o: Option<_>| o.map(|_: Row| true).unwrap_or(false))
}

fn pg_get_raw_ignore_missing_bucket_new_func_shared(
    c: Client,
) -> impl FnMut(&Device, &Date, &[u8]) -> Result<Option<RawItem>, Event> {
    let sel = |c: &mut Client, b: &Bucket, key: &[u8]| {
        let row: Option<Row> = pg_select(c, b, key)?;
        match row {
            Some(r) => row2bytes(&r).map(Some),
            None => Ok(None),
        }
    };

    get_raw_ignore_missing_bucket_new_func_shared(sel, pg_bucket_exists, c)
}

pub fn get_raw_ignore_missing_bucket() -> Result<(), Event> {
    let mut c: Client = Config::new()
        .host(env::var("PGHOST").unwrap().as_str())
        .dbname(env::var("PGDATABASE").unwrap().as_str())
        .user(env::var("PGUSER").unwrap().as_str())
        .password(env::var("PGPASSWORD").unwrap_or_default())
        .connect(NoTls)
        .map_err(|e| Event::ConnectError(format!("Unable to connect: {}", e)))?;

    c.execute(
        r#"
            CREATE TABLE IF NOT EXISTS data_2022_11_07_cafef00ddeadbeafface864299792458(
                key BYTEA,
                val BYTEA,
                CONSTRAINT data_2022_11_07_cafef00ddeadbeafface864299792458_pkc
                PRIMARY KEY (key)
            )
        "#,
        &[],
    )
    .map_err(|e| Event::UnexpectedError(format!("Unable to create a bucket: {}", e)))?;

    c.execute(
        r#"
            INSERT INTO data_2022_11_07_cafef00ddeadbeafface864299792458 AS tgt
            VALUES(
                '02:47:21.0Z'::BYTEA,
                '42'::BYTEA
            )
            ON CONFLICT ON CONSTRAINT data_2022_11_07_cafef00ddeadbeafface864299792458_pkc
            DO UPDATE
            SET val=EXCLUDED.val
            WHERE tgt.val <> EXCLUDED.val
        "#,
        &[],
    )
    .map_err(|e| Event::UnexpectedError(format!("Unable to insert a row: {}", e)))?;

    let dev1: Device = Device::from(0xcafef00ddeadbeafface864299792458);
    let dev2: Device = Device::from(0xffffffffffffffffffffffffffffffff);
    let date: Date = Date::new_unchecked("2022_11_07".into());
    let k1: &[u8] = b"02:47:21.0Z";
    let k2: &[u8] = b"02:48:35.0Z";

    let mut getter = pg_get_raw_ignore_missing_bucket_new_func_shared(c);

    let o1: Option<RawItem> = getter(&dev1, &date, k1)?;
    let o2: Option<RawItem> = getter(&dev1, &date, k2)?;
    let o3: Option<RawItem> = getter(&dev2, &date, k2)?;

    println!("raw item 1: {:#?}", o1);
    println!("raw item 2: {:#?}", o2);
    println!("raw item 3: {:#?}", o3);

    Ok(())
}
