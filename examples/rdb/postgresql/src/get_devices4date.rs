use std::env;

use rs_kv2spacetimedb::item::{Item, RawItem};
use rs_kv2spacetimedb::{bucket::Bucket, date::Date, evt::Event};

use rs_kv2spacetimedb::kvstore::get::get_devices4date_ignore_missing_bucket_new;

use postgres::{Client, Config, NoTls, Row};

fn pg_row2item(r: Row) -> Result<RawItem, Event> {
    let key: Vec<u8> = r
        .try_get(0)
        .map_err(|e| Event::UnexpectedError(format!("Unable to get a key from a row: {}", e)))?;
    let val: Vec<u8> = r
        .try_get(1)
        .map_err(|e| Event::UnexpectedError(format!("Unable to get a key from a row: {}", e)))?;
    Ok(Item::new(key, val))
}

fn pg_select_all2rows(c: &mut Client, b: &Bucket) -> Result<Vec<Row>, Event> {
    let query: String = format!(
        r#"
            SELECT key, val FROM {}
            ORDER BY key
        "#,
        b.as_str(),
    );
    c.query(query.as_str(), &[])
        .map_err(|e| Event::UnexpectedError(format!("Unable to try to get a row: {}", e)))
}

fn pg_select_all(c: &mut Client, b: &Bucket) -> Result<Vec<RawItem>, Event> {
    let rows: Vec<Row> = pg_select_all2rows(c, b)?;
    rows.into_iter().map(pg_row2item).collect()
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

fn pg_get_devices4date_iore_missing_bucket_new(
    c: Client,
) -> impl FnMut(&Date) -> Result<Vec<RawItem>, Event> {
    get_devices4date_ignore_missing_bucket_new(pg_select_all, pg_bucket_exists, c)
}

pub fn get_devices4date() -> Result<(), Event> {
    let mut c: Client = Config::new()
        .host(env::var("PGHOST").unwrap().as_str())
        .dbname(env::var("PGDATABASE").unwrap().as_str())
        .user(env::var("PGUSER").unwrap().as_str())
        .password(env::var("PGPASSWORD").unwrap_or_default())
        .connect(NoTls)
        .map_err(|e| Event::ConnectError(format!("Unable to connect: {}", e)))?;

    let mut exec = |query: &str| {
        c.execute(query, &[])
            .map_err(|e| Event::UnexpectedError(format!("Unable to exec: {}", e)))
    };

    exec(
        r#"
            CREATE TABLE IF NOT EXISTS devices_2022_11_09 (
                key BYTEA,
                val BYTEA,
                CONSTRAINT devices_2022_11_09_pkc PRIMARY KEY(key)
            )
        "#,
    )?;

    exec(
        r#"
            INSERT INTO devices_2022_11_09
            VALUES
                ('bafef00ddeadbeafface864299792458'::BYTEA, ''::BYTEA),
                ('cafef00ddeadbeafface864299792458'::BYTEA, ''::BYTEA)
            ON CONFLICT ON CONSTRAINT devices_2022_11_09_pkc
            DO NOTHING
        "#,
    )?;

    let mut getter = pg_get_devices4date_iore_missing_bucket_new(c);

    let d: Date = Date::new_unchecked("2022_11_09".into());
    //let d: Date = Date::new_unchecked("9999_12_31".into());

    let items: Vec<_> = getter(&d)?;

    println!("items: {:#?}", items);

    Ok(())
}
