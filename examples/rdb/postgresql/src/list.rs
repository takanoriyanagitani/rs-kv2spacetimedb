use std::env;

use rs_kv2spacetimedb::{bucket::Bucket, date::Date, device::Device, evt::Event};

use rs_kv2spacetimedb::kvstore::list::list_keys4data_ignore_missing_bucket_new;

use postgres::{Client, Config, NoTls, Row};

fn pg_row2key(r: Row) -> Result<Vec<u8>, Event> {
    r.try_get(0)
        .map_err(|e| Event::UnexpectedError(format!("Unable to get a value from a row: {}", e)))
}

fn pg_select_all2rows(c: &mut Client, b: &Bucket) -> Result<Vec<Row>, Event> {
    let query: String = format!(
        r#"
            SELECT key FROM {}
            ORDER BY key
        "#,
        b.as_str(),
    );
    c.query(query.as_str(), &[])
        .map_err(|e| Event::UnexpectedError(format!("Unable to try to get a row: {}", e)))
}

fn pg_select_all(c: &mut Client, b: &Bucket) -> Result<Vec<Vec<u8>>, Event> {
    let rows: Vec<Row> = pg_select_all2rows(c, b)?;
    rows.into_iter().map(pg_row2key).collect()
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

fn pg_list_new(c: Client) -> impl FnMut(&Date, &Device) -> Result<Vec<Vec<u8>>, Event> {
    list_keys4data_ignore_missing_bucket_new(pg_select_all, pg_bucket_exists, c)
}

pub fn list() -> Result<(), Event> {
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
            CREATE TABLE IF NOT EXISTS data_2022_11_09_aafef00ddeadbeafface864299792458 (
                key BYTEA,
                val BYTEA,
                CONSTRAINT data_2022_11_09_aafef00ddeadbeafface864299792458_pkc PRIMARY KEY(key)
            )
        "#,
    )?;

    exec(
        r#"
            INSERT INTO data_2022_11_09_aafef00ddeadbeafface864299792458
            VALUES
                ('05:23:22.0Z'::BYTEA, ''::BYTEA),
                ('06:23:22.0Z'::BYTEA, ''::BYTEA)
            ON CONFLICT ON CONSTRAINT data_2022_11_09_aafef00ddeadbeafface864299792458_pkc
            DO NOTHING
        "#,
    )?;

    let mut lst = pg_list_new(c);

    let d: Date = Date::new_unchecked("2022_11_09".into());
    let dev: Device = Device::new_unchecked("aafef00ddeadbeafface864299792458".into());

    let values: Vec<_> = lst(&d, &dev)?;

    println!("values: {:#?}", values);

    Ok(())
}
