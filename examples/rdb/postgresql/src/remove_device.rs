use std::env;

use rs_kv2spacetimedb::{bucket::Bucket, device::Device, evt::Event};

use rs_kv2spacetimedb::kvstore::delete::delete_device_default_func;

use postgres::{Client, Config, NoTls, Row, Transaction};

fn pg_row2bucket(r: Row) -> Result<Bucket, Event> {
    let s: String = r.try_get(0).map_err(|e| {
        Event::UnexpectedError(format!("Unable to get a bucket string from a row: {}", e))
    })?;
    Ok(Bucket::from(s))
}

fn pg_list_bucket_rows(t: &mut Transaction) -> Result<Vec<Row>, Event> {
    t.query(
        r#"
            SELECT table_name::TEXT
            FROM information_schema.tables
            WHERE table_schema='public'
            ORDER BY table_name
        "#,
        &[],
    )
    .map_err(|e| Event::UnexpectedError(format!("Unable to get list of buckets: {}", e)))
}

fn pg_list_buckets(t: &mut Transaction) -> Result<Vec<Bucket>, Event> {
    let rows: Vec<Row> = pg_list_bucket_rows(t)?;
    rows.into_iter().map(pg_row2bucket).collect()
}

fn pg_drop_bucket(t: &mut Transaction, b: &Bucket) -> Result<u64, Event> {
    let query: String = format!(
        r#"
            DROP TABLE IF EXISTS {}
        "#,
        b.as_str(),
    );
    t.execute(query.as_str(), &[])
        .map_err(|e| Event::UnexpectedError(format!("Unable to drop a bucket: {}", e)))
}

fn pg_delete_rows(t: &mut Transaction, b: &Bucket, key: &[u8]) -> Result<u64, Event> {
    let query: String = format!(
        r#"
            DELETE FROM {}
            WHERE key = $1::BYTEA
        "#,
        b.as_str(),
    );
    t.execute(query.as_str(), &[&key])
        .map_err(|e| Event::UnexpectedError(format!("Unable to delete rows: {}", e)))
}

fn pg_commit(t: Transaction) -> Result<(), Event> {
    t.commit()
        .map_err(|e| Event::UnexpectedError(format!("Unable to commit changes: {}", e)))
}

fn pg_remove_device(t: Transaction, target: Device) -> Result<u64, Event> {
    delete_device_default_func(
        pg_drop_bucket,
        pg_delete_rows,
        pg_list_buckets,
        t,
        pg_commit,
        target,
    )
}

pub fn remove_device() -> Result<(), Event> {
    let mut c: Client = Config::new()
        .host(env::var("PGHOST").unwrap().as_str())
        .dbname(env::var("PGDATABASE").unwrap().as_str())
        .user(env::var("PGUSER").unwrap().as_str())
        .password(env::var("PGPASSWORD").unwrap_or_default())
        .connect(NoTls)
        .map_err(|e| Event::ConnectError(format!("Unable to connect: {}", e)))?;

    let t = c
        .transaction()
        .map_err(|e| Event::UnexpectedError(format!("Unable to start transaction: {}", e)))?;

    let d: Device = Device::new_unchecked("cafef00ddeadbeafface864299792458".into());

    let cnt: u64 = pg_remove_device(t, d)?;

    println!("drop/delete device cnt: {}", cnt);

    Ok(())
}
