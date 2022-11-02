use std::collections::BTreeSet;
use std::env;

use rs_kv2spacetimedb::{bucket::Bucket, evt::Event, kvstore::bucket};

use postgres::{Client, Config, NoTls, Row};

fn row2table_name(r: &Row) -> Result<String, Event> {
    r.try_get(0)
        .map_err(|e| Event::UnexpectedError(format!("Unable to get table name from row: {}", e)))
}

fn pg_list_bucket(c: &mut Client) -> Result<Vec<Row>, Event> {
    let query: &str = r#"
        SELECT table_name::TEXT
        FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY table_name
    "#;
    c.query(query, &[])
        .map_err(|e| Event::UnexpectedError(format!("Unable to get table names: {}", e)))
}

fn pg_list_bucket_new(mut c: Client) -> impl FnMut() -> Result<Vec<String>, Event> {
    move || {
        let rows: Vec<Row> = pg_list_bucket(&mut c)?;
        rows.into_iter().map(|r| row2table_name(&r)).collect()
    }
}

fn pg_show_list_bucket(c: Client) -> Result<(), Event> {
    let mut f = bucket::list_bucket(pg_list_bucket_new(c));
    let buckets: BTreeSet<Bucket> = f()?;
    for b in buckets {
        println!("bucket: {:#?}", b);
    }
    Ok(())
}

pub fn list_bucket() -> Result<(), Event> {
    let c: Client = Config::new()
        .host(env::var("PGHOST").unwrap().as_str())
        .dbname(env::var("PGDATABASE").unwrap().as_str())
        .user(env::var("PGUSER").unwrap().as_str())
        .password(env::var("PGPASSWORD").unwrap_or_default())
        .connect(NoTls)
        .map_err(|e| Event::ConnectError(format!("Unable to connect: {}", e)))?;

    pg_show_list_bucket(c)?;
    Ok(())
}
