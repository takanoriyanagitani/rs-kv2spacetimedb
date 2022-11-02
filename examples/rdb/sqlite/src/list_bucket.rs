use std::collections::BTreeSet;

use rs_kv2spacetimedb::{bucket::Bucket, evt::Event, kvstore::bucket};

use rusqlite::{Connection, Row, Statement};

fn sqlite_list_bucket(s: &mut Statement) -> Result<Vec<String>, Event> {
    let rows = s
        .query_map([], |r: &Row| {
            let s: String = r.get(0)?;
            Ok(s)
        })
        .map_err(|e| Event::UnexpectedError(format!("Unable to get list of table names: {}", e)))?;
    rows.map(|r| r.map_err(|e| Event::UnexpectedError(format!("Unable to get a row: {}", e))))
        .collect()
}

fn sqlite_get_buckets(c: &Connection) -> Result<BTreeSet<Bucket>, Event> {
    let mut s = c
        .prepare(
            r#"
                SELECT name FROM sqlite_master
                WHERE type='table'
                ORDER BY name
            "#,
        )
        .map_err(|e| Event::UnexpectedError(format!("Unable to prepare: {}", e)))?;
    let list_f = || sqlite_list_bucket(&mut s);
    let mut f = bucket::list_bucket(list_f);
    f()
}

pub fn list_bucket() -> Result<(), Event> {
    let mut c: Connection = Connection::open_in_memory()
        .map_err(|e| Event::ConnectError(format!("Unable to open db: {}", e)))?;

    let t = c
        .transaction()
        .map_err(|e| Event::UnexpectedError(format!("Unable to start transaction: {}", e)))?;
    t.execute(
        r#"
            CREATE TABLE IF NOT EXISTS t(
                col BLOB PRIMARY KEY
            )
        "#,
        [],
    )
    .map_err(|e| Event::UnexpectedError(format!("Unable to create dummy table: {}", e)))?;
    t.commit()
        .map_err(|e| Event::UnexpectedError(format!("Unable to commit changes: {}", e)))?;

    let buckets: BTreeSet<_> = sqlite_get_buckets(&c)?;
    for b in buckets {
        println!("bucket: {:#?}", b);
    }
    Ok(())
}
