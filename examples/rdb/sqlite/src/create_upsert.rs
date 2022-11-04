use rs_kv2spacetimedb::{
    bucket::Bucket,
    data::Data,
    date::Date,
    device::Device,
    evt::Event,
    item::{Item, RawItem},
    kvstore::upsert::create_upsert_all_shared,
};

use rusqlite::{params, Connection, Transaction};

fn bucket2create(b: &Bucket) -> Result<String, Event> {
    Ok(format!(
        r#"
            CREATE TABLE IF NOT EXISTS {} (
                key BLOB,
                val BLOB,
                CONSTRAINT {}_pkc PRIMARY KEY(key)
            )
        "#,
        b.as_str(),
        b.as_str(),
    ))
}

fn bucket2upsert(b: &Bucket) -> Result<String, Event> {
    Ok(format!(
        r#"
            INSERT INTO {}
            VALUES (?1, ?2)
            ON CONFLICT (key)
            DO UPDATE
            SET val=EXCLUDED.val
            WHERE {}.val <> EXCLUDED.val
        "#,
        b.as_str(),
        b.as_str(),
    ))
}

fn sqlite_create(t: &mut Transaction, b: &Bucket) -> Result<u64, Event> {
    let query: String = bucket2create(b)?;
    t.execute(query.as_str(), [])
        .map_err(|e| Event::UnexpectedError(format!("Unable to create a bucket: {}", e)))
        .map(|cnt| cnt as u64)
}

fn sqlite_upsert(t: &mut Transaction, b: &Bucket, i: &RawItem) -> Result<u64, Event> {
    let query: String = bucket2upsert(b)?;
    let key: &[u8] = i.as_key();
    let val: &[u8] = i.as_val();
    t.execute(query.as_str(), params![key, val])
        .map_err(|e| Event::UnexpectedError(format!("Unable to upsert: {}", e)))
        .map(|cnt| cnt as u64)
}

fn sqlite_commit(t: Transaction) -> Result<(), Event> {
    t.commit()
        .map_err(|e| Event::UnexpectedError(format!("Unable to commit changes: {}", e)))
}

pub fn create_upsert() -> Result<(), Event> {
    let mut c: Connection = Connection::open_in_memory()
        .map_err(|e| Event::ConnectError(format!("Unable to open db: {}", e)))?;
    let tx = c
        .transaction()
        .map_err(|e| Event::UnexpectedError(format!("Unable to start transaction: {}", e)))?;

    let source = vec![
        Data::new(
            Device::new_unchecked("cafef00ddeadbeafface864299792458".into()),
            Date::new_unchecked("2022_11_02".into()),
            Item::new(
                String::from("00:30:21.0Z").into_bytes(),
                String::from(
                    r#"
                        "timestamp": "2022-11-02T00:30:21.0Z",
                        "data": [
                        ]
                    "#,
                )
                .into_bytes(),
            ),
        ),
        Data::new(
            Device::new_unchecked("dafef00ddeadbeafface864299792458".into()),
            Date::new_unchecked("2022_11_02".into()),
            Item::new(
                String::from("00:30:21.0Z").into_bytes(),
                String::from(
                    r#"{
                        "timestamp": "2022-11-02T00:30:21.0Z",
                        "data": [
                        ]
                    }"#,
                )
                .into_bytes(),
            ),
        ),
    ];

    let cnt: u64 = create_upsert_all_shared(
        source.into_iter(),
        sqlite_create,
        sqlite_upsert,
        tx,
        sqlite_commit,
    )?;
    println!("create/upserted: {}", cnt);
    Ok(())
}
