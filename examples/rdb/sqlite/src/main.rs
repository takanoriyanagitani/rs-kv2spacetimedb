use rs_kv2spacetimedb::kvstore::upsert::upsert_all;
use rs_kv2spacetimedb::{
    bucket::Bucket,
    data::{Data, RawData},
    date::Date,
    device::Device,
    evt::Event,
    item::{Item, RawItem},
};

use rusqlite::{params, Connection, Transaction};

fn sqlite3_upsert_new() -> impl Fn(&Transaction, &Bucket, &RawItem) -> Result<u64, Event> {
    move |t: &Transaction, b: &Bucket, i: &RawItem| {
        let key: &[u8] = i.as_key();
        let val: &[u8] = i.as_val();
        let query: String = format!(
            r#"
                INSERT INTO {}
                VALUES (?1, ?2)
                ON CONFLICT (key)
                DO UPDATE
                SET val = EXCLUDED.val
                WHERE {}.val <> EXCLUDED.val
            "#,
            b.as_str(),
            b.as_str(),
        );
        t.execute(query.as_str(), params![key, val])
            .map_err(|e| Event::UnexpectedError(format!("Unable to upsert: {}", e)))
            .map(|cnt| cnt as u64)
    }
}

fn sqlite3_upsert_all<I>(source: I, tx: Transaction) -> Result<u64, Event>
where
    I: Iterator<Item = RawData>,
{
    let f = sqlite3_upsert_new();
    let mut g = |b: &Bucket, i: &RawItem| f(&tx, b, i);
    let cnt: u64 = upsert_all(source, &mut g)?;
    tx.commit()
        .map_err(|e| Event::UnexpectedError(format!("Unable to commit changes: {}", e)))?;
    Ok(cnt)
}

fn upsert() -> Result<(), Event> {
    let mut c: Connection = Connection::open_in_memory()
        .map_err(|e| Event::ConnectError(format!("Unable to open db: {}", e)))?;
    let tx = c
        .transaction()
        .map_err(|e| Event::UnexpectedError(format!("Unable to start transaction: {}", e)))?;

    let create = |name: &str| {
        let query: String = format!(
            r#"
                CREATE TABLE IF NOT EXISTS {} (
                    key BLOB,
                    val BLOB,
                    CONSTRAINT {}_pkc PRIMARY KEY(key)
                )
            "#,
            name, name,
        );
        tx.execute(query.as_str(), [])
            .map_err(|e| Event::UnexpectedError(format!("Unable to create a bucket: {}", e)))
    };

    create("data_2022_11_02_cafef00ddeadbeafface864299792458")?;
    create("data_2022_11_02_dafef00ddeadbeafface864299792458")?;
    create("dates_cafef00ddeadbeafface864299792458")?;
    create("dates_dafef00ddeadbeafface864299792458")?;
    create("devices_2022_11_02")?;
    create("devices")?;
    create("dates")?;

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
    let cnt: u64 = sqlite3_upsert_all(source.into_iter(), tx)?;
    println!("upserted: {}", cnt);
    Ok(())
}

fn sub() -> Result<(), Event> {
    upsert()?;
    Ok(())
}

fn main() {
    match sub() {
        Ok(_) => {}
        Err(e) => eprintln!("{:#?}", e),
    }
}
