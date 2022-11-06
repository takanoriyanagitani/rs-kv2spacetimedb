use std::env;
use std::ops::DerefMut;
use std::sync::Mutex;

use rs_kv2spacetimedb::{bucket::Bucket, date::Date, evt::Event};

use rs_kv2spacetimedb::kvstore::delete::remove_by_date_default;

use postgres::{Client, Config, NoTls, Row, Transaction};

fn pg_get_tables<L>(list: &mut L) -> Result<Vec<Bucket>, Event>
where
    L: FnMut() -> Result<Vec<Row>, Event>,
{
    let rows: Vec<Row> = list()?;
    let row2str = |r: &Row| {
        let name: String = r.try_get(0).map_err(|e| {
            Event::UnexpectedError(format!("Unable to get a table name from a row: {}", e))
        })?;
        Ok(Bucket::from(name))
    };
    let mapd = rows.into_iter().map(|r: Row| row2str(&r));
    mapd.collect()
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

fn pg_delete_row(t: &mut Transaction, b: &Bucket, date: &[u8]) -> Result<u64, Event> {
    let query: String = format!(
        r#"
            DELETE FROM {}
            WHERE key = $1::BYTEA
        "#,
        b.as_str(),
    );
    t.execute(query.as_str(), &[&date])
        .map_err(|e| Event::UnexpectedError(format!("Unable to drop a bucket: {}", e)))
}

fn pg_remove_by_date(tx: Transaction, d: &Date) -> Result<u64, Event> {
    let mt: Mutex<Transaction> = Mutex::new(tx);
    let rmd = || {
        let mut drp = |b: &Bucket| {
            let mut mg = mt.lock().map_err(|e| {
                Event::UnexpectedError(format!("Unable to lock transaction: {}", e))
            })?;
            let t: &mut Transaction = mg.deref_mut();
            pg_drop_bucket(t, b)
        };
        let mut del = |b: &Bucket, date: &[u8]| {
            let mut mg = mt.lock().map_err(|e| {
                Event::UnexpectedError(format!("Unable to lock transaction: {}", e))
            })?;
            let t: &mut Transaction = mg.deref_mut();
            pg_delete_row(t, b, date)
        };
        let mut sel = || {
            let mut mg = mt.lock().map_err(|e| {
                Event::UnexpectedError(format!("Unable to lock transaction: {}", e))
            })?;
            let t: &mut Transaction = mg.deref_mut();
            let mut sel = || {
                t.query(
                    r#"
                        SELECT table_name::TEXT
                        FROM information_schema.tables
                        WHERE table_schema='public'
                        ORDER BY table_name
                    "#,
                    &[],
                )
                .map_err(|e| Event::UnexpectedError(format!("Unable to get bucket names: {}", e)))
            };
            pg_get_tables(&mut sel)
        };
        remove_by_date_default(&mut sel, &mut drp, &mut del, d)
    };
    let cnt: u64 = rmd()?;
    let tx: Transaction = mt
        .into_inner()
        .map_err(|e| Event::UnexpectedError(format!("Unable to get transaction: {}", e)))?;
    tx.commit()
        .map_err(|e| Event::UnexpectedError(format!("Unable to commit changes: {}", e)))?;
    Ok(cnt)
}

pub fn remove_by_date() -> Result<(), Event> {
    let mut c: Client = Config::new()
        .host(env::var("PGHOST").unwrap().as_str())
        .dbname(env::var("PGDATABASE").unwrap().as_str())
        .user(env::var("PGUSER").unwrap().as_str())
        .password(env::var("PGPASSWORD").unwrap_or_default())
        .connect(NoTls)
        .map_err(|e| Event::ConnectError(format!("Unable to connect: {}", e)))?;

    let d: Date = Date::new_unchecked("2022_11_06".into());

    let mut exec = |q: &str| {
        c.execute(q, &[])
            .map_err(|e| Event::UnexpectedError(format!("Unable to create a bucket: {}", e)))
    };

    exec(
        r#"
            CREATE TABLE IF NOT EXISTS devices_2022_11_06(
                key BYTEA,
                val BYTEA
            )
        "#,
    )?;

    exec(
        r#"
            CREATE TABLE IF NOT EXISTS dates(
                key BYTEA,
                val BYTEA
            )
        "#,
    )?;

    exec(
        r#"
            INSERT INTO dates VALUES(
                '2022_11_06'::BYTEA,
                ''::BYTEA
            )
        "#,
    )?;

    exec(
        r#"
            CREATE TABLE IF NOT EXISTS dates_cafef00ddeadbeafface864299792458(
                key BYTEA,
                val BYTEA
            )
        "#,
    )?;

    exec(
        r#"
            CREATE TABLE IF NOT EXISTS dates_dafef00ddeadbeafface864299792458(
                key BYTEA,
                val BYTEA
            )
        "#,
    )?;

    exec(
        r#"
            INSERT INTO dates_cafef00ddeadbeafface864299792458 VALUES(
                '2022_11_06'::BYTEA,
                ''::BYTEA
            )
        "#,
    )?;

    exec(
        r#"
            INSERT INTO dates_dafef00ddeadbeafface864299792458 VALUES(
                '2022_11_06'::BYTEA,
                ''::BYTEA
            )
        "#,
    )?;

    exec(
        r#"
            CREATE TABLE IF NOT EXISTS data_2022_11_06_cafef00ddeadbeafface864299792458(
                key BYTEA,
                val BYTEA
            )
        "#,
    )?;

    let tx: Transaction = c
        .transaction()
        .map_err(|e| Event::UnexpectedError(format!("Unable to start transaction: {}", e)))?;

    let cnt: u64 = pg_remove_by_date(tx, &d)?;
    println!("drop/removed: {}", cnt);
    Ok(())
}
