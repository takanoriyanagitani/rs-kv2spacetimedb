use std::env;

use rs_kv2spacetimedb::{bucket::Bucket, date::Date, evt::Event};

use rs_kv2spacetimedb::kvstore::delete::delete_stale_data_default_func;

use postgres::{Client, Config, NoTls, Row, Transaction};

fn pg_row2string(r: Row) -> Result<String, Event> {
    r.try_get(0).map_err(|e| {
        Event::UnexpectedError(format!("Unable to get a bucket name from a row: {}", e))
    })
}

fn pg_list_buckets(t: &mut Transaction) -> Result<Vec<Bucket>, Event> {
    let rows: Vec<Row> = t
        .query(
            r#"
            SELECT table_name::TEXT
            FROM information_schema.tables
            WHERE table_schema='public'
            ORDER BY table_name
        "#,
            &[],
        )
        .map_err(|e| Event::UnexpectedError(format!("Unable to get list of buckets: {}", e)))?;
    rows.into_iter()
        .map(pg_row2string)
        .map(|r| r.map(Bucket::from))
        .collect()
}

fn pg_commit(t: Transaction) -> Result<(), Event> {
    t.commit()
        .map_err(|e| Event::UnexpectedError(format!("Unable to commit changes: {}", e)))
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

fn pg_delete_by_date(t: Transaction, lbi: Date) -> Result<u64, Event> {
    delete_stale_data_default_func(
        pg_drop_bucket,
        pg_delete_row,
        pg_list_buckets,
        t,
        pg_commit,
        lbi,
    )
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

    let cnt: u64 = pg_delete_by_date(tx, d)?;
    println!("drop/removed: {}", cnt);
    Ok(())
}
