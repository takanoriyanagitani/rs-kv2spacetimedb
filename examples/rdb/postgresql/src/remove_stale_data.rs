use std::env;

use rs_kv2spacetimedb::{bucket::Bucket, date::Date, evt::Event};

use rs_kv2spacetimedb::kvstore::delete::remove_stale_data_default_shared;

use postgres::{Client, Config, NoTls, Row, Transaction};

fn pg_row2bucket(r: &Row) -> Result<Bucket, Event> {
    let s: String = r.try_get(0).map_err(|e| {
        Event::UnexpectedError(format!("Unable to get a bucket string from a row: {}", e))
    })?;
    Ok(Bucket::from(s))
}

fn pg_list_buckets(t: &mut Transaction) -> Result<Vec<Row>, Event> {
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

fn pg_delete_stale_rows(t: &mut Transaction, b: &Bucket, key_lbi: &[u8]) -> Result<u64, Event> {
    let query: String = format!(
        r#"
            DELETE FROM {}
            WHERE key < $1::BYTEA
        "#,
        b.as_str(),
    );
    t.execute(query.as_str(), &[&key_lbi])
        .map_err(|e| Event::UnexpectedError(format!("Unable to delete rows: {}", e)))
}

fn pg_remove_stale_data(lbi: &Date, t: Transaction) -> Result<u64, Event> {
    let sel = |tx: &mut Transaction| {
        let rows: Vec<Row> = pg_list_buckets(tx)?;
        let buckets = rows.into_iter().map(|row: Row| pg_row2bucket(&row));
        buckets.collect()
    };
    let finalize = |tx: Transaction| {
        tx.commit()
            .map_err(|e| Event::UnexpectedError(format!("Unable to commit changes: {}", e)))
    };
    remove_stale_data_default_shared(
        &sel,
        &pg_drop_bucket,
        &pg_delete_stale_rows,
        lbi,
        t,
        &finalize,
    )
}

pub fn remove_stale_data() -> Result<(), Event> {
    let mut c: Client = Config::new()
        .host(env::var("PGHOST").unwrap().as_str())
        .dbname(env::var("PGDATABASE").unwrap().as_str())
        .user(env::var("PGUSER").unwrap().as_str())
        .password(env::var("PGPASSWORD").unwrap_or_default())
        .connect(NoTls)
        .map_err(|e| Event::ConnectError(format!("Unable to connect: {}", e)))?;

    let lbi: Date = Date::new_unchecked("2022_11_07".into());

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
                val BYTEA,
                CONSTRAINT dates_pkc PRIMARY KEY(key)
            )
        "#,
    )?;

    exec(
        r#"
            INSERT INTO dates
            VALUES
                ('2019_05_01'::BYTEA, ''::BYTEA),
                ('2022_11_04'::BYTEA, ''::BYTEA),
                ('2022_11_05'::BYTEA, ''::BYTEA),
                ('2022_11_06'::BYTEA, ''::BYTEA),
                ('2022_11_07'::BYTEA, ''::BYTEA)
            ON CONFLICT ON CONSTRAINT dates_pkc
            DO UPDATE
            SET val = EXCLUDED.val
            WHERE dates.val <> EXCLUDED.val
        "#,
    )?;

    let tx: Transaction = c
        .transaction()
        .map_err(|e| Event::UnexpectedError(format!("Unable to start transaction: {}", e)))?;

    let cnt: u64 = pg_remove_stale_data(&lbi, tx)?;
    println!("removed stale data count: {}", cnt);
    Ok(())
}
