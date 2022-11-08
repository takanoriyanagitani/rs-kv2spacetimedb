use std::env;

use rs_kv2spacetimedb::{bucket::Bucket, datetime::DateTime, evt::Event};

use rs_kv2spacetimedb::count::Count;

use rs_kv2spacetimedb::kvstore::count::{
    counter_cached_new_default_std, counter_new_from_func, counter_new_func,
};

use postgres::{Client, Config, NoTls, Row};

fn counter_cached_func_new<S>(slow: S) -> impl FnMut(&Bucket) -> Result<Count, Event>
where
    S: FnMut(&Bucket) -> Result<Count, Event>,
{
    let counter = counter_new_from_func(slow);
    let cntr = counter_cached_new_default_std(counter);
    counter_new_func(cntr)
}

fn pg_count(c: &mut Client, b: &Bucket) -> Result<Row, Event> {
    let query: String = format!(
        r#"
            SELECT COUNT(*)::BIGINT FROM {}
        "#,
        b.as_str(),
    );
    c.query_one(query.as_str(), &[])
        .map_err(|e| Event::UnexpectedError(format!("Unable to get table count: {}", e)))
}

fn pg_count_new(mut c: Client) -> impl FnMut(&Bucket) -> Result<Row, Event> {
    move |b: &Bucket| pg_count(&mut c, b)
}

fn row2count_new<T>(time_source: T) -> impl Fn(Row) -> Result<Count, Event>
where
    T: Fn() -> Result<DateTime, Event>,
{
    move |r: Row| {
        let cnt: i64 = r.try_get(0).map_err(|e| {
            Event::UnexpectedError(format!("Unable to get count from a row: {}", e))
        })?;
        let c: u64 = cnt
            .try_into()
            .map_err(|e| Event::UnexpectedError(format!("Negative count: {}", e)))?;
        let d: DateTime = time_source()?;
        Ok(Count::new(c, d))
    }
}

fn pg2count_new<T>(c: Client, time_source: T) -> impl FnMut(&Bucket) -> Result<Count, Event>
where
    T: Fn() -> Result<DateTime, Event>,
{
    let mut counter = pg_count_new(c);
    let row2cnt = row2count_new(time_source);
    move |b: &Bucket| {
        let row: Row = counter(b)?;
        row2cnt(row)
    }
}

pub fn count() -> Result<(), Event> {
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
                ('2022_11_07'::BYTEA, ''::BYTEA),
                ('2022_11_08'::BYTEA, ''::BYTEA)
            ON CONFLICT ON CONSTRAINT dates_pkc
            DO NOTHING
        "#,
    )?;

    let time_source = DateTime::time_source_new_std();

    let counter = pg2count_new(c, time_source);
    let mut cached = counter_cached_func_new(counter);
    let b: Bucket = Bucket::from(String::from("dates"));
    let cnt: Count = cached(&b)?;
    println!("count: {:#?}", cnt);
    Ok(())
}
