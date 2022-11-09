use std::env;

use rs_kv2spacetimedb::{
    bucket::Bucket, compose::compose_err_mut, date::Date, device::Device, evt::Event,
};

use rs_kv2spacetimedb::count::Count;

use rs_kv2spacetimedb::kvstore::count::count_data_bucket4date_new_default_std;

use postgres::{Client, Config, NoTls, Row};

fn pg_row2u(r: Row) -> Result<u64, Event> {
    let i: i64 = r
        .try_get(0)
        .map_err(|e| Event::UnexpectedError(format!("Unable to get count: {}", e)))?;
    u64::try_from(i).map_err(|e| Event::UnexpectedError(format!("Count out of range: {}", e)))
}

fn pg_count2row(c: &mut Client, b: &Bucket) -> Result<Row, Event> {
    let query: String = format!(
        r#"
            SELECT COUNT(*)::BIGINT FROM {}
        "#,
        b.as_str(),
    );
    c.query_one(query.as_str(), &[])
        .map_err(|e| Event::UnexpectedError(format!("Unable to get table count: {}", e)))
}

fn pg_count2u(c: &mut Client, b: &Bucket) -> Result<u64, Event> {
    let bucket2row = |_: &Bucket| pg_count2row(c, b);
    let mut bucket2u = compose_err_mut(bucket2row, pg_row2u);
    bucket2u(b)
}

fn pg_count2u_new(mut c: Client) -> impl FnMut(&Bucket) -> Result<u64, Event> {
    move |b: &Bucket| pg_count2u(&mut c, b)
}

fn pg_count_new(c: Client) -> impl FnMut(&Device, &Date) -> Result<Count, Event> {
    count_data_bucket4date_new_default_std(pg_count2u_new(c))
}

pub fn count_data() -> Result<(), Event> {
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
            CREATE TABLE IF NOT EXISTS data_2022_11_09_cafef00ddeadbeafface864299792458(
                key BYTEA,
                val BYTEA,
                CONSTRAINT data_2022_11_09_cafef00ddeadbeafface864299792458_pkc
                PRIMARY KEY(key)
            )
        "#,
    )?;

    let mut counter = pg_count_new(c);

    let dev: Device = Device::new_unchecked("cafef00ddeadbeafface864299792458".into());
    let date: Date = Date::new_unchecked("2022_11_09".into());
    let cnt: Count = counter(&dev, &date)?;

    println!("count: {:#?}", cnt);

    Ok(())
}
