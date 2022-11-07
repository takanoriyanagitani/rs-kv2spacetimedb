use std::collections::BTreeMap;
use std::env;
use std::ops::{Deref, DerefMut};
use std::sync::RwLock;

use rs_kv2spacetimedb::{bucket::Bucket, datetime::DateTime, evt::Event};

use rs_kv2spacetimedb::count::{stale_checker_builder_new, Count};

use rs_kv2spacetimedb::kvstore::count::{count_cached_checked, count_keys_cached};

use postgres::{Client, Config, NoTls, Row};

fn stale_checker_new(duration_us: u64) -> impl Fn(&Count) -> Result<bool, Event> {
    stale_checker_builder_new(DateTime::time_source_new_std(), duration_us)
}

fn stale_checker_default() -> impl Fn(&Count) -> bool {
    let checker = stale_checker_new(1_000_000); // 1 second
    move |c: &Count| checker(c).unwrap_or(true)
}

fn update_cache(m: &mut BTreeMap<Bucket, Count>, b: &Bucket, c: &Count) -> Result<(), Event> {
    match m.get_mut(b) {
        Some(cnt) => {
            cnt.replace(c);
            Ok(())
        }
        None => {
            m.insert(b.clone(), *c);
            Ok(())
        }
    }
}

fn count_keys<S>(mut slow_get: S) -> impl FnMut(&Bucket) -> Result<Count, Event>
where
    S: FnMut(&Bucket) -> Result<Count, Event>,
{
    let mem: BTreeMap<Bucket, Count> = BTreeMap::new();
    let locked: RwLock<_> = RwLock::new(mem);
    let stale_checker = stale_checker_default();

    move |b: &Bucket| {
        let mut fast_reader = |b: &Bucket| {
            locked
                .read()
                .map_err(|e| {
                    Event::UnexpectedError(format!("Unable to read from fast cache: {}", e))
                })
                .and_then(|g| {
                    let m: &BTreeMap<_, _> = g.deref();
                    m.get(b)
                        .copied()
                        .ok_or_else(|| Event::UnexpectedError(String::from("Not found")))
                })
        };

        let mut fast_writer = |b: &Bucket, c: &Count| match locked.write() {
            Ok(mut g) => {
                let m: &mut BTreeMap<_, _> = g.deref_mut();
                update_cache(m, b, c)
            }
            Err(e) => Err(Event::UnexpectedError(format!(
                "Unable to write to fast cache: {}",
                e
            ))),
        };

        let mut fast_checked_reader =
            |b: &Bucket| count_cached_checked(&mut fast_reader, &stale_checker, b);

        count_keys_cached(&mut fast_checked_reader, &mut fast_writer, &mut slow_get, b)
    }
}

fn pg_row2count<T>(r: &Row, time_source: &T) -> Result<Count, Event>
where
    T: Fn() -> Result<DateTime, Event>,
{
    let cnt: i64 = r
        .try_get(0)
        .map_err(|e| Event::UnexpectedError(format!("Unable to get count from a row: {}", e)))?;
    let c: u64 = cnt
        .try_into()
        .map_err(|e| Event::UnexpectedError(format!("Negative count: {}", e)))?;
    let d: DateTime = time_source()?;
    Ok(Count::new(c, d))
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

fn pg_count_keys(mut c: Client) -> impl FnMut(&Bucket) -> Result<Count, Event> {
    let t1 = DateTime::time_source_new_std();

    let bucket2count =
        move |b: &Bucket| pg_count(&mut c, b).and_then(|r: Row| pg_row2count(&r, &t1));

    count_keys(bucket2count)
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

    let mut counter = pg_count_keys(c);
    let b: Bucket = Bucket::from(String::from("dates"));
    let cnt: Count = counter(&b)?;
    println!("count: {:#?}", cnt);
    Ok(())
}
