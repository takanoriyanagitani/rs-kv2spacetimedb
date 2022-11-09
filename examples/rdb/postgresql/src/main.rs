use rs_kv2spacetimedb::evt::Event;

mod count;
mod count_data;
mod create_cached;
mod create_upsert;
mod drop_by_date;
mod get;
mod get_devices4date;
mod list_bucket;
mod remove_device;
mod remove_stale_data;
mod upsert;

fn sub() -> Result<(), Event> {
    upsert::upsert()?;
    list_bucket::list_bucket()?;
    create_upsert::create_upsert()?;
    create_cached::create_cached()?;
    drop_by_date::remove_by_date()?;
    remove_stale_data::remove_stale_data()?;
    get::get_raw_ignore_missing_bucket()?;
    count::count()?;
    remove_device::remove_device()?;
    count_data::count_data()?;
    get_devices4date::get_devices4date()?;
    Ok(())
}

fn main() {
    match sub() {
        Ok(_) => {}
        Err(e) => eprintln!("{:#?}", e),
    }
}
