use rs_kv2spacetimedb::evt::Event;

mod create_upsert;
mod list_bucket;
mod upsert;

fn sub() -> Result<(), Event> {
    upsert::upsert()?;
    list_bucket::list_bucket()?;
    create_upsert::create_upsert()?;
    Ok(())
}

fn main() {
    match sub() {
        Ok(_) => {}
        Err(e) => eprintln!("{:#?}", e),
    }
}
