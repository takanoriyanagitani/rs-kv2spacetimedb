use rs_kv2spacetimedb::evt::Event;

mod list_bucket;
mod upsert;

fn sub() -> Result<(), Event> {
    upsert::upsert()?;
    list_bucket::list_bucket()?;
    Ok(())
}

fn main() {
    match sub() {
        Ok(_) => {}
        Err(e) => eprintln!("{:#?}", e),
    }
}
