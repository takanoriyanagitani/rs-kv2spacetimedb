use crate::{bucket::Bucket, date::Date, day::Day, evt::Event, month::Month, year::Year};

fn is_drop_target_str(bs: &str, ds: &str) -> Result<bool, Event> {
    let pat = format!("data_{}", ds);
    Ok(bs.starts_with(&pat))
}

/// Checks if the bucket must be dropped.
///
/// The date info in the bucket will be compared and the bucket will be dropped on match.
///
/// # Arguments
/// - b: The bucket to be checked.
/// - d: The date to be compared.
pub fn is_drop_target(b: &Bucket, d: &Date) -> bool {
    let bs: &str = b.as_str();
    let ds: &str = d.as_str();

    format!("devices_{}", ds)
        .eq(bs)
        .then_some(true)
        .unwrap_or_else(|| is_drop_target_str(bs, ds).unwrap_or(false))
}

fn is_delete_target_str(bs: &str) -> Result<bool, Event> {
    Ok(bs.starts_with("dates_"))
}

/// Checks if the bucket can have rows to be deleted.
pub fn is_delete_target(b: &Bucket) -> bool {
    let bs: &str = b.as_str();

    "dates"
        .eq(bs)
        .then_some(true)
        .unwrap_or_else(|| is_delete_target_str(bs).unwrap_or(false))
}

fn is_target_stale_compare_date(
    year: &str,
    month: &str,
    day: &str,
    lbi: &Date,
) -> Result<bool, Event> {
    let yu: u16 =
        str::parse(year).map_err(|e| Event::UnexpectedError(format!("Invalid year: {}", e)))?;
    let mu: u8 =
        str::parse(month).map_err(|e| Event::UnexpectedError(format!("Invalid month: {}", e)))?;
    let du: u8 =
        str::parse(day).map_err(|e| Event::UnexpectedError(format!("Invalid day: {}", e)))?;
    let y: Year = Year::try_from(yu)?;
    let m: Month = Month::try_from(mu)?;
    let d: Day = Day::try_from(du)?;
    let dt: Date = Date::new(y, m, d);
    let fresh: bool = lbi.le(&dt);
    Ok(!fresh)
}

// data_2022_11_07_cafef00ddeadbeafface864299792458
fn is_drop_target_stale_data_bucket(bs: &str, lbi: &Date) -> Result<bool, Event> {
    let mut splited = bs.splitn(5, '_');
    splited.next(); // data
    let ys: &str = splited
        .next()
        .ok_or_else(|| Event::UnexpectedError(String::from("Year unknown")))?;
    let ms: &str = splited
        .next()
        .ok_or_else(|| Event::UnexpectedError(String::from("Month unknown")))?;
    let ds: &str = splited
        .next()
        .ok_or_else(|| Event::UnexpectedError(String::from("Day unknown")))?;
    is_target_stale_compare_date(ys, ms, ds, lbi)
}

// devices_2022_11_07
fn is_drop_target_stale_devices_master(bs: &str, lbi: &Date) -> Result<bool, Event> {
    let mut splited = bs.splitn(4, '_');
    splited.next(); // devices
    let ys: &str = splited
        .next()
        .ok_or_else(|| Event::UnexpectedError(String::from("Year unknown")))?;
    let ms: &str = splited
        .next()
        .ok_or_else(|| Event::UnexpectedError(String::from("Month unknown")))?;
    let ds: &str = splited
        .next()
        .ok_or_else(|| Event::UnexpectedError(String::from("Day unknown")))?;
    is_target_stale_compare_date(ys, ms, ds, lbi)
}

/// Checks if the bucket must be dropped.
///
/// # Arguments
/// - b: The bucket to be checked.
/// - lbe: Lower bound(inclusive) which must "not" be dropped.
pub fn is_drop_target_stale(b: &Bucket, lbi: &Date) -> bool {
    let bs: &str = b.as_str();
    let is_data_bucket: bool = bs.starts_with("data_");
    is_data_bucket
        .then(|| is_drop_target_stale_data_bucket(bs, lbi))
        .unwrap_or_else(|| is_drop_target_stale_devices_master(bs, lbi))
        .unwrap_or(false)
}

#[cfg(test)]
mod test_remove {

    mod is_drop_target_stale {
        use crate::remove;
        use crate::{bucket::Bucket, date::Date};

        #[test]
        fn test_invalid() {
            let b: Bucket = Bucket::from(String::from("date_2022/11/07"));
            let d: Date = Date::new_unchecked("2022_11_07".into());
            let b: bool = remove::is_drop_target_stale(&b, &d);
            assert_eq!(b, false);
        }

        #[test]
        fn test_stale_devices_master() {
            let b: Bucket = Bucket::from(String::from("devices_2022_11_06"));
            let d: Date = Date::new_unchecked("2022_11_07".into());
            let b: bool = remove::is_drop_target_stale(&b, &d);
            assert_eq!(b, true);
        }

        #[test]
        fn test_stale_data_bucket() {
            let b: Bucket = Bucket::from(String::from(
                "data_2019_05_01_cafef00ddeadbeafface864299792458",
            ));
            let d: Date = Date::new_unchecked("2022_11_07".into());
            let b: bool = remove::is_drop_target_stale(&b, &d);
            assert_eq!(b, true);
        }

        #[test]
        fn test_same_date() {
            let b: Bucket = Bucket::from(String::from(
                "data_2019_05_01_cafef00ddeadbeafface864299792458",
            ));
            let d: Date = Date::new_unchecked("2019_05_01".into());
            let b: bool = remove::is_drop_target_stale(&b, &d);
            assert_eq!(b, false);
        }

        #[test]
        fn test_fresh_date() {
            let b: Bucket = Bucket::from(String::from(
                "data_2022_11_07_cafef00ddeadbeafface864299792458",
            ));
            let d: Date = Date::new_unchecked("2019_05_01".into());
            let b: bool = remove::is_drop_target_stale(&b, &d);
            assert_eq!(b, false);
        }
    }
}
