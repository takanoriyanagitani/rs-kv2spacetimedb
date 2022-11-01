pub struct Date {
    date: String, // 2022/11/01 => 2022_11_01
}

impl Date {
    pub fn new_unchecked(date: String) -> Self {
        Self { date }
    }

    pub fn as_str(&self) -> &str {
        self.date.as_str()
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.as_str().as_bytes()
    }
}
