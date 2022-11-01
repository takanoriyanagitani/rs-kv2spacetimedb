pub struct Device {
    id: String, // cafef00d-dead-beaf-face-864299792458 => cafef00ddeadbeafface864299792458
}

impl Device {
    pub fn new_unchecked(id: String) -> Self {
        Self { id }
    }

    pub fn as_str(&self) -> &str {
        self.id.as_str()
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.as_str().as_bytes()
    }
}
