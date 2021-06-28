use config::{Config, ConfigError, File};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Aws {
    pub credentials_profile: String,
    pub queue_url: String,
    pub region: String,
}

#[derive(Debug, Deserialize)]
pub struct Producer {
    pub message_volume: usize,
}

#[derive(Debug, Deserialize)]
pub struct Consumer {
    pub worker_count: usize,
}

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub aws: Aws,
    pub producer: Producer,
    pub consumer: Consumer,
}

impl Settings {
    pub fn new() -> Result<Self, ConfigError> {
        let mut s = Config::default();
        s.merge(File::with_name("Settings"))?;
        s.try_into()
    }
}
