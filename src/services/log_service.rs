use std::{fs::OpenOptions, path::PathBuf};

use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;

pub struct Logger;

impl Logger {
    pub fn from_env() -> Self {
        dotenv::dotenv().ok();

        let path = std::env::var("LOG_PATH").expect(&format!(
            "Missing env var IMAGE_CACHE_URL required for Logger"
        ));

        let log_file_path = PathBuf::from(path).join("all.log");

        let log_file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&log_file_path)
            .expect(&format!("{:?}", log_file_path));

        let filter = EnvFilter::from_default_env()
            .add_directive(LevelFilter::WARN.into())
            .add_directive("platform::services::image_service=debug".parse().unwrap());

        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(log_file)
            .compact()
            .init();

        Self
    }
}