use std::{path::PathBuf, time::Duration};

use figment::{
    Figment,
    providers::{Format, Toml},
};
use lazy_static::lazy_static;
use opentelemetry::KeyValue;
use opentelemetry_sdk::Resource;
use serde::{Deserialize, Serialize};
use tracing::subscriber::with_default;
use url::Url;

use crate::services::log_service::stdout::STDOUT_LOGGER;

lazy_static! {
    static ref CONFIG: Config = Config::from_env().unwrap();

    // logging
    pub static ref LOG_FILE_ALL: PathBuf =
        CONFIG.log_directory.join("output.log");
    pub static ref LOG_TO_FILE: bool =
        CONFIG.log_to_file;
    pub static ref LOG_DIRECTORY: &'static PathBuf =
        &CONFIG.log_directory;
    pub static ref IMAGE_CACHE_DIRECTORY: &'static PathBuf =
        &CONFIG.image_cache_directory;

    // image cache
    pub static ref IMAGE_CACHE_GRACE_PERIOD_MS: u64 =
        CONFIG.image_cache.grace_period_ms;
    pub static ref IMAGE_CACHE_GRACE_DURATION: Duration =
        Duration::from_millis(CONFIG.image_cache.grace_period_ms);

    // otlp
    pub static ref OTLP_LOGS_ENDPOINT: &'static Option<Url> =
        &CONFIG.otlp.logs_endpoint;
    pub static ref OTLP_COLLECT_LOGS: bool =
        CONFIG.otlp.collect_logs;
    pub static ref OTLP_METRICS_ENDPOINT: &'static Option<Url> =
        &CONFIG.otlp.metrics_endpoint;
    pub static ref OTLP_COLLECT_METRICS: bool =
        CONFIG.otlp.collect_metrics;
    pub static ref OTLP_RESOURCE: Resource =
        Resource::from(&*CONFIG);

    // image transport
    pub static ref IMAGE_ENCODER_QUEUE_SIZE: usize =
        CONFIG.image_transport.encoder_queue_size;
    pub static ref IMAGE_ROUTE_HANDLER_QUEUE_SIZE: usize =
        CONFIG.image_transport.route_handler_queue_size;
    pub static ref IMAGE_CHUNK_SIZE: usize =
        CONFIG.image_transport.chunk_size;

    // image encoding
    pub static ref IMAGE_ENCODING_QUALITY: u8 =
        CONFIG.image_encoding.quality;
    pub static ref MAX_IMAGE_HEIGHT: u32 =
        CONFIG.image_encoding.max_height;
    pub static ref MAX_IMAGE_WIDTH: u32 =
        CONFIG.image_encoding.max_width;
}

#[derive(Deserialize, Serialize, Clone)]
struct ImageCache {
    grace_period_ms: u64,
}

#[derive(Deserialize, Serialize, Clone)]
struct ImageEncoding {
    quality: u8,
    max_height: u32,
    max_width: u32,
}

#[derive(Deserialize, Serialize, Clone)]
struct ImageTransport {
    encoder_queue_size: usize,
    route_handler_queue_size: usize,
    chunk_size: usize,
}

#[derive(Deserialize, Serialize, Clone)]
struct Package {
    pub name: String,
    pub version: String,
}

#[derive(Deserialize, Serialize, Clone)]
struct OTLP {
    pub instance_id: String,
    pub logs_endpoint: Option<Url>,
    pub collect_logs: bool,
    pub metrics_endpoint: Option<Url>,
    pub collect_metrics: bool,
}

#[derive(Deserialize, Serialize, Clone)]
struct Config {
    pub package: Package,
    pub otlp: OTLP,
    pub image_cache: ImageCache,
    pub image_transport: ImageTransport,
    pub image_encoding: ImageEncoding,
    pub environment: String,
    pub log_directory: PathBuf,
    pub image_cache_directory: PathBuf,
    pub log_to_file: bool,
}

impl Config {
    fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        with_default(STDOUT_LOGGER.clone(), || {
            let config_path = std::env::var("PLATFORM_CONFIG_PATH")
                .map(|p| {
                    let path = PathBuf::from(&p);
                    tracing::info!(path = %path.display(), "Using config path from environment");
                    path
                })
                .unwrap_or_else(|_| {
                    let path = PathBuf::from("./Config.toml");
                    tracing::info!(path = %path.display(), "Using default config path");
                    path
                });

            if !config_path.exists() {
                tracing::error!(
                    path = %config_path.display(),
                    "Configuration file not found"
                );

                return Err(
                    format!("Configuration file not found at path {:?}", config_path).into(),
                );
            }

            let config = Figment::new()
                .merge(Toml::file(&config_path).nested())
                .extract()?;

            tracing::info!(
                path = %config_path.display(),
                "Configuration loaded successfully"
            );

            Ok(config)
        })
    }
}

impl From<&Config> for Resource {
    fn from(value: &Config) -> Self {
        Resource::builder()
            .with_attributes([
                KeyValue::new("service.name", value.package.name.clone()),
                KeyValue::new("service.version", value.package.version.clone()),
                KeyValue::new("service.instance.id", value.otlp.instance_id.clone()),
                KeyValue::new("environment", value.environment.clone()),
            ])
            .build()
    }
}

impl From<Config> for Resource {
    fn from(value: Config) -> Self {
        Resource::from(&value)
    }
}
