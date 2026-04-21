use std::path::PathBuf;

use figment::{Figment, providers::{Format, Toml}};
use opentelemetry::KeyValue;
use opentelemetry_sdk::Resource;
use serde::{Deserialize, Serialize};
use tracing::subscriber::with_default;
use url::Url;

use crate::services::log_service::STDOUT_LOGGER;

#[derive(Deserialize, Serialize)]
pub struct Package {
    pub name: String,
    pub version: String,
}

#[derive(Deserialize, Serialize)]
pub struct OTLP {
    pub instance_id: String,
    pub collector_endpoint: Option<Url>,
    pub collect_logs: bool,
    pub collect_metrics: bool,
}

#[derive(Deserialize, Serialize)]
pub struct Config {
    pub package: Package,
    pub otlp: OTLP,
    pub environment: String,
    pub log_directory: PathBuf,
    pub image_cache_directory: PathBuf,
}

impl Config {
    pub fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        with_default(STDOUT_LOGGER.clone(), || {
            let config_path = std::env::var("PLATFORM_CONFIG_PATH")
                .map(|p| {
                    let path = PathBuf::from(&p);
                    tracing::info!(path = %path.display(), "Using config path from environment");
                    path
                })
                .unwrap_or_else(|_| {
                    let path = PathBuf::from("./config.toml");
                    tracing::info!(path = %path.display(), "Using default config path");
                    path
                });
    
            if !config_path.exists() {
                tracing::error!(
                    path = %config_path.display(),
                    "Configuration file not found"
                );
    
                return Err(format!(
                    "Configuration file not found at path {:?}",
                    config_path
                )
                .into());
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