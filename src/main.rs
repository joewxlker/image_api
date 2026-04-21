use std::fs::OpenOptions;
use std::path::PathBuf;

use figment::Figment;
use figment::providers::{Format, Toml};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge as OtelBridge;
use opentelemetry_sdk::logs::{BatchConfig, BatchLogProcessor, SdkLoggerProvider};
use serde::{Deserialize, Serialize};
use tokio::select;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{EnvFilter, Layer};
use url::Url;

use crate::middleware::metrics::RequestMetricsFairing;
use crate::routes::images;
use crate::services::image_service::cache::MmapImageCache;
use crate::services::image_service::client::ImageClient;
use crate::services::image_service::r#gen::ImageGenerator;
use crate::services::image_service::metrics::ImageMetrics;

mod middleware;
mod routes;
mod services;

use opentelemetry::{KeyValue, global};
use opentelemetry_otlp::{LogExporter, MetricExporter, Protocol, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};

use rocket::fairing::{Fairing, Info, Kind};
use rocket::http::Header;
use rocket::{Request, Response};

pub struct CORS;

#[rocket::async_trait]
impl Fairing for CORS {
    fn info(&self) -> Info {
        Info {
            name: "Add CORS headers to responses",
            kind: Kind::Response,
        }
    }

    async fn on_response<'r>(&self, _request: &'r Request<'_>, response: &mut Response<'r>) {
        response.set_header(Header::new("Access-Control-Allow-Origin", "*"));
        response.set_header(Header::new(
            "Access-Control-Allow-Methods",
            "POST, GET, PATCH, OPTIONS",
        ));
        response.set_header(Header::new("Access-Control-Allow-Headers", "*"));
        response.set_header(Header::new("Access-Control-Allow-Credentials", "true"));
    }
}

#[derive(Deserialize, Serialize)]
struct Package {
    name: String,
    version: String,
}

#[derive(Deserialize, Serialize)]
struct OTLP {
    instance_id: String,
    collector_endpoint: Option<Url>,
    collect_logs: Option<bool>,
    collect_metrics: Option<bool>,
}

#[derive(Deserialize, Serialize)]
struct Config {
    package: Package,
    otlp: OTLP,
    environment: String,
    log_directory: PathBuf,
    image_cache_directory: PathBuf,
}

pub struct Otel {
    meter_provider: Option<SdkMeterProvider>,
    logs_provider: Option<SdkLoggerProvider>,
}

impl Otel {
    pub fn default() -> Self {
        Self {
            logs_provider: None,
            meter_provider: None,
        }
    }
    fn get_resource(config: &Config) -> Resource {
        Resource::builder()
            .with_attributes([
                KeyValue::new("service.name", config.package.name.clone()),
                KeyValue::new("service.version", config.package.version.clone()),
                KeyValue::new("service.instance.id", config.otlp.instance_id.clone()),
                KeyValue::new("environment", config.environment.clone()),
            ])
            .build()
    }
    async fn from_config(config: &Config) -> Result<Self, Box<dyn std::error::Error>> {
        let mut this = Self::default();
        let resource = Self::get_resource(&config);

        if let Some(ref collector_endpoint) = config.otlp.collector_endpoint {
            if !(config.otlp.collect_logs == Some(false)) {
                println!(
                    "Initializing OTLP logs exporter with collector endpoint: {}",
                    collector_endpoint
                );
                this.logs_provider = Some(Self::init_logger(collector_endpoint, &resource).await?);
            } else {
                println!("OTLP logs collection is disabled by configuration.");
            }

            if !(config.otlp.collect_metrics == Some(false)) {
                println!(
                    "Initializing OTLP metrics exporter with collector endpoint: {}",
                    collector_endpoint
                );
                this.meter_provider = Some(Self::init_meter(collector_endpoint, &resource).await?);
            } else {
                println!("OTLP metrics collection is disabled by configuration.");
            }
        } else {
            println!(
                "OTLP collector endpoint is not configured; logs and metrics exporters will not be initialized."
            );
        }

        Ok(this)
    }
    async fn init_logger(
        collector_endpoint: &Url,
        resource: &Resource,
    ) -> Result<SdkLoggerProvider, Box<dyn std::error::Error>> {
        let log_endpoint = collector_endpoint.join("/v1/logs")?;

        let exporter = LogExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpJson)
            .with_endpoint(log_endpoint)
            .build()?;

        let processor = BatchLogProcessor::builder(exporter)
            .with_batch_config(BatchConfig::default())
            .build();

        let logger_provider = SdkLoggerProvider::builder()
            .with_log_processor(processor)
            .with_resource(resource.clone())
            .build();

        Ok(logger_provider)
    }
    async fn init_meter(
        collector_endpoint: &Url,
        resource: &Resource,
    ) -> Result<SdkMeterProvider, Box<dyn std::error::Error>> {
        let metrics_endpoint = collector_endpoint.join("/v1/metrics")?;
        let exporter = MetricExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpJson)
            .with_endpoint(metrics_endpoint)
            .build()?;

        let reader = PeriodicReader::builder(exporter).build();

        let provider = SdkMeterProvider::builder()
            .with_resource(resource.clone())
            .with_reader(reader)
            .build();

        Ok(provider)
    }
    fn shutdown(&self) {
        let mut errors = vec![];

        if let Some(ref provider) = self.meter_provider {
            if let Err(err) = provider.shutdown() {
                errors.push(format!("metrics: {err}"));
            }
        }

        if let Some(ref provider) = self.logs_provider {
            if let Err(err) = provider.shutdown() {
                errors.push(format!("logs: {err}"));
            }
        }

        if !errors.is_empty() {
            eprintln!("Failed to shutdown providers: {}", errors.join("\n"))
        }
    }
}

#[rocket::get("/")]
fn index() -> &'static str {
    "OK"
}

#[rocket::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Config
    let config_path = std::env::var("PLATFORM_CONFIG_PATH")
        .unwrap_or_else(|_| "./config.toml".into())
        .parse::<PathBuf>()?;

    if !config_path.exists() {
        return Err(format!("config file at path {:?} was not found", config_path).into());
    }

    let config: Config = Figment::new()
        .merge(Toml::file(config_path).nested())
        .extract()?;

    let otel = Otel::from_config(&config).await?;

    // Logging
    if let Some(ref logs_provider) = otel.logs_provider {
        tracing_subscriber::registry()
            .with(OtelBridge::new(logs_provider).with_filter(EnvFilter::new("warn")));
    } else {
        let log_file_path = config.log_directory.join("all.log");

        let log_file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&log_file_path)
            .map_err(|e| format!("{e}: {:?}", log_file_path))?;

        let fmt_layer = tracing_subscriber::fmt::layer()
            .with_writer(log_file)
            .compact()
            .with_filter(EnvFilter::new("warn"));

        tracing_subscriber::registry().with(fmt_layer);

        println!("Logging all outputs to {:?}", log_file_path);
    }

    // Metrics
    if let Some(meter_provider) = otel.meter_provider.clone() {
        global::set_meter_provider(meter_provider)
    }

    // Image Cache
    let image_cache = MmapImageCache::from_path(config.image_cache_directory)?;

    let image_client = ImageClient::new(
        ImageGenerator::new(),
        image_cache.clone(),
        ImageMetrics::new(),
    );

    // Rocket
    let server = rocket::Rocket::build()
        .attach(CORS)
        .attach(RequestMetricsFairing::new())
        .manage(image_client)
        .manage(image_cache)
        .mount("/", rocket::routes![index])
        .mount("/api/images", images::images_routes())
        .launch();

    select! {
        rocket = server => {
            let _ = rocket?;
        }
        _ = tokio::signal::ctrl_c() => {
            println!("Received SIGINT. Requesting shutdown.");

            otel.shutdown();
        }
    }

    Ok(())
}
