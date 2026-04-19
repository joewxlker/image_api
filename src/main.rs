use std::fs::OpenOptions;
use std::path::PathBuf;

use figment::Figment;
use figment::providers::{Format, Toml};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_sdk::logs::{BatchConfig, BatchLogProcessor, SdkLoggerProvider};
use serde::{Deserialize, Serialize};
use tokio::select;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
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
    collector_endpoint: Url,
}

#[derive(Deserialize, Serialize)]
struct Config {
    package: Package,
    otlp: OTLP,
    environment: String,
    log_directory: PathBuf,
    image_cache_directory: PathBuf,
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

    // Logging
    let log_file_path = config.log_directory.join("all.log");

    let log_file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(&log_file_path)
        .map_err(|e| format!("{e}: {:?}", log_file_path))?;

    let log_endpoint = config.otlp.collector_endpoint.join("/v1")?.join("/logs")?;
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
        .with_resource(get_resource(&config))
        .build();

    let otel_layer =
        OpenTelemetryTracingBridge::new(&logger_provider).with_filter(EnvFilter::new("warn"));

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_writer(log_file)
        .compact()
        .with_filter(EnvFilter::new("warn"));

    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(otel_layer)
        .init();

    // Metrics
    let metrics_endpoint = config.otlp.collector_endpoint.join("/v1")?.join("/metrics")?;
    let exporter = MetricExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpJson)
        .with_endpoint(metrics_endpoint)
        .build()?;

    let reader = PeriodicReader::builder(exporter).build();

    let provider = SdkMeterProvider::builder()
        .with_resource(get_resource(&config))
        .with_reader(reader)
        .build();

    global::set_meter_provider(provider.clone());

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

            let mut errors = vec![];

            if let Err(err) = provider.shutdown() {
                errors.push(format!("metrics: {err}"));
            }

            if let Err(err) = logger_provider.shutdown() {
                errors.push(format!("logs: {err}"));
            }

            if !errors.is_empty() {
                eprintln!(
                    "Failed to shutdown providers: {}",
                    errors.join("\n")
                )
            } else {
                println!("Shutdown successful.")
            }
        }
    }

    Ok(())
}
