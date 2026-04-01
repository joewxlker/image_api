use tokio::select;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;
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
use opentelemetry_otlp::{MetricExporter, Protocol, WithExportConfig};
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

#[rocket::main]
async fn main() {
    dotenv::dotenv().ok();

    let config: Config = Figment::new()
        .merge(Toml::file("Cargo.toml"))
        .join(Json::file("app.json"))
        .extract()
        .unwrap();

    let log_file_path = config.log_directory.join("all.log");

    let log_file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(&log_file_path)
        .expect(&format!("{:?}", log_file_path));

    let filter = EnvFilter::from_default_env()
        .add_directive(LevelFilter::WARN.into());

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(log_file)
        .compact()
        .init();


    // Metrics
    let exporter = MetricExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpJson)
        .with_endpoint("http://localhost:4318/v1/metrics")
        .build()
        .expect("Failed to create OTLP metrics exporter");

    let reader = PeriodicReader::builder(exporter).build();
    let resource = Resource::builder()
        .with_attributes([
            KeyValue::new("service.name", "image-api"),
            KeyValue::new("service.version", "0.1.0"),
            KeyValue::new("service.instance.id", "home"),
            KeyValue::new("environment", "development"),
        ])
        .build();

    let provider = SdkMeterProvider::builder()
        .with_resource(resource)
        .with_reader(reader)
        .build();

    global::set_meter_provider(provider);

    // Rocket
    let image_cache = MmapImageCache::from_env();
    let image_client = ImageClient::new(
        ImageGenerator::new(),
        image_cache.clone(),
        ImageMetrics::new(),
    );

    let server = rocket::Rocket::build()
        .attach(CORS)
        .attach(RequestMetricsFairing::new())
        .manage(image_client)
        .manage(image_cache)
        .mount("/api/images", images::images_routes())
        .launch();

    select! {
        rocket = server => {
            if let Err(err) = rocket {
                eprintln!("{err}");
            }
        }
    }
}
