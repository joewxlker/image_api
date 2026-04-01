use std::fs::OpenOptions;
use std::path::PathBuf;

use figment::Figment;
use figment::providers::{Format, Json, Toml};
use tokio::select;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;
use url::Url;

use crate::routes::images;
use crate::services::image_service::cache::MmapImageCache;
use crate::services::image_service::client::ImageClient;
use crate::services::image_service::r#gen::ImageGenerator;
use crate::services::image_service::metrics::ImageMetrics;

mod routes;
mod services;

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

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct Package {
    name: String,
    version: String
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct Config {
    package: Package,
    otlp_instance_id: String,
    otlp_environment: String,
    otlp_pendpoint: Url,
    log_directory: PathBuf,
    cache_directory: PathBuf,
}

#[rocket::main]
async fn main() {
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

    let image_cache = MmapImageCache::from_env();
    let image_client = ImageClient::new(ImageGenerator::new(), image_cache.clone(), ImageMetrics);

    let server = rocket::Rocket::build()
        .attach(CORS)
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
