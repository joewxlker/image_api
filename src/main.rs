use std::fs::OpenOptions;
use std::path::PathBuf;

use tokio::select;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;

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

const PROJECT_ROOT: &str = env!("PROJECT_ROOT");

#[rocket::main]
async fn main() {
    let project_root = PathBuf::from(PROJECT_ROOT);
    let image_cache = MmapImageCache::new(project_root.join("cache/images"));
    let image_client = ImageClient::new(ImageGenerator::new(), image_cache.clone(), ImageMetrics);
    let log_file_path = project_root.join("logs/all.log");

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

    let server = rocket::Rocket::build()
        .attach(CORS)
        .manage(image_client)
        .manage(image_cache)
        .mount("/api/images", images::images_routes())
        .launch();

    tracing::debug!("PROJECT_ROOT: {:?}", project_root);
    tracing::info!("Logging all outputs to: {:?}", log_file_path);

    select! {
        rocket = server => {
            if let Err(err) = rocket {
                eprintln!("{err}");
            }
        }
    }
}
