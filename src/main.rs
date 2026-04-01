use figment::Figment;
use figment::providers::{Format, Json, Toml};
use tokio::select;

use crate::routes::images;
use crate::services::image_service::cache::MmapImageCache;
use crate::services::image_service::client::ImageClient;
use crate::services::image_service::r#gen::ImageGenerator;
use crate::services::image_service::metrics::ImageMetrics;
use crate::services::log_service::Logger;

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
    otlp_pendpoint: String,
    log_directory: String,
    cache_directory: String,
}

#[rocket::main]
async fn main() {
    let _: Config = Figment::new()
        .merge(Toml::file("Cargo.toml"))
        .join(Json::file("app.json"))
        .extract()
        .unwrap();

    let _ = Logger::from_env();
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
