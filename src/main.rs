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

#[rocket::main]
async fn main() {
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
