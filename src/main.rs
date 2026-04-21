use tokio::select;

use tracing::instrument::WithSubscriber;

use crate::config::Config;
use crate::middleware::cors::CORS;
use crate::middleware::metrics::RequestMetricsFairing;
use crate::routes::images;
use crate::services::image_service::cache::MmapImageCache;
use crate::services::image_service::client::ImageClient;
use crate::services::image_service::r#gen::ImageGenerator;
use crate::services::image_service::metrics::ImageMetrics;
use crate::services::log_service::otel::shutdown_otel_logging;
use crate::services::log_service::{STDOUT_LOGGER, initialize_logging};
use crate::services::metrics_service::initialize_metrics;
use crate::services::metrics_service::otel::shutdown_otel_metrics;

mod config;
mod middleware;
mod routes;
mod services;

#[rocket::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Config
    let config = Config::from_env()?;

    // Logging
    let logger_provider = initialize_logging(&config)
        .with_subscriber(STDOUT_LOGGER.clone())
        .await?;

    // Metrics
    let meter_provider = initialize_metrics(&config)
        .with_subscriber(STDOUT_LOGGER.clone())
        .await?;

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
        .mount("/images", images::images_routes())
        .launch();

    select! {
        rocket = server => {
            let _ = rocket?;
        }
        _ = tokio::signal::ctrl_c() => {
            if let Some(provider) = logger_provider {
                shutdown_otel_logging(provider)
                    .with_subscriber(STDOUT_LOGGER.clone()).await;
            }
            if let Some(provider) = meter_provider {
                shutdown_otel_metrics(provider)
                    .with_subscriber(STDOUT_LOGGER.clone()).await;
            }
        }
    }

    Ok(())
}
