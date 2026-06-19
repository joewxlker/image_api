use tracing::instrument::WithSubscriber;

use platform::config::IMAGE_CACHE_DIRECTORY;
use platform::middleware::cors::CORS;
use platform::middleware::metrics::RequestMetricsFairing;
use platform::routes::{health, images};
use platform::services::image_service::cache::MmapImageCache;
use platform::services::image_service::client::ImageClient;
use platform::services::image_service::r#gen::ImageGenerator;
use platform::services::image_service::metrics::ImageMetrics;
use platform::services::log_service::initialize_logging;
use platform::services::log_service::otel::shutdown_otel_logging;
use platform::services::log_service::stdout::STDOUT_LOGGER;
use platform::services::metrics_service::initialize_metrics;
use platform::services::metrics_service::otel::shutdown_otel_metrics;

#[rocket::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Logging
    let logger_provider = initialize_logging()
        .with_subscriber(STDOUT_LOGGER.clone())
        .await?;

    // Metrics
    let meter_provider = initialize_metrics()
        .with_subscriber(STDOUT_LOGGER.clone())
        .await?;

    // Image Client;
    let image_client = ImageClient::new(
        ImageGenerator::new(),
        MmapImageCache::from_static_path(&IMAGE_CACHE_DIRECTORY)?,
        ImageMetrics::new(),
    );

    // Rocket
    rocket::Rocket::build()
        .attach(CORS)
        .attach(RequestMetricsFairing::new())
        .manage(image_client)
        .mount("/images", images::images_routes())
        .mount("/health", health::health_routes())
        .launch()
        .await?;

    if let Some(provider) = logger_provider {
        shutdown_otel_logging(provider)
            .with_subscriber(STDOUT_LOGGER.clone())
            .await;
    }

    if let Some(provider) = meter_provider {
        shutdown_otel_metrics(provider)
            .with_subscriber(STDOUT_LOGGER.clone())
            .await;
    }

    Ok(())
}
