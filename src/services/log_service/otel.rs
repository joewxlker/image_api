use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge as OtelBridge;
use opentelemetry_otlp::{LogExporter, Protocol, WithExportConfig};
use opentelemetry_sdk::{
    Resource,
    logs::{BatchConfig, BatchLogProcessor, SdkLoggerProvider},
};
use tracing_subscriber::{Layer, layer::SubscriberExt, util::SubscriberInitExt};
use url::Url;

use crate::services::log_service::{ENV_FILTER, LogServiceError};

/// Sets the global tracing subscriber to forward logs to the otlp log exporter  
/// using the env filter [ENV_FILTER].
///
/// Logs are batched and exported via HTTP to the defined endpoint.
///
/// # Important
///
/// - This function must not be called more than once or with other
/// global subscriber setting functions.
///
/// - The endpoint's status is tested asynchronously for diagnostic purposes.
///
///   HTTP failures during this test are logged and do not affect execution.
///
/// # Arguments
///
/// - `logs_endpoint` - The OTLP HTTP endpoint to export logs to.
///   Typically of the form `http://host:4318/v1/logs`. The `/v1/logs`
///   path is optional.
///
/// - `resource` - The OpenTelemetry resource describing this service.
///
/// # Examples
///
/// ```rust
/// # use url::Url;
/// # use platform::config::OTLP_RESOURCE;
/// # use platform::services::log_service::otel::otel_logger;
/// # use opentelemetry_sdk::Resource;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let endpoint: Url = "http://localhost:4318/v1/logs".parse().unwrap();
///
/// let provider = otel_logger(&endpoint, &OTLP_RESOURCE)
///     .await
///     .unwrap();
/// # Ok(())
/// # }
/// ```
///
/// # Panics
///
/// - Calling multiple 'global-subscriber' setting functions will result in a panic
///
/// - Calling this function with invalid opentelemetry-otlp feature flags will result in a panic.
///
///   The supported feature-flag set is:
///   
///   ```toml
///   default-features = false
///   features = [
///       "reqwest-blocking-client",
///       "http-proto",
///       "trace",
///       "metrics",
///       "logs",
///       "internal-logs"
///   ]
///   ```
///
///   This is due to the SDK supporting multiple clients, which may or may not work in Tokio runtimes.
pub async fn otel_logger(
    logs_endpoint: &Url,
    resource: &Resource,
) -> Result<SdkLoggerProvider, LogServiceError> {
    let endpoint = logs_endpoint.clone();
    tokio::spawn(async move { check_otlp_endpoint(&endpoint).await });

    let exporter = LogExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpJson)
        .with_endpoint(logs_endpoint.clone())
        .build()?;

    let processor = BatchLogProcessor::builder(exporter)
        .with_batch_config(BatchConfig::default())
        .build();

    let provider = SdkLoggerProvider::builder()
        .with_log_processor(processor)
        .with_resource(resource.clone())
        .build();

    let filter = ENV_FILTER.clone();

    tracing_subscriber::registry()
        .with(OtelBridge::new(&provider).with_filter(filter))
        .init();

    Ok(provider)
}

/// Performs a diagnostic check of the OTLP endpoint.
///
/// Failures are logged and do not affect execution.
async fn check_otlp_endpoint(logs_endpoint: &Url) {
    // The OTLP SDK does not require the endpoint to be reachable.
    // This check exists only to surface connectivity issues via logs
    // for troubleshooting, and does not affect initialization.
    if let Err(err) = reqwest::Client::new()
        .get(logs_endpoint.as_str())
        .send()
        .await
    {
        tracing::warn!(
            error = %err,
            endpoint = %logs_endpoint,
            "OTLP logs endpoint is unreachable; logs will still be buffered and export attempts will continue"
        );
    } else {
        tracing::debug!(
            endpoint = %logs_endpoint,
            "Successfully reached OTLP logs endpoint"
        );
    }
}

pub async fn shutdown_otel_logging(logger_provider: SdkLoggerProvider) {
    if let Err(err) = logger_provider.shutdown() {
        tracing::error!(
            error = %err,
            "Failed to shut down logger provider"
        );
    } else {
        tracing::debug!("Logger provider shut down successfully");
    }
}
